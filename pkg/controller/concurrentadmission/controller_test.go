/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package concurrentadmission

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	testingclock "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

var (
	workloadCmpOpts = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreFields(
			kueue.Workload{}, "TypeMeta", "ObjectMeta.ResourceVersion", "ObjectMeta.UID", "Status.AccumulatedPastExecutionTimeSeconds",
		),
		cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
		cmpopts.SortSlices(func(a, b kueue.Workload) bool { return a.Name < b.Name }),
		cmpopts.SortSlices(func(a, b metav1.Condition) bool { return a.Type < b.Type }),
	}
)

func TestReconcile(t *testing.T) {
	defaultCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("spot").Obj(),
			*utiltestingapi.MakeFlavorQuotas("on-demand").Obj(),
		).Obj()
	defaultLQ := utiltestingapi.MakeLocalQueue("lq", "default").ClusterQueue("cq").Obj()
	migrationCQ := utiltestingapi.MakeClusterQueue("cq-migration").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("spot").Obj(),
			*utiltestingapi.MakeFlavorQuotas("on-demand").Obj(),
			*utiltestingapi.MakeFlavorQuotas("fallback").Obj(),
		).Obj()
	migrationCQ.Spec.ConcurrentAdmission = &kueue.ConcurrentAdmission{
		MigrationConstraints: kueue.ConcurrentAdmissionMigrationConstraints{
			MinTargetFlavor: ptr.To(kueue.ResourceFlavorReference("on-demand")),
		},
	}
	migrationLQ := utiltestingapi.MakeLocalQueue("lq-migration", "default").ClusterQueue("cq-migration").Obj()

	testCases := map[string]struct {
		parentWorkload       *kueue.Workload
		variantWorkloads     []kueue.Workload
		wantParentWorkload   *kueue.Workload
		wantVariantWorkloads []kueue.Workload
		req                  reconcile.Request
		wantResult           reconcile.Result
		wantErr              bool
	}{
		"workload not found": {
			req: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "non-existing",
				},
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"workload is not parent or variant": {
			parentWorkload: utiltestingapi.MakeWorkload("wl", "default").Obj(),
			wantParentWorkload: utiltestingapi.MakeWorkload("wl", "default").Obj(),
			wantResult:         reconcile.Result{},
			wantErr:            false,
		},
		"parent workload without variants creates them": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", ""). // UID is ignored in cmp
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"admitted variant syncs admission to parent": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			variantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq", "spot", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
			},
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Admission(utiltestingapi.MakeAdmission("cq", "main").
					PodSets(kueue.PodSetAssignment{
						Name: "main",
						Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{
							corev1.ResourceCPU: "spot",
						},
						Count:         ptr.To[int32](1),
						ResourceUsage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
					}).Obj()).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadAdmitted,
					Status:  metav1.ConditionTrue,
					Reason:  "Admitted",
					Message: "The variant parent-variant-spot is admitted",
				}).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadQuotaReserved,
					Status:  metav1.ConditionTrue,
					Reason:  "QuotaReserved",
					Message: "Quota reserved in ClusterQueue cq",
				}).
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq", "spot", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"evicted variant clears admission on parent": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				SimpleReserveQuota("cq", "spot", metav1.Now().Time).
				AdmittedAt(true, metav1.Now().Time).
				Obj(),
			variantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
			},
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				// We expect Admission to NOT be cleared due to fake client SSA limitations
				SimpleReserveQuota("cq", "spot", metav1.Now().Time).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadQuotaReserved,
					Status:  metav1.ConditionFalse,
					Reason:  "Pending",
					Message: "No variant is admitted",
				}).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadAdmitted,
					Status:  metav1.ConditionFalse,
					Reason:  "NoReservation",
					Message: "The workload has no reservation",
				}).
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"migration constraints deactivate fallback flavor": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq-migration").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			variantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq-migration").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(true).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq-migration").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq-migration", "on-demand", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-fallback", "default").
					Queue("lq-migration").
					AllowedFlavors("fallback").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(true).
					Obj(),
			},
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq-migration").
				Label(workload.ParentVariantLabel, "true").
				Admission(utiltestingapi.MakeAdmission("cq-migration", "main").
					PodSets(kueue.PodSetAssignment{
						Name: "main",
						Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{
							corev1.ResourceCPU: "on-demand",
						},
						Count:         ptr.To[int32](1),
						ResourceUsage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
					}).Obj()).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadAdmitted,
					Status:  metav1.ConditionTrue,
					Reason:  "Admitted",
					Message: "The variant parent-variant-on-demand is admitted",
				}).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadQuotaReserved,
					Status:  metav1.ConditionTrue,
					Reason:  "QuotaReserved",
					Message: "Quota reserved in ClusterQueue cq-migration",
				}).
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq-migration").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(true).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq-migration").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq-migration", "on-demand", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-fallback", "default").
					Queue("lq-migration").
					AllowedFlavors("fallback").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(false). // Should be deactivated!
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"admitted variant evicted; clear the reservation": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			variantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq", "spot", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadEvicted,
						Status:  metav1.ConditionTrue,
						Reason:  kueue.WorkloadEvictedByPreemption,
						Message: "Evicted by preemption",
					}).
					Active(true).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(false).
					Obj(),
			},
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					// SimpleReserveQuota("cq", "spot", metav1.Now().Time). // Kept due to fake client limitations
					Condition(metav1.Condition{
						Type:    kueue.WorkloadEvicted,
						Status:  metav1.ConditionTrue,
						Reason:  kueue.WorkloadEvictedByPreemption,
						Message: "Evicted by preemption",
					}).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadQuotaReserved,
						Status:  metav1.ConditionFalse,
						Reason:  kueue.WorkloadEvictedByPreemption,
						Message: "Evicted by preemption",
					}).
					Active(true).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(false).
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
		"evicted parent workload evicts admitted variant": {
			parentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				SimpleReserveQuota("cq", "spot", metav1.Now().Time).
				AdmittedAt(true, metav1.Now().Time).
				Condition(metav1.Condition{
					Type:    kueue.WorkloadEvicted,
					Status:  metav1.ConditionTrue,
					Reason:  kueue.WorkloadEvictedDueToNodeFailures,
					Message: "Evicted due to node failures",
					LastTransitionTime: metav1.NewTime(metav1.Now().Time.Add(1 * time.Hour)),
				}).
				Obj(),
			variantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq", "spot", metav1.Now().Time).
					AdmittedAt(true, metav1.Now().Time).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(false).
					Obj(),
			},
			wantParentWorkload: utiltestingapi.MakeWorkload("parent", "default").
				Queue("lq").
				Label(workload.ParentVariantLabel, "true").
				SimpleReserveQuota("cq", "spot", metav1.Now().Time). // Kept due to fake client
				Condition(metav1.Condition{
					Type:    kueue.WorkloadEvicted,
					Status:  metav1.ConditionTrue,
					Reason:  kueue.WorkloadEvictedDueToNodeFailures,
					Message: "Evicted due to node failures",
				}).
				Obj(),
			wantVariantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("parent-variant-spot", "default").
					Queue("lq").
					AllowedFlavors("spot").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					SimpleReserveQuota("cq", "spot", metav1.Now().Time). // Kept due to fake client
					Condition(metav1.Condition{
						Type:    kueue.WorkloadQuotaReserved,
						Status:  metav1.ConditionFalse,
						Reason:  kueue.WorkloadEvictedDueToNodeFailures,
						Message: "Evicted due to node failures",
					}).
					Active(true).
					Obj(),
				*utiltestingapi.MakeWorkload("parent-variant-on-demand", "default").
					Queue("lq").
					AllowedFlavors("on-demand").
					Request(corev1.ResourceCPU, "1").
					ControllerReference(kueue.GroupVersion.WithKind("Workload"), "parent", "").
					Active(false).
					Obj(),
			},
			wantResult: reconcile.Result{},
			wantErr:    false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objects []client.Object
			if tc.parentWorkload != nil {
				objects = append(objects, tc.parentWorkload)
			}
			for i := range tc.variantWorkloads {
				objects = append(objects, &tc.variantWorkloads[i])
			}
			cl := utiltesting.NewClientBuilder().WithObjects(objects...).WithStatusSubresource(objects...).Build()
			cqCache := schdcache.New(cl)
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			roleTracker := roletracker.NewFakeRoleTracker(roletracker.RoleLeader)

			// Always create all CQs and LQs
			cqs := []*kueue.ClusterQueue{defaultCQ.DeepCopy(), migrationCQ.DeepCopy()}
			lqs := []*kueue.LocalQueue{defaultLQ.DeepCopy(), migrationLQ.DeepCopy()}

			for _, cq := range cqs {
				if err := cl.Create(context.Background(), cq); err != nil {
					t.Fatal(err)
				}
				if err := cqCache.AddClusterQueue(context.Background(), cq); err != nil {
					t.Fatal(err)
				}
				if err := qManager.AddClusterQueue(context.Background(), cq); err != nil {
					t.Fatal(err)
				}
			}

			for _, lq := range lqs {
				if err := cl.Create(context.Background(), lq); err != nil {
					t.Fatal(err)
				}
				if err := cqCache.AddLocalQueue(lq); err != nil {
					t.Fatal(err)
				}
				if err := qManager.AddLocalQueue(context.Background(), lq); err != nil {
					t.Fatal(err)
				}
			}

			if tc.parentWorkload != nil {
				cqCache.AddOrUpdateWorkload(ctrl.Log, tc.parentWorkload.DeepCopy())
				qManager.AddOrUpdateWorkload(ctrl.Log, tc.parentWorkload.DeepCopy())
			}
			for i := range tc.variantWorkloads {
				cqCache.AddOrUpdateWorkload(ctrl.Log, tc.variantWorkloads[i].DeepCopy())
				qManager.AddOrUpdateWorkload(ctrl.Log, tc.variantWorkloads[i].DeepCopy())
			}

			r := &variantReconciler{
				logName:     ConcurrentAdmissionController,
				client:      cl,
				queues:      qManager,
				cache:       cqCache,
				roleTracker: roleTracker,
				clock:       testingclock.NewFakeClock(metav1.Now().Time),
			}

			req := tc.req
			if req.Name == "" && tc.parentWorkload != nil {
				req = reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: tc.parentWorkload.Namespace,
						Name:      tc.parentWorkload.Name,
					},
				}
			}

			got, err := r.Reconcile(context.Background(), req)
			if (err != nil) != tc.wantErr {
				t.Errorf("Reconcile() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !cmp.Equal(got, tc.wantResult) {
				t.Errorf("Reconcile() got = %v, want %v", got, tc.wantResult)
			}

			// Verify Parent Workload
			if tc.wantParentWorkload != nil {
				var gotParent kueue.Workload
				err := cl.Get(context.Background(), types.NamespacedName{Namespace: tc.wantParentWorkload.Namespace, Name: tc.wantParentWorkload.Name}, &gotParent)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(tc.wantParentWorkload, &gotParent, workloadCmpOpts); diff != "" {
					t.Errorf("Unexpected parent workload (-want +got):\n%s", diff)
				}
			}

			// Verify Variant Workloads
			var gotVariants kueue.WorkloadList
			if err := cl.List(context.Background(), &gotVariants, client.InNamespace(tc.req.Namespace)); err != nil {
				t.Fatal(err)
			}

			// Filter out parent from gotVariants
			var variants []kueue.Workload
			for _, wl := range gotVariants.Items {
				if wl.Name != tc.wantParentWorkload.Name {
					variants = append(variants, wl)
				}
			}

			if diff := cmp.Diff(tc.wantVariantWorkloads, variants, workloadCmpOpts); diff != "" {
				t.Errorf("Unexpected variant workloads (-want +got):\n%s", diff)
			}
		})
	}
}
