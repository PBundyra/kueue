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
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/core"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	ConcurrentAdmissionController = "concurrent-admission-controller"
)

type variantReconciler struct {
	logName string
	queues  *qcache.Manager
	// cache       *schdcache.Cache
	client      client.Client
	recorder    record.EventRecorder
	roleTracker *roletracker.RoleTracker
	clock       clock.Clock
}

var _ reconcile.Reconciler = (*variantReconciler)(nil)
var _ predicate.TypedPredicate[*kueue.Workload] = (*variantReconciler)(nil)

func GetVariants(c client.Client, parent *kueue.Workload) ([]kueue.Workload, error) {
	if !isParentVariant(parent) {
		return nil, fmt.Errorf("workload %s/%s is not a parent variant", parent.Namespace, parent.Name)
	}
	// get workloads for which the parents is an owner using owner references
	list := &kueue.WorkloadList{}
	if err := c.List(context.Background(), list, client.InNamespace(parent.Namespace)); err != nil {
		// TODO: add an index for parent variant to avoid listing all workloads in the namespace
		return nil, err
	}
	variants := make([]kueue.Workload, 0)
	for i := range list.Items {
		if workload.GetParentVariant(&list.Items[i]) != parent.Name {
			continue
		}
		variants = append(variants, list.Items[i])
	}
	return variants, nil
}

func (r *variantReconciler) getVariants(wl *kueue.Workload) ([]kueue.Workload, error) {
	if !isParentVariant(wl) {
		return nil, fmt.Errorf("workload %s/%s is not a parent variant", wl.Namespace, wl.Name)
	}
	// get workloads for which the parents is an owner using owner references
	list := &kueue.WorkloadList{}
	if err := r.client.List(context.Background(), list, client.InNamespace(wl.Namespace)); err != nil {
		// TODO: add an index for parent variant to avoid listing all workloads in the namespace
		return nil, err
	}
	variants := make([]kueue.Workload, 0)
	for i := range list.Items {
		if workload.GetParentVariant(&list.Items[i]) != wl.Name {
			continue
		}
		variants = append(variants, list.Items[i])
	}
	return variants, nil
}

func (r *variantReconciler) getClusterQueue(wl *kueue.Workload) (*kueue.ClusterQueue, error) {
	cqName, ok := r.queues.ClusterQueueForWorkload(wl)
	if !ok {
		return nil, fmt.Errorf("could not find cluster queue for workload %s/%s", wl.Namespace, wl.Name)
	}
	cq := &kueue.ClusterQueue{}
	if err := r.client.Get(context.Background(), client.ObjectKey{Name: string(cqName)}, cq); err != nil {
		return nil, err
	}
	return cq, nil
}

// activateVariants activates variants. It covers the case of preemption, where some variants were previously deactivated, because one of them
// got admitted. When the admitted variants gets evicted, it activates all variants to allow them to be scheduled.
// Additionally it reconciles variants to follow the migrationConstraints specified in the ClusterQueue.
func (r *variantReconciler) activateVariants(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload, cq *kueue.ClusterQueue, flavorOrder map[kueue.ResourceFlavorReference]int) error {
	if !workload.IsActive(parent) {
		r.logger().V(2).Info("Parent is not active, no needed to activate variants", "parent", parent.Name)
		return nil
	}
	admittedVariant := workload.GetAdmittedVariant(variants)
	if admittedVariant == nil {
		r.logger().V(2).Info("No admitted variant, activating all variants")
		// no admitted variants so activate all variants if they are not active, case of preemption
		for i := range variants {
			v := &variants[i]
			if workload.IsActive(v) {
				continue
			}
			v.Spec.Active = ptr.To(true)
			if err := r.client.Update(ctx, v); err != nil {
				return err
			}
		}
		return nil
	}
	// activate all variants that are at least at the minTargetFlavor if specificed
	var minTargetFlavor *kueue.ResourceFlavorReference
	if cq.Spec.ConcurrentAdmission != nil {
		minTargetFlavor = cq.Spec.ConcurrentAdmission.MigrationConstraints.MinTargetFlavor
	}
	if minTargetFlavor != nil {
		for i := range variants {
			v := &variants[i]
			if workload.IsActive(v) {
				continue
			}
			if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] <= flavorOrder[*minTargetFlavor] {
				// activate the variant, the smaller or equal the flavor order is to the minTargetFlavor, the higher the priority is
				v.Spec.Active = ptr.To(true)
				if err := r.client.Update(ctx, v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	// no minTargetFlavor specified, so activate all variants that are below the admitted variant in the flavor order
	for i := range variants {
		v := &variants[i]
		if workload.IsActive(v) {
			continue
		}
		if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] < flavorOrder[admittedVariant.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] {
			// activate the variant, the smaller the flavor order is to the admitted variant, the higher the priority is
			v.Spec.Active = ptr.To(true)
			if err := r.client.Update(ctx, v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *variantReconciler) deactivateVariant(ctx context.Context, v *kueue.Workload) error {
	if !workload.IsActive(v) {
		return nil
	}
	v.Spec.Active = ptr.To(false)
	if err := r.client.Update(ctx, v); err != nil {
		return err
	}
	// fetch the updated variant and unset quota
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		return err
	}
	if evCond := apimeta.FindStatusCondition(v.Status.Conditions, kueue.WorkloadEvicted); evCond != nil && evCond.Status == metav1.ConditionTrue {
		if workload.HasQuotaReservation(v) {
			r.logger().V(2).Info("The variant is no longer active, clear the workloads admission")
			err := workload.PatchAdmissionStatus(ctx, r.client, v, r.clock, func(wl *kueue.Workload) (bool, error) {
				// The requeued condition status set to true only on EvictedByPreemption
				setRequeued := (evCond.Reason == kueue.WorkloadEvictedByPreemption) || (evCond.Reason == kueue.WorkloadEvictedDueToNodeFailures)
				updated := workload.SetRequeuedCondition(wl, evCond.Reason, evCond.Message, setRequeued)
				if workload.UnsetQuotaReservationWithCondition(wl, "Pending", evCond.Message, r.clock.Now()) {
					updated = true
				}
				return updated, nil
			})
			if err != nil {
				return fmt.Errorf("clearing admission: %w", err)
			}
		}
	}
	return nil
}

// deactivateVariants deactivates variants after one of the variants gets admitted.
// If minTargetFlavor is specified, it deactivates variants that are below the minTargetFlavor in the flavor order.
// Otherwise it deactivates variants that are below the admitted variant in the flavor order.
func (r *variantReconciler) deactivateVariants(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload, cq *kueue.ClusterQueue, flavorOrder map[kueue.ResourceFlavorReference]int) error {
	if !workload.IsActive(parent) {
		r.logger().V(2).Info("Parent is not active, deactivating all variants", "parent", parent.Name)
		for i := range variants {
			v := &variants[i]
			if err := r.deactivateVariant(ctx, v); err != nil {
				return err
			}
		}
		return nil
	}

	admittedWl := workload.GetAdmittedVariant(variants)
	if admittedWl == nil {
		r.logger().V(2).Info("No admitted variant, no need to deactivate any variant")
		return nil
	}
	// deactivate Variants below minTargetFlavor if specified
	var minTargetFlavor *kueue.ResourceFlavorReference
	if cq.Spec.ConcurrentAdmission != nil {
		minTargetFlavor = cq.Spec.ConcurrentAdmission.MigrationConstraints.MinTargetFlavor
	}
	if minTargetFlavor != nil {
		r.logger().V(2).Info("Deactivating variants below minTargetFlavor", "minTargetFlavor", *minTargetFlavor)
		for i := range variants {
			v := &variants[i]
			if v.Name == admittedWl.Name {
				continue
			}
			if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] > flavorOrder[*minTargetFlavor] {
				r.logger().V(2).Info("Deactivating variant because it is below the minTargetFlavor", "variant", v.Name, "flavor", v.Spec.AdmissionConstraints.AllowedResourceFlavors[0], "minTargetFlavor", *minTargetFlavor)
				if err := r.deactivateVariant(ctx, v); err != nil {
					return err
				}
			}
		}
	}
	// also deactivate Variants below the admitted variant regardless of minTargetFlavor
	r.logger().V(2).Info("Deactivating variants below the admitted variant", "admittedVariant", admittedWl.Name, "admittedFlavor", admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0])
	for i := range variants {
		v := &variants[i]
		if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] > flavorOrder[admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] {
			r.logger().V(2).Info("Deactivating variant because it is below the admitted variant", "variant", v.Name, "flavor", v.Spec.AdmissionConstraints.AllowedResourceFlavors[0], "admittedFlavor", admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0])
			if err := r.deactivateVariant(ctx, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// sort variants based on the order of resource flavors in the cluster queue, the variant with the flavor that is first in the order should be admitted first
func sortVariantsByFlavorOrder(variants []kueue.Workload, cq *kueue.ClusterQueue, flavorOrder map[kueue.ResourceFlavorReference]int) []kueue.Workload {
	slices.SortFunc(variants, func(a, b kueue.Workload) int {
		aFlavor := a.Spec.AdmissionConstraints.AllowedResourceFlavors[0] // we only support one flavor per variant for now
		bFlavor := b.Spec.AdmissionConstraints.AllowedResourceFlavors[0]
		return flavorOrder[aFlavor] - flavorOrder[bFlavor]
	})
	return variants
}

func (r *variantReconciler) syncFinished(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload) error {
	finishCond := apimeta.FindStatusCondition(parent.Status.Conditions, kueue.WorkloadFinished)
	for i := range variants {
		v := &variants[i]
		if err := workload.Finish(ctx, r.client, v, finishCond.Reason, finishCond.Message, r.clock); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *variantReconciler) syncAdmissionStatus(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload) error {
	if workload.IsFinished(parent) {
		return r.syncFinished(ctx, parent, variants)
	}
	admittedVariant := workload.GetAdmittedVariant(variants)
	switch {
	case admittedVariant == nil && workload.IsAdmitted(parent):
		// variant got evicted
		r.logger().V(2).Info("Parent is admitted but no variant is admitted, updating parent to not admitted", "parent", parent.Name)
		// delete parents admission from status and change its conditions to quotareserved=false and pending with the message that no variant is admitted,
		// TODO maybe evict instead of unset (?)
		err := workload.PatchAdmissionStatus(ctx, r.client, parent, r.clock, func(wl *kueue.Workload) (bool, error) {
			if workload.UnsetQuotaReservationWithCondition(wl, "Pending", "No variant is admitted", r.clock.Now()) {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return fmt.Errorf("clearing admission: %w", err)
		}
		// parent got evicted or parent's admission status has not been synced yet
	case admittedVariant != nil && !workload.IsAdmitted(parent):
		// It can either mean that Parent has not been admitted yet and needs admission
		// Or Parent has been preempted, and we need to propagate that to the variant e.g. due to WaitForPodsReady.
		variantAdmittedCond := apimeta.FindStatusCondition(admittedVariant.Status.Conditions, kueue.WorkloadAdmitted)
		parentEvictedCond := apimeta.FindStatusCondition(parent.Status.Conditions, kueue.WorkloadEvicted)
		evictVariant := false
		if apimeta.IsStatusConditionTrue(parent.Status.Conditions, kueue.WorkloadEvicted) {
			evictVariant = variantAdmittedCond.LastTransitionTime.Before(&parentEvictedCond.LastTransitionTime) || variantAdmittedCond.LastTransitionTime.Equal(&parentEvictedCond.LastTransitionTime)
		}
		if evictVariant {
			r.logger().V(2).Info("Evicting variant because parent is evicted", "variant", admittedVariant.Name, "parent", parent.Name)
			return workload.Evict(ctx, r.client, r.recorder, admittedVariant, parentEvictedCond.Reason, parentEvictedCond.Message, "", r.clock, false, nil, nil, nil)
		}

		r.logger().V(2).Info("Parent is not admitted but a variant is admitted, updating parent to admitted", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
		if err := workload.PatchAdmissionStatus(ctx, r.client, parent, r.clock, func(wl *kueue.Workload) (bool, error) {
			workload.SetQuotaReservation(wl, admittedVariant.Status.Admission, r.clock)
			workload.SetAdmittedCondition(wl, r.clock.Now(), "Admitted", fmt.Sprintf("The variant %s is admitted", admittedVariant.Name))
			r.logger().V(2).Info("Parent status at the end of patch", "conditions", wl.Status.Conditions, "admission", wl.Status.Admission)
			return true, nil
		}); err != nil {
			return client.IgnoreNotFound(err)
		}
	case admittedVariant != nil && workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent and admitted variant are both admitted, checking if the parent's admission status is the same as the admitted variant", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
		if err := workload.PatchAdmissionStatus(ctx, r.client, parent, r.clock, func(wl *kueue.Workload) (bool, error) {
			// check if the admission of the parent is the same a variant's
			oldStatus := wl.Status.Admission.DeepCopy()
			updated := false
			updated = workload.SetQuotaReservation(wl, admittedVariant.Status.Admission, r.clock) || updated
			updated = !equality.Semantic.DeepEqual(oldStatus, &wl.Status.Admission) || updated
			variantAdmittedCond := apimeta.FindStatusCondition(admittedVariant.Status.Conditions, kueue.WorkloadAdmitted)
			updated = apimeta.SetStatusCondition(&wl.Status.Conditions, *variantAdmittedCond) || updated
			if updated {
				r.logger().V(2).Info("Updated parent's admission status to match the admitted variant", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
			} else {
				r.logger().V(2).Info("Parent's admission status is already up to date with the admitted variant, no update needed", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
			}
			return updated, nil
		}); err != nil {
			return client.IgnoreNotFound(err)
		}
	case admittedVariant == nil && !workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent and variants are both not admitted, no action needed", "parent", parent.Name)
	}
	return nil
}

func (r *variantReconciler) hasVariantWithFlavor(variants []kueue.Workload, flavor kueue.ResourceFlavorReference) bool {
	r.logger().V(2).Info("Checking if there is a variant with the flavor", "flavor", flavor)
	for _, v := range variants {
		if v.Spec.AdmissionConstraints.AllowedResourceFlavors[0] == flavor {
			r.logger().V(2).Info("Found a variant with the flavor", "variant", v.Name, "flavor", flavor)
			return true
		}
	}
	return false
}

func generateVariant(parent *kueue.Workload, flavor kueue.ResourceFlavorReference) *kueue.Workload {
	variant := &kueue.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:          jobframework.GetWorkloadNameForVariant(parent.Name, parent.UID, parent.GroupVersionKind(), string(flavor)),
			Namespace:     parent.Namespace,
			Labels:        parent.Labels,
			Annotations:   parent.Annotations,
			ManagedFields: parent.ManagedFields,
		},
		Spec:   parent.Spec,
		Status: parent.Status,
	}
	delete(variant.Labels, workload.ParentVariantLabel)
	variant.Spec.AdmissionConstraints = &kueue.AdmissionConstraints{
		// we only support one flavor per variant for now, we can extend this in the future if needed
		AllowedResourceFlavors: []kueue.ResourceFlavorReference{flavor},
	}
	return variant
}

// Create variants based on the flavors in the cluster queue, each variant should have a different resource flavors in the spec allowed flavors, and owner reference to the parent workload
func (r *variantReconciler) createVariants(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload, resourceFlavors map[kueue.ResourceFlavorReference]int) error {
	log := ctrl.LoggerFrom(ctx)
	for flavor, _ := range resourceFlavors {
		if r.hasVariantWithFlavor(variants, flavor) {
			continue
		}

		variant := generateVariant(parent, flavor)
		log.V(2).Info("Creating variant for flavor", "flavor", flavor, "variant", variant.Name)

		// Set the owner reference to the parent workload
		if err := ctrl.SetControllerReference(parent, variant, r.client.Scheme()); err != nil {
			r.logger().V(2).Info("Failed to set owner reference for variant", "variant", variant.Name, "parent", parent.Name, "error", err)
			return err
		}
		if err := r.client.Create(ctx, variant); err != nil {
			r.logger().V(2).Info("Failed to create variant", "variant", variant.Name, "error", err)
			return err
		}
		r.logger().V(2).Info("Variant created", "variant", variant.Name, "flavor", flavor)
		// TODO think if we need any events recording here
	}
	return nil
}

// syncVariantEvictionStatus frees quota reservation of the evicted variant leading to requeuing, mimicking the behavior of jobframework for regular workloads.
// This is needed as jobframework ignores variant workloads and processes only parents, to preserve 1:1 mapping between a Workload and a Job.
func (r *variantReconciler) syncVariantEvictionStatus(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload) error {
	r.logger().V(2).Info("Syncing eviction status of variants", "parent", parent.Name)
	// Handle variant evictions, e.g. due to priorities in the cluster queue
	for i := range variants {
		v := &variants[i] // Safe pointer reference to the slice element
		evCond := apimeta.FindStatusCondition(v.Status.Conditions, kueue.WorkloadEvicted)
		if evCond != nil && evCond.Status == metav1.ConditionTrue && workload.HasQuotaReservation(v) {
			r.logger().V(2).Info("The variant has been evicted, clearing the workload's admission", "variant", v.Name)
			if err := r.clearWorkloadAdmission(ctx, v, evCond); err != nil {
				return fmt.Errorf("clearing variant admission: %w", err)
			}
		}
	}
	return nil
}

func (r *variantReconciler) clearWorkloadAdmission(ctx context.Context, wl *kueue.Workload, evCond *metav1.Condition) error {
	return workload.PatchAdmissionStatus(ctx, r.client, wl, r.clock, func(w *kueue.Workload) (bool, error) {
		setRequeued := (evCond.Reason == kueue.WorkloadEvictedByPreemption) ||
			(evCond.Reason == kueue.WorkloadEvictedDueToNodeFailures)
		updated := workload.SetRequeuedCondition(w, evCond.Reason, evCond.Message, setRequeued)
		if workload.UnsetQuotaReservationWithCondition(w, "Pending", evCond.Message, r.clock.Now()) {
			updated = true
		}
		return updated, nil
	})
}

// Reconcile reconciles Worklaods that are Parents of Variants.
func (r *variantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Reconcile Workload, with more logs to sync")

	wl := &kueue.Workload{}
	if err := r.client.Get(ctx, req.NamespacedName, wl); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(2).Info("Workload not found, might have been deleted", "name", req.Name, "namespace", req.Namespace)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !isParentVariant(wl) {
		return ctrl.Result{}, nil
	}
	parent := wl
	variants, err := r.getVariants(parent)
	if err != nil {
		return ctrl.Result{}, err
	}
	cq, err := r.getClusterQueue(parent)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !r.queues.ConcurrentAdmissionEnabled(kueue.ClusterQueueReference(cq.Name)) {
		// if ConcurrentAdmission is no longer enabled for this CQ, delete parent and variants
		if err := r.client.Delete(ctx, parent); err != nil {
			return ctrl.Result{}, err
		}
		for _, v := range variants {
			if err := r.client.Delete(ctx, &v); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	flavorOrder := make(map[kueue.ResourceFlavorReference]int)
	for i, flavor := range cq.Spec.ResourceGroups[0].Flavors {
		flavorOrder[flavor.Name] = i
	}
	variants = sortVariantsByFlavorOrder(variants, cq, flavorOrder)

	log.V(2).Info("Found Workload family and ClusterQueue", "parent", parent.Name, "clusterQueue", cq.Name, "variants", utilslices.Map(variants, func(v *kueue.Workload) string {
		return v.Name
	}))

	// TODO: compare the flavors of the variants with the flavors in the cluster queue and not just the number of variants,
	//  to cover the case when the cluster queue got updated with a different set of flavors, or when the flavors in the cluster queue got reduced,
	//  so we need to delete the excess variants that are not in the cluster queue flavors anymore, or that are above the number of flavors in the cluster queue
	if len(variants) < len(flavorOrder) {
		// TODO: change into checking each flavor instead of sum
		log.V(2).Info("Too few variants, creating new ones", "desired", len(flavorOrder), "actual", len(variants))
		if err := r.createVariants(ctx, parent, variants, flavorOrder); err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(variants) == len(flavorOrder) {
		log.V(2).Info("Desired number of variants, no action needed", "desired", len(flavorOrder), "actual", len(variants))
	}

	log.V(2).Info("Syncing variants if needed")
	if err := r.syncVariantEvictionStatus(ctx, parent, variants); err != nil {
		r.logger().V(2).Info("Failed to sync variant eviction status", "error", err)
		return ctrl.Result{}, err
	}

	log.V(2).Info("Deactivating variants if needed")
	if err := r.deactivateVariants(ctx, parent, variants, cq, flavorOrder); err != nil {
		r.logger().V(2).Info("Failed to deactivate variants", "error", err)
		return ctrl.Result{}, err
	}

	// TODO: Handle delayed requeueing e.g. waitForPodsReady, and AdmissionChecks
	log.V(2).Info("Activating variants if needed")
	if err := r.activateVariants(ctx, parent, variants, cq, flavorOrder); err != nil {
		r.logger().V(2).Info("Failed to activate variants", "error", err)
		return ctrl.Result{}, err
	}

	// TODO: think if this call should be before or after activating/deactivating variants, or if we need to call it multiple times, e.g. after activating and after deactivating
	if err := r.syncAdmissionStatus(ctx, parent, variants); err != nil {
		log.V(2).Info("Failed to sync admission status", "error", err)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *variantReconciler) Generic(event.TypedGenericEvent[*kueue.Workload]) bool {
	return false
}

func (r *variantReconciler) Create(e event.TypedCreateEvent[*kueue.Workload]) bool {
	log := r.logger()
	log.V(2).Info("Create event for Workload", "name", e.Object.Name, "namespace", e.Object.Namespace)
	return r.shouldReconcile(e.Object)
}

func (r *variantReconciler) Update(e event.TypedUpdateEvent[*kueue.Workload]) bool {
	log := r.logger()
	log.V(2).Info("Update event for Workload", "name", e.ObjectNew.Name, "namespace", e.ObjectNew.Namespace)
	return r.shouldReconcile(e.ObjectNew)

}

func (r *variantReconciler) Delete(e event.TypedDeleteEvent[*kueue.Workload]) bool {
	log := r.logger()
	log.V(2).Info("Delete event for Workload", "name", e.Object.Name, "namespace", e.Object.Namespace)
	return r.shouldReconcile(e.Object)
}

func (r *variantReconciler) shouldReconcile(workload *kueue.Workload) bool {
	log := r.logger()
	if isParentVariant(workload) {
		log.V(2).Info("Workload is a parent variant, reconciling", "name", workload.Name, "namespace", workload.Namespace)
		return true
	}
	if isVariant(workload) {
		log.V(2).Info("Workload is a variant, reconciling", "name", workload.Name, "namespace", workload.Namespace)
		return true
	}
	log.V(2).Info("Workload is neither a parent variant nor a variant, ignoring", "name", workload.Name, "namespace", workload.Namespace)
	return false
}

var _ handler.EventHandler = (*clusterQueueHandler)(nil)

type clusterQueueHandler struct {
	client client.Client
	queues *qcache.Manager
	cache  *schdcache.Cache
}

func (h *clusterQueueHandler) Create(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *clusterQueueHandler) Update(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldCQ, okOld := e.ObjectOld.(*kueue.ClusterQueue)
	newCQ, okNew := e.ObjectNew.(*kueue.ClusterQueue)
	if !okOld || !okNew {
		return
	}
	if !equality.Semantic.DeepEqual(oldCQ.Spec.ConcurrentAdmission, newCQ.Spec.ConcurrentAdmission) {
		h.queueReconcileForCQ(newCQ, q)
	}
}

func (h *clusterQueueHandler) Delete(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *clusterQueueHandler) Generic(_ context.Context, _ event.GenericEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *clusterQueueHandler) queueReconcileForCQ(object client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	cq, ok := object.(*kueue.ClusterQueue)
	if !ok {
		return
	}
	// Reconcile all parent variants that are in this cluster queue
	workloadsInfo := h.queues.PendingWorkloadsInfo(kueue.ClusterQueueReference(cq.Name))
	for _, info := range workloadsInfo {
		wl := info.Obj
		parentName := workload.GetParentVariant(wl)
		q.Add(reconcile.Request{
			NamespacedName: client.ObjectKey{
				Namespace: wl.Namespace,
				Name:      parentName,
			},
		})
	}
	for wlName, cqRef := range h.cache.WorkloadAssignedQueues() {
		if cqRef == kueue.ClusterQueueReference(cq.Name) {
			wl := h.cache.GetWorkloadInfo(wlName)
			parentName := workload.GetParentVariant(wl.Obj)
			// requeue parent of running variant
			q.Add(reconcile.Request{
				NamespacedName: client.ObjectKey{
					Namespace: wl.Obj.Namespace,
					Name:      parentName,
				},
			})
		}
	}
}

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch

func newVariantReconciler(c client.Client, queues *qcache.Manager, recorder record.EventRecorder, roleTracker *roletracker.RoleTracker) *variantReconciler {
	// func newVariantReconciler(c client.Client, queues *qcache.Manager, cache *schdcache.Cache, recorder record.EventRecorder, roleTracker *roletracker.RoleTracker) *variantReconciler {
	return &variantReconciler{
		logName: ConcurrentAdmissionController,
		client:  c,
		queues:  queues,
		// cache:       cache,
		recorder:    recorder,
		roleTracker: roleTracker,
		clock:       clock.RealClock{},
	}
}

func (r *variantReconciler) logger() logr.Logger {
	return roletracker.WithReplicaRole(ctrl.Log.WithName(r.logName), r.roleTracker)
}

func (r *variantReconciler) setupWithManager(mgr ctrl.Manager, cfg *configapi.Configuration) (string, error) {
	// func (r *variantReconciler) setupWithManager(mgr ctrl.Manager, cache *schdcache.Cache, cfg *configapi.Configuration) (string, error) {
	cqHandler := &clusterQueueHandler{client: r.client, queues: r.queues}
	return ConcurrentAdmissionController, builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named(ConcurrentAdmissionController).
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.Workload{},
			handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, obj *kueue.Workload) []reconcile.Request {
				if isParentVariant(obj) {
					return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(obj)}}
				}
				if isVariant(obj) {
					return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.Namespace, Name: workload.GetParentVariant(obj)}}}
				}
				return nil
			}),
			r,
		)).
		Watches(&kueue.ClusterQueue{}, cqHandler).
		WithOptions(controller.Options{
			NeedLeaderElection:      ptr.To(false),
			MaxConcurrentReconciles: mgr.GetControllerOptions().GroupKindConcurrency[kueue.GroupVersion.WithKind("Workload").GroupKind().String()],
		}).
		WithLogConstructor(roletracker.NewLogConstructor(r.roleTracker, ConcurrentAdmissionController)).
		Complete(core.WithLeadingManager(mgr, r, &kueue.Workload{}, cfg))
}

func SetupControllers(mgr ctrl.Manager, queues *qcache.Manager, cfg *configapi.Configuration, roleTracker *roletracker.RoleTracker) (string, error) {
	// func SetupControllers(mgr ctrl.Manager, queues *qcache.Manager, cache *schdcache.Cache, cfg *configapi.Configuration, roleTracker *roletracker.RoleTracker) (string, error) {
	recorder := mgr.GetEventRecorderFor(ConcurrentAdmissionController)
	variantRec := newVariantReconciler(mgr.GetClient(), queues, recorder, roleTracker)
	// variantRec := newVariantReconciler(mgr.GetClient(), queues, cache, recorder, roleTracker)
	if ctrlName, err := variantRec.setupWithManager(mgr, cfg); err != nil {
		// if ctrlName, err := variantRec.setupWithManager(mgr, cache, cfg); err != nil {
		return ctrlName, err
	}
	return "", nil
}
