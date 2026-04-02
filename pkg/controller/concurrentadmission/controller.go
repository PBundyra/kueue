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
	// "sync"
	"time"

	"github.com/go-logr/logr"
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
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	ConcurrentAdmissionController = "concurrent-admission-controller"
)

type variantReconciler struct {
	logName     string
	queues      *qcache.Manager
	cache       *schdcache.Cache
	client      client.Client
	recorder    record.EventRecorder
	roleTracker *roletracker.RoleTracker
	// mutex       sync.Mutex
	clock       clock.Clock
}

var _ reconcile.Reconciler = (*variantReconciler)(nil)
var _ predicate.TypedPredicate[*kueue.Workload] = (*variantReconciler)(nil)

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
		if getParentVariant(&list.Items[i]) != wl.Name {
			continue
		}
		variants = append(variants, list.Items[i])
	}
	return variants, nil
}

func (r *variantReconciler) getParent(wl *kueue.Workload) (*kueue.Workload, error) {
	if !isVariant(wl) {
		return nil, fmt.Errorf("workload %s/%s is not a variant", wl.Namespace, wl.Name)
	}
	parentName := getParentVariant(wl)
	parent := &kueue.Workload{}
	if err := r.client.Get(context.Background(), client.ObjectKey{Name: parentName, Namespace: wl.Namespace}, parent); err != nil {
		return nil, err
	}
	return parent, nil
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

// func (r *variantReconciler) getWorkloadFamily(wl *kueue.Workload) (parent *kueue.Workload, variants []kueue.Workload, err error) {
// 	if isParentVariant(wl) {
// 		variants, err := r.getVariants(wl)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		return wl, variants, nil
// 	}
// 	if isVariant(wl) {
// 		parent, err := r.getParent(wl)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		variants, err := r.getVariants(parent)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		return parent, variants, nil
// 	}
// 	return nil, nil, fmt.Errorf("workload %s/%s is neither a parent variant nor a variant", wl.Namespace, wl.Name)
// }

func getAdmittedVariant(variants []kueue.Workload) *kueue.Workload {
	for _, wl := range variants {
		if workload.IsAdmitted(&wl) {
			return &wl
		}
	}
	return nil
}

func (r *variantReconciler) deactivateVariant(ctx context.Context, v *kueue.Workload) error {
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

func (r *variantReconciler) deactivateVariants(ctx context.Context, variants []kueue.Workload, cq *kueue.ClusterQueue) error {
	admittedWl := getAdmittedVariant(variants)
	if admittedWl == nil {
		r.logger().V(2).Info("No admitted variant, no need to deactivate any variant")
		return nil
	}
	flavorOrder := make(map[kueue.ResourceFlavorReference]int)
	for i, flavor := range cq.Spec.ResourceGroups[0].Flavors {
		flavorOrder[flavor.Name] = i
	}

	// deactivate Variants below minTargetFlavor if specified
	minTargetFlavor := cq.Spec.ConcurrentAdmission.MigrationConstraints.MinTargetFlavor
	if minTargetFlavor != nil {
		r.logger().V(2).Info("Deactivating variants below minTargetFlavor", "minTargetFlavor", *minTargetFlavor)
		for _, v := range variants {
			if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] > flavorOrder[*minTargetFlavor] {
				r.logger().V(2).Info("Deactivating variant because it is below the minTargetFlavor", "variant", v.Name, "flavor", v.Spec.AdmissionConstraints.AllowedResourceFlavors[0], "minTargetFlavor", *minTargetFlavor)
				if err := r.deactivateVariant(ctx, &v); err != nil {
					return err
				}
			}
		}
		return nil
	}

	r.logger().V(2).Info("Deactivating variants below the admitted variant", "admittedVariant", admittedWl.Name, "admittedFlavor", admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0])
	// deactivate Variants below the admitted variant
	for _, v := range variants {
		if flavorOrder[v.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] > flavorOrder[admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0]] {
			r.logger().V(2).Info("Deactivating variant because it is below the admitted variant", "variant", v.Name, "flavor", v.Spec.AdmissionConstraints.AllowedResourceFlavors[0], "admittedFlavor", admittedWl.Spec.AdmissionConstraints.AllowedResourceFlavors[0])
			if err := r.deactivateVariant(ctx, &v); err != nil {
				return err
			}
		}
	}
	return nil
}

// sort variants based on the order of resource flavors in the cluster queue, the variant with the flavor that is first in the order should be admitted first
func sortVariantsByFlavorOrder(variants []kueue.Workload, cq *kueue.ClusterQueue) []kueue.Workload {
	flavorOrder := make(map[kueue.ResourceFlavorReference]int)
	for i, flavor := range cq.Spec.ResourceGroups[0].Flavors {
		flavorOrder[flavor.Name] = i
	}
	slices.SortFunc(variants, func(a, b kueue.Workload) int {
		aFlavor := a.Spec.AdmissionConstraints.AllowedResourceFlavors[0] // we only support one flavor per variant for now
		bFlavor := b.Spec.AdmissionConstraints.AllowedResourceFlavors[0]
		return flavorOrder[aFlavor] - flavorOrder[bFlavor]
	})
	return variants
}

func (r *variantReconciler) syncAdmissionStatus(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload) error {
	if workload.IsFinished(parent) {
		finishCond := apimeta.FindStatusCondition(parent.Status.Conditions, kueue.WorkloadFinished)
		reason := finishCond.Reason
		message := finishCond.Message
		// parent finished, deactivate all variants and set them to finished
		for _, v := range variants {
			if err := workload.Finish(ctx, r.client, &v, reason, message, r.clock); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// TODO: handle finished parent workload and eviction case, for eviction we should reset the status of all variants to pending and reactivate them if needed, for finished workloads we should set all variants to finished
	admittedVariant := getAdmittedVariant(variants)
	switch {
	case admittedVariant == nil && workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent is admitted but no variant is admitted, updating parent to not admitted", "parent", parent.Name)
		// handle the case where the variant got evicted, evict the parent as well to trigger the requeue and reactivate the variants if needed
		// check if the variant got evicted first, and then evict the parent
		variantEvictedCond := apimeta.FindStatusCondition(admittedVariant.Status.Conditions, kueue.WorkloadEvicted)
		if variantEvictedCond != nil && variantEvictedCond.Status == metav1.ConditionTrue {
			r.logger().V(2).Info("Admitted variant is evicted, evicting the parent workload", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
			if err := workload.Evict(ctx, r.client, parent, variantEvictedCond.Reason, variantEvictedCond.Message, r.clock); err != nil {
				return err
			}
		}
		// TODO: figure out what to do with delayed requeueing e.g. waitForPodsReady, and AdmissionChecks
		


		// delete the WorkloadAdmitted condition from the parent
		// reactivate all variants
		// TODO: check if there are any other cases than preemption
	case admittedVariant != nil && !workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent is not admitted but a variant is admitted, updating parent to admitted", "parent", parent.Name, "admittedVariant", admittedVariant.Name)

		if err := workload.PatchAdmissionStatus(ctx, r.client, parent, r.clock, func(wl *kueue.Workload) (bool, error) {
			wl.Status.Admission = admittedVariant.Status.Admission
			r.logger().V(2).Info("Parent admission status", "admission", wl.Status.Admission)
			workload.SetQuotaReservation(wl, wl.Status.Admission, r.clock)
			admittedCond := metav1.Condition{
				Type:               kueue.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				Message:            fmt.Sprintf("The variant %s is admitted", admittedVariant.Name),
				ObservedGeneration: parent.Generation,
				LastTransitionTime: metav1.NewTime(time.Now()),
			}
			apimeta.SetStatusCondition(&wl.Status.Conditions, admittedCond)
			r.logger().V(2).Info("Parent status at the end of patch", "conditions", wl.Status.Conditions, "admission", wl.Status.Admission)
			return true, nil
		}); err != nil {
			return client.IgnoreNotFound(err)
		}

	case admittedVariant != nil && workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent and admitted variant are both admitted, no action needed", "parent", parent.Name, "admittedVariant", admittedVariant.Name)
	case admittedVariant == nil && !workload.IsAdmitted(parent):
		r.logger().V(2).Info("Parent and variants are both not admitted, no action needed", "parent", parent.Name)
	}

	return nil
}

// Create variants based on the flavors in the cluster queue, each variant should have a different resource flavors in the spec allowed flavors, and owner reference to the parent workload
func (r *variantReconciler) createVariants(ctx context.Context, parent *kueue.Workload, variants []kueue.Workload, resourceFlavors []kueue.ResourceFlavorReference) error {
	log := ctrl.LoggerFrom(ctx)
	for _, flavor := range resourceFlavors {
		// check if a variant with the same flavor already exists
		variantExists := false
		for _, v := range variants {
			log.V(2).Info("Checking if variant has the same flavor", "variant", v.Name, "flavor", v.Spec.AdmissionConstraints.AllowedResourceFlavors[0], "desiredFlavor", flavor)
			if v.Spec.AdmissionConstraints.AllowedResourceFlavors[0] == flavor {
				variantExists = true
				log.V(2).Info("Variant with the same flavor already exists, no action needed", "variant", v.Name, "flavor", flavor)
				break
			}
		}
		if variantExists {
			continue
		}

		variant := &kueue.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-variant-%s", parent.Name, flavor),
				Namespace:    parent.Namespace,
				Labels:       parent.Labels,
				Annotations:  parent.Annotations,
			},
			Spec: parent.Spec,
		}
		// delete parent label from variants
		delete(variant.Labels, workload.ParentVariantLabel)
		variant.Spec.AdmissionConstraints = &kueue.AdmissionConstraints{
			// we only support one flavor per variant for now, we can extend this in the future if needed
			AllowedResourceFlavors: []kueue.ResourceFlavorReference{flavor},
		}
		log.V(2).Info("Creating variant for flavor", "flavor", flavor, "variant", variant.Name)

		// Set the owner reference to the parent workload
		if err := ctrl.SetControllerReference(parent, variant, r.client.Scheme()); err != nil {
			return err
		}
		// Create the variant
		if err := r.client.Create(ctx, variant); err != nil {
			return err
		}
		r.logger().V(2).Info("Created variant", "variant", variant.Name, "flavor", flavor)
	}

	return nil
}

// Reconcile reconciles only Parent Worklaods
func (r *variantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Reconcile Workload")

	wl := &kueue.Workload{}
	if err := r.client.Get(ctx, req.NamespacedName, wl); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(2).Info("Workload not found, might have been deleted", "name", req.Name, "namespace", req.Namespace)
			// TODO: add delete logic if needed
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Safety check: The map func and predicates should only ever send us Parent Workloads
	if !isParentVariant(wl) {
		return ctrl.Result{}, nil
	}
	parent := wl
	variants, err := r.getVariants(parent)
	if err != nil {
		return ctrl.Result{}, err
	}
	cq, err := r.getClusterQueue(parent)
	// TODO: add logic that deletes all Workloads if ConcurrentAdmission is disabled
	if err != nil {
		return ctrl.Result{}, err
	}

	log.V(2).Info("Found Workload family and ClusterQueue", "parent", parent.Name, "variants", len(variants), "clusterQueue", cq.Name)

	flavorsInCq := make([]kueue.ResourceFlavorReference, len(cq.Spec.ResourceGroups[0].Flavors))
	for i, flavor := range cq.Spec.ResourceGroups[0].Flavors {
		flavorsInCq[i] = flavor.Name
	}

	variants = sortVariantsByFlavorOrder(variants, cq)
	log.V(2).Info("Sorted variants by flavor order", "variants", func() []string {
		names := make([]string, len(variants))
		for i, v := range variants {
			names[i] = v.Name
		}
		return names
	}())
	// sort variants based on the order of resource flavors in the cluster queue, the variant with the flavor that is first in the order should be admitted first

	if len(variants) > len(flavorsInCq) {
		log.V(2).Info("Too many variants, deleting the excess ones", "desired", len(flavorsInCq), "actual", len(variants))
	}
	if len(variants) < len(flavorsInCq) {
		log.V(2).Info("Too few variants, creating new ones", "desired", len(flavorsInCq), "actual", len(variants))
		if err := r.createVariants(ctx, parent, variants, flavorsInCq); err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(variants) == len(flavorsInCq) {
		log.V(2).Info("Desired number of variants, no action needed", "desired", len(flavorsInCq), "actual", len(variants))
	}

	// respect the concurrent admission policy from the ClusterQueue about deactivation
	constraints := cq.Spec.ConcurrentAdmission.MigrationConstraints
	if constraints.Mode == kueue.ConcurrentAdmissionUpgradeOnly {
		// if one variant is admitted deactivate all variants that are below the minFlavorTarget if present, or below the admitted variant otherwise
		log.V(2).Info("Concurrent admission policy is UpgradeOnly, deactivating variants if needed")
		if err := r.deactivateVariants(ctx, variants, cq); err != nil {
			r.logger().V(2).Info("Failed to deactivate variants", "error", err)
			return ctrl.Result{}, err
		}
	}

	if err := r.syncAdmissionStatus(ctx, parent, variants); err != nil {
		log.V(2).Info("Failed to sync admission status", "error", err)
		return ctrl.Result{}, err
	}
	// TODO: add logic to reactivate variants if the parent Workload is evicted and requeued

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
}

func (h *clusterQueueHandler) Create(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForCQ(e.Object, q)
}

func (h *clusterQueueHandler) Update(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForCQ(e.ObjectNew, q)
}

func (h *clusterQueueHandler) Delete(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForCQ(e.Object, q)
}

func (h *clusterQueueHandler) Generic(_ context.Context, _ event.GenericEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *clusterQueueHandler) queueReconcileForCQ(object client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch

func newVariantReconciler(c client.Client, queues *qcache.Manager, cache *schdcache.Cache, recorder record.EventRecorder, roleTracker *roletracker.RoleTracker) *variantReconciler {
	return &variantReconciler{
		logName:     ConcurrentAdmissionController,
		client:      c,
		queues:      queues,
		cache:       cache,
		recorder:    recorder,
		roleTracker: roleTracker,
		clock:       clock.RealClock{},
	}
}

func (r *variantReconciler) logger() logr.Logger {
	return roletracker.WithReplicaRole(ctrl.Log.WithName(r.logName), r.roleTracker)
}

func (r *variantReconciler) setupWithManager(mgr ctrl.Manager, cache *schdcache.Cache, cfg *configapi.Configuration) (string, error) {
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
					return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.Namespace, Name: getParentVariant(obj)}}}
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

func SetupControllers(mgr ctrl.Manager, queues *qcache.Manager, cache *schdcache.Cache, cfg *configapi.Configuration, roleTracker *roletracker.RoleTracker) (string, error) {
	recorder := mgr.GetEventRecorderFor(ConcurrentAdmissionController)
	variantRec := newVariantReconciler(mgr.GetClient(), queues, cache, recorder, roleTracker)
	if ctrlName, err := variantRec.setupWithManager(mgr, cache, cfg); err != nil {
		return ctrlName, err
	}
	return "", nil
}
