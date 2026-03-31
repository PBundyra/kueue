package workloadvariant

import (
	"context"

	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
)

// WorkloadVariantReconciler reconciles a Workload's variants.
type WorkloadVariantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch

func (r *WorkloadVariantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(5).Info("Reconciling Workload")

	var wl kueue.Workload
	if err := r.Get(ctx, req.NamespacedName, &wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if wl.Labels["kueue.x-k8s.io/parent-variant"] == "true" {
		return r.reconcileParent(ctx, &wl)
	}

	for _, owner := range wl.OwnerReferences {
		if owner.Kind == "Workload" && owner.APIVersion == kueue.GroupVersion.String() {
			return ctrl.Result{}, r.reconcileParentByName(ctx, types.NamespacedName{Namespace: wl.Namespace, Name: owner.Name})
		}
	}

	return ctrl.Result{}, nil
}

func (r *WorkloadVariantReconciler) reconcileParentByName(ctx context.Context, name types.NamespacedName) error {
	var parent kueue.Workload
	if err := r.Get(ctx, name, &parent); err != nil {
		return client.IgnoreNotFound(err)
	}
	_, err := r.reconcileParent(ctx, &parent)
	return err
}

func (r *WorkloadVariantReconciler) reconcileParent(ctx context.Context, parent *kueue.Workload) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("parent", klog.KObj(parent))
	log.V(2).Info("Reconciling Parent Workload")

	variants, err := r.listVariants(ctx, parent)
	if err != nil {
		return ctrl.Result{}, err
	}

	var lq kueue.LocalQueue
	if err := r.Get(ctx, types.NamespacedName{Namespace: parent.Namespace, Name: string(parent.Spec.QueueName)}, &lq); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var cq kueue.ClusterQueue
	if err := r.Get(ctx, types.NamespacedName{Name: string(lq.Spec.ClusterQueue)}, &cq); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if cq.Spec.ConcurrentAdmission == nil {
		return ctrl.Result{}, nil
	}

	if len(cq.Spec.ResourceGroups) == 0 {
		return ctrl.Result{}, nil
	}

	rg := cq.Spec.ResourceGroups[0]

	existingByFlavor := make(map[string]*kueue.Workload)
	for i := range variants {
		v := &variants[i]
		if v.Spec.AdmissionConstraints != nil && len(v.Spec.AdmissionConstraints.AllowedResourceFlavors) > 0 {
			flavor := string(v.Spec.AdmissionConstraints.AllowedResourceFlavors[0].Name)
			existingByFlavor[flavor] = v
		}
	}

	for _, flavorQuota := range rg.Flavors {
		flavorName := string(flavorQuota.Name)
		if _, exists := existingByFlavor[flavorName]; !exists {
			if err := r.createVariant(ctx, parent, flavorQuota.Name); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	var minAdmittedIdx *int
	for _, v := range variants {
		if v.Status.Admission != nil {
			var flavorName string
			if v.Spec.AdmissionConstraints != nil && len(v.Spec.AdmissionConstraints.AllowedResourceFlavors) > 0 {
				flavorName = string(v.Spec.AdmissionConstraints.AllowedResourceFlavors[0].Name)
			}
			if flavorName == "" {
				continue
			}
			idx := -1
			for j, f := range rg.Flavors {
				if string(f.Name) == flavorName {
					idx = j
					break
				}
			}
			if idx != -1 {
				if minAdmittedIdx == nil || idx < *minAdmittedIdx {
					minAdmittedIdx = &idx
				}
			}
		}
	}

	if minAdmittedIdx != nil {
		for i := range variants {
			v := &variants[i]
			var flavorName string
			if v.Spec.AdmissionConstraints != nil && len(v.Spec.AdmissionConstraints.AllowedResourceFlavors) > 0 {
				flavorName = string(v.Spec.AdmissionConstraints.AllowedResourceFlavors[0].Name)
			}
			if flavorName == "" {
				continue
			}
			idx := -1
			for j, f := range rg.Flavors {
				if string(f.Name) == flavorName {
					idx = j
					break
				}
			}
			if idx != -1 && idx > *minAdmittedIdx {
				if v.Spec.Active == nil || *v.Spec.Active {
					vCopy := v.DeepCopy()
					vCopy.Spec.Active = ptr.To(false)
					if err := r.Update(ctx, vCopy); err != nil {
						return ctrl.Result{}, err
					}
					log.V(2).Info("Deactivated less favorable variant", "variant", klog.KObj(v), "reason", "more favorable variant admitted")
				}
			}
		}
	}

	var admittedVariant *kueue.Workload
	for _, v := range variants {
		if v.Status.Admission != nil {
			admittedVariant = &v
			break
		}
	}

	if admittedVariant != nil {
		if parent.Status.Admission == nil {
			parentCopy := parent.DeepCopy()
			parentCopy.Status.Admission = admittedVariant.Status.Admission.DeepCopy()
			for _, c := range admittedVariant.Status.Conditions {
				if c.Type == kueue.WorkloadQuotaReserved {
					apimeta.SetStatusCondition(&parentCopy.Status.Conditions, c)
				}
			}
			if err := r.Status().Update(ctx, parentCopy); err != nil {
				return ctrl.Result{}, err
			}
			log.V(2).Info("Aggregated status from admitted variant to parent", "variant", klog.KObj(admittedVariant))
		}
	}

	return ctrl.Result{}, nil
}

func (r *WorkloadVariantReconciler) createVariant(ctx context.Context, parent *kueue.Workload, flavor kueue.ResourceFlavorReference) error {
	log := ctrl.LoggerFrom(ctx).WithValues("parent", klog.KObj(parent), "flavor", flavor)
	log.V(2).Info("Creating Variant Workload")

	variantName := fmt.Sprintf("%s-v-%s", parent.Name, flavor)

	variant := &kueue.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      variantName,
			Namespace: parent.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(parent, kueue.GroupVersion.WithKind("Workload")),
			},
		},
		Spec: *parent.Spec.DeepCopy(),
	}

	variant.Spec.AdmissionConstraints = &kueue.AdmissionConstraints{
		AllowedResourceFlavors: []kueue.AllowedResourceFlavor{
			{Name: flavor},
		},
	}

	if err := r.Create(ctx, variant); err != nil {
		return err
	}

	log.V(2).Info("Created Variant", "variant", klog.KObj(variant))
	return nil
}

func (r *WorkloadVariantReconciler) listVariants(ctx context.Context, parent *kueue.Workload) ([]kueue.Workload, error) {
	var list kueue.WorkloadList
	gvk := kueue.GroupVersion.WithKind("Workload")
	err := r.List(ctx, &list, client.InNamespace(parent.Namespace), coreindexer.OwnerReferenceIndexFieldMatcher(gvk, parent.Name))
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *WorkloadVariantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	gvk := kueue.GroupVersion.WithKind("Workload")
	if err := coreindexer.SetupWorkloadOwnerIndex(context.Background(), mgr.GetFieldIndexer(), gvk); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&kueue.Workload{}).
		Complete(r)
}
