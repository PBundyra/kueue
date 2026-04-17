// /*
// Copyright The Kubernetes Authors.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package concurrentadmission

// import (
// 	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
// )

// const (
// 	// ParentVariantLabel is the label key in the Workload that is a parent of Variants
// 	// The value of this label is boolean, and it is set to "true" if the Workload is a parent of Variants.
// 	ParentVariantLabel = "kueue.x-k8s.io/parent-variant"
// )

// func isParentVariant(workload *kueue.Workload) bool {
// 	if workload == nil {
// 		return false
// 	}
// 	val, ok := workload.Labels[ParentVariantLabel]
// 	return ok && val == "true"
// }

// func isVariant(workload *kueue.Workload) bool {
// 	if workload == nil {
// 		return false
// 	}
// 	return isOwnedByAWorkload(workload)
// }

// func isOwnedByAWorkload(workload *kueue.Workload) bool {
// 	if workload == nil {
// 		return false
// 	}
// 	for _, owner := range workload.OwnerReferences {
// 		if owner.Kind == "Workload" && owner.APIVersion == "kueue.x-k8s.io/v1beta2" {
// 			return true
// 		}
// 	}
// 	return false
// }
