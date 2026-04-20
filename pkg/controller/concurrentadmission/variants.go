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

import (
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/workload"
)

func IsVariant(workload *kueue.Workload) bool {
	if workload == nil {
		return false
	}
	return isOwnedByAWorkload(workload)
}

func isOwnedByAWorkload(workload *kueue.Workload) bool {
	if workload == nil {
		return false
	}
	for _, owner := range workload.OwnerReferences {
		if owner.Kind == "Workload" && owner.APIVersion == "kueue.x-k8s.io/v1beta2" {
			return true
		}
	}
	return false
}

func getParentVariant(workload *kueue.Workload) string {
	if workload == nil {
		return ""
	}
	for _, owner := range workload.OwnerReferences {
		if owner.Kind == "Workload" && owner.APIVersion == "kueue.x-k8s.io/v1beta2" {
			return owner.Name
		}
	}
	return ""
}

func getAdmittedVariant(variants []kueue.Workload) *kueue.Workload {
	for i := range variants {
		v := &variants[i]
		if workload.IsAdmitted(v) {
			return v
		}
	}
	return nil
}

func SetParentVariantLabel(workload *kueue.Workload) {
	if workload.Labels == nil {
		workload.Labels = make(map[string]string)
	}
	workload.Labels[constants.ParentVariantLabel] = "true"
}
