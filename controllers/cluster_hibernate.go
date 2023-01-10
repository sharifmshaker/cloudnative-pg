/*
Copyright The CloudNativePG Contributors

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

package controllers

import (
	"context"
	"fmt"

	apierrs "k8s.io/apimachinery/pkg/api/errors"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/log"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

// hibernateCluster removes all instance pods in the cluster, but leaves PVCs
func (r *ClusterReconciler) hibernateCluster(
	ctx context.Context,
	cluster *apiv1.Cluster,
	resources *managedResources,
) (waitingForDeletion bool, err error) {
	contextLogger := log.FromContext(ctx)

	contextLogger.Info("Hibernating cluster",
		"cluster", cluster.Name)

	if areStopped, err := r.allInstancesAreStopped(ctx, resources.instances.Items); err != nil {
		return true, err
	} else if !areStopped {
		contextLogger.Info("instances are not stopped")
		return true, nil
	}

	for _, pvc := range resources.pvcs.Items {
		contextLogger.Info("checking pvc annotations", "pvc", pvc.Name, "annotations", pvc.Annotations)
		pvcStatusAnnotation := pvc.Annotations[specs.PVCStatusAnnotationName]
		_, hasHibernateAnnotation := pvc.Annotations[utils.HibernateClusterManifestAnnotationName]
		_, hasPgControlDataAnnotation := pvc.Annotations[utils.HibernatePgControlDataAnnotationName]
		if !hasPgControlDataAnnotation || !hasHibernateAnnotation || pvcStatusAnnotation != specs.PVCStatusDetached {
			contextLogger.Info("PVC does not have hibernation annotation; continue waiting", "PVC Name", pvc.Name)
			return true, nil
		}
	}

	contextLogger.Info("done waiting for pvcs for cluster hibernation")

	for idx := range resources.instances.Items {
		instance := &resources.instances.Items[idx]
		if err := r.Delete(ctx, instance); err != nil {
			// Ignore if NotFound, otherwise report the error
			if !apierrs.IsNotFound(err) {
				return true, fmt.Errorf("cannot kill the Pod to scale down: %w", err)
			}
		}

		contextLogger.Info("Deleting instance for cluster hibernation",
			"pod", instance.Name)
	}

	return false, nil
}
