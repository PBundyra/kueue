/*
Copyright 2022 The Kubernetes Authors.

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
// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	json "encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
	v1alpha1 "sigs.k8s.io/kueue/apis/visibility/v1alpha1"
	visibilityv1alpha1 "sigs.k8s.io/kueue/client-go/applyconfiguration/visibility/v1alpha1"
	scheme "sigs.k8s.io/kueue/client-go/clientset/versioned/scheme"
)

// ClusterQueuesGetter has a method to return a ClusterQueueInterface.
// A group's client should implement this interface.
type ClusterQueuesGetter interface {
	ClusterQueues() ClusterQueueInterface
}

// ClusterQueueInterface has methods to work with ClusterQueue resources.
type ClusterQueueInterface interface {
	Create(ctx context.Context, clusterQueue *v1alpha1.ClusterQueue, opts v1.CreateOptions) (*v1alpha1.ClusterQueue, error)
	Update(ctx context.Context, clusterQueue *v1alpha1.ClusterQueue, opts v1.UpdateOptions) (*v1alpha1.ClusterQueue, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ClusterQueue, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ClusterQueueList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ClusterQueue, err error)
	Apply(ctx context.Context, clusterQueue *visibilityv1alpha1.ClusterQueueApplyConfiguration, opts v1.ApplyOptions) (result *v1alpha1.ClusterQueue, err error)
	GetPendingWorkloadsSummary(ctx context.Context, clusterQueueName string, options v1.GetOptions) (*v1alpha1.PendingWorkloadsSummary, error)

	ClusterQueueExpansion
}

// clusterQueues implements ClusterQueueInterface
type clusterQueues struct {
	client rest.Interface
}

// newClusterQueues returns a ClusterQueues
func newClusterQueues(c *VisibilityV1alpha1Client) *clusterQueues {
	return &clusterQueues{
		client: c.RESTClient(),
	}
}

// Get takes name of the clusterQueue, and returns the corresponding clusterQueue object, and an error if there is any.
func (c *clusterQueues) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.ClusterQueue, err error) {
	result = &v1alpha1.ClusterQueue{}
	err = c.client.Get().
		Resource("clusterqueues").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of ClusterQueues that match those selectors.
func (c *clusterQueues) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.ClusterQueueList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.ClusterQueueList{}
	err = c.client.Get().
		Resource("clusterqueues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested clusterQueues.
func (c *clusterQueues) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Resource("clusterqueues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a clusterQueue and creates it.  Returns the server's representation of the clusterQueue, and an error, if there is any.
func (c *clusterQueues) Create(ctx context.Context, clusterQueue *v1alpha1.ClusterQueue, opts v1.CreateOptions) (result *v1alpha1.ClusterQueue, err error) {
	result = &v1alpha1.ClusterQueue{}
	err = c.client.Post().
		Resource("clusterqueues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(clusterQueue).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a clusterQueue and updates it. Returns the server's representation of the clusterQueue, and an error, if there is any.
func (c *clusterQueues) Update(ctx context.Context, clusterQueue *v1alpha1.ClusterQueue, opts v1.UpdateOptions) (result *v1alpha1.ClusterQueue, err error) {
	result = &v1alpha1.ClusterQueue{}
	err = c.client.Put().
		Resource("clusterqueues").
		Name(clusterQueue.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(clusterQueue).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the clusterQueue and deletes it. Returns an error if one occurs.
func (c *clusterQueues) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Resource("clusterqueues").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *clusterQueues) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Resource("clusterqueues").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched clusterQueue.
func (c *clusterQueues) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ClusterQueue, err error) {
	result = &v1alpha1.ClusterQueue{}
	err = c.client.Patch(pt).
		Resource("clusterqueues").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

// Apply takes the given apply declarative configuration, applies it and returns the applied clusterQueue.
func (c *clusterQueues) Apply(ctx context.Context, clusterQueue *visibilityv1alpha1.ClusterQueueApplyConfiguration, opts v1.ApplyOptions) (result *v1alpha1.ClusterQueue, err error) {
	if clusterQueue == nil {
		return nil, fmt.Errorf("clusterQueue provided to Apply must not be nil")
	}
	patchOpts := opts.ToPatchOptions()
	data, err := json.Marshal(clusterQueue)
	if err != nil {
		return nil, err
	}
	name := clusterQueue.Name
	if name == nil {
		return nil, fmt.Errorf("clusterQueue.Name must be provided to Apply")
	}
	result = &v1alpha1.ClusterQueue{}
	err = c.client.Patch(types.ApplyPatchType).
		Resource("clusterqueues").
		Name(*name).
		VersionedParams(&patchOpts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

// GetPendingWorkloadsSummary takes name of the clusterQueue, and returns the corresponding v1alpha1.PendingWorkloadsSummary object, and an error if there is any.
func (c *clusterQueues) GetPendingWorkloadsSummary(ctx context.Context, clusterQueueName string, options v1.GetOptions) (result *v1alpha1.PendingWorkloadsSummary, err error) {
	result = &v1alpha1.PendingWorkloadsSummary{}
	err = c.client.Get().
		Resource("clusterqueues").
		Name(clusterQueueName).
		SubResource("pendingworkloads").
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}