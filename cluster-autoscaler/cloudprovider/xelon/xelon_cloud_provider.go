/*
Copyright 2024 The Kubernetes Authors.

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

package xelon

import (
	"io"
	"os"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/klog/v2"
)

var _ cloudprovider.CloudProvider = (*xelonCloudProvider)(nil)

// xelonCloudProvider implements CloudProvider interface.
type xelonCloudProvider struct {
	manager         *Manager
	resourceLimiter *cloudprovider.ResourceLimiter
}

func newXelonCloudProvider(manager *Manager, rl *cloudprovider.ResourceLimiter) (*xelonCloudProvider, error) {
	if err := manager.Refresh(); err != nil {
		return nil, err
	}
	return &xelonCloudProvider{
		manager:         manager,
		resourceLimiter: rl,
	}, nil
}

func (p *xelonCloudProvider) Name() string {
	return cloudprovider.XelonProviderName
}

func (p *xelonCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	nodeGroups := make([]cloudprovider.NodeGroup, len(p.manager.nodeGroups))
	for i, nodeGroup := range p.manager.nodeGroups {
		nodeGroups[i] = nodeGroup
	}
	return nodeGroups
}

func (p *xelonCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	providerID := node.Spec.ProviderID
	nodeID := toNodeID(providerID)

	klog.V(4).InfoS("Checking node groups for node", "node_id", nodeID)
	for _, nodeGroup := range p.manager.nodeGroups {
		klog.V(4).InfoS("Iterating over node group", "node_group_id", nodeGroup.Id())
		nodes, err := nodeGroup.Nodes()
		if err != nil {
			return nil, err
		}

		for _, node := range nodes {
			if node.Id == providerID {
				klog.V(4).InfoS("Found node that matched providerID", "node", node, "provider_id", providerID)
				return nodeGroup, nil
			}
		}
	}

	return nil, nil
}

func (p *xelonCloudProvider) HasInstance(_ *apiv1.Node) (bool, error) {
	return true, cloudprovider.ErrNotImplemented
}

func (p *xelonCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (p *xelonCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (p *xelonCloudProvider) NewNodeGroup(_ string, _ map[string]string, _ map[string]string, _ []apiv1.Taint, _ map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (p *xelonCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return p.resourceLimiter, nil
}

func (p *xelonCloudProvider) GPULabel() string {
	return ""
}

func (p *xelonCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return nil
}

func (p *xelonCloudProvider) GetNodeGpuConfig(node *apiv1.Node) *cloudprovider.GpuConfig {
	return gpu.GetNodeGPUFromCloudProvider(p, node)
}

func (p *xelonCloudProvider) Cleanup() error {
	return nil
}

func (p *xelonCloudProvider) Refresh() error {
	return p.manager.Refresh()
}

// BuildXelon builds the Xelon cloud provider.
func BuildXelon(
	opts config.AutoscalingOptions,
	_ cloudprovider.NodeGroupDiscoveryOptions,
	rl *cloudprovider.ResourceLimiter,
) cloudprovider.CloudProvider {
	var configFile io.ReadCloser
	if opts.CloudConfig != "" {
		configFile, err := os.Open(opts.CloudConfig)
		if err != nil {
			klog.Fatalf("Failed to open Xelon cloud provider configuration %s: %v", opts.CloudConfig, err)
		}
		defer func() {
			_ = configFile.Close()
		}()
	}

	manager, err := newManager(configFile)
	if err != nil {
		klog.Fatalf("Failed to create Xelon manager: %v", err)
	}

	provider, err := newXelonCloudProvider(manager, rl)
	if err != nil {
		klog.Fatalf("Failed to create Xelon cloud provider: %v", err)
	}

	return provider
}
