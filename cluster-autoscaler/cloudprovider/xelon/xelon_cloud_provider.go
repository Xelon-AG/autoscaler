/*
Copyright 2026 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	xelonsdk "github.com/Xelon-AG/xelon-sdk-go/xelon"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	coreoptions "k8s.io/autoscaler/cluster-autoscaler/core/options"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/version"
	"k8s.io/klog/v2"
)

var _ cloudprovider.CloudProvider = (*xelonCloudProvider)(nil)

// BuildVersion and BuildRevision are set by the Xelon image build. They remain
// variables so local builds can inject them with -ldflags -X.
var (
	BuildVersion  = "development"
	BuildRevision = "unknown"
)

type xelonCloudProvider struct {
	nodeGroup       *NodeGroup
	resourceLimiter *cloudprovider.ResourceLimiter
}

func newXelonCloudProvider(client kubernetesService, config *Config, poolID string, minSize, maxSize int, limiter *cloudprovider.ResourceLimiter) (*xelonCloudProvider, error) {
	group := newNodeGroup(client, config.ClusterID, poolID, minSize, maxSize)
	snapshot, err := group.fetchPublicSnapshot(context.Background())
	if err != nil {
		return nil, fmt.Errorf("validate configured XKS worker pool: %w", err)
	}
	if snapshot.targetSize < minSize || snapshot.targetSize > maxSize {
		return nil, fmt.Errorf("configured XKS worker pool target %d is outside bounds %d:%d", snapshot.targetSize, minSize, maxSize)
	}
	return &xelonCloudProvider{nodeGroup: group, resourceLimiter: limiter}, nil
}

func (provider *xelonCloudProvider) Name() string { return cloudprovider.XelonProviderName }

func (provider *xelonCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	return []cloudprovider.NodeGroup{provider.nodeGroup}
}

func (provider *xelonCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	localVMID, err := parseProviderID(node.Spec.ProviderID)
	if errors.Is(err, errForeignProviderID) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("parse provider ID for Kubernetes node %q: %w", node.Name, err)
	}
	snapshot, err := provider.nodeGroup.fetchPublicSnapshot(context.Background())
	if err != nil {
		return nil, err
	}
	if _, found := snapshot.workersByLocalID[localVMID]; !found {
		return nil, nil
	}
	return provider.nodeGroup, nil
}

func (provider *xelonCloudProvider) HasInstance(*apiv1.Node) (bool, error) {
	return false, cloudprovider.ErrNotImplemented
}

func (provider *xelonCloudProvider) Pricing() (cloudprovider.PricingModel, caerrors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (provider *xelonCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (provider *xelonCloudProvider) NewNodeGroup(string, map[string]string, map[string]string, []apiv1.Taint, map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (provider *xelonCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return provider.resourceLimiter, nil
}

func (provider *xelonCloudProvider) GPULabel() string { return "" }

func (provider *xelonCloudProvider) GetAvailableGPUTypes() map[string]struct{} { return nil }

func (provider *xelonCloudProvider) GetNodeGpuConfig(*apiv1.Node) *cloudprovider.GpuConfig {
	return nil
}

func (provider *xelonCloudProvider) Cleanup() error { return nil }

func (provider *xelonCloudProvider) Refresh() error {
	_, err := provider.nodeGroup.fetchPublicSnapshot(context.Background())
	return err
}

// BuildXelon constructs the Xelon provider from one explicit --nodes entry.
func BuildXelon(opts *coreoptions.AutoscalerOptions, discovery cloudprovider.NodeGroupDiscoveryOptions, limiter *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {
	provider, err := buildXelon(opts, discovery, limiter)
	if err != nil {
		klog.Fatalf("Failed to build Xelon cloud provider: %v", err)
	}
	return provider
}

func buildXelon(opts *coreoptions.AutoscalerOptions, discovery cloudprovider.NodeGroupDiscoveryOptions, limiter *cloudprovider.ResourceLimiter) (cloudprovider.CloudProvider, error) {
	if len(discovery.NodeGroupAutoDiscoverySpecs) != 0 {
		return nil, fmt.Errorf("Xelon v0 does not support node group autodiscovery")
	}
	if len(discovery.NodeGroupSpecs) != 1 {
		return nil, fmt.Errorf("Xelon v0 requires exactly one --nodes entry, got %d", len(discovery.NodeGroupSpecs))
	}
	spec, err := dynamic.SpecFromString(discovery.NodeGroupSpecs[0], false)
	if err != nil {
		return nil, fmt.Errorf("parse Xelon --nodes entry: %w", err)
	}

	configReader, err := openCloudConfig(opts.CloudConfig)
	if err != nil {
		return nil, err
	}
	defer func() { _ = configReader.Close() }()
	config, err := readConfig(configReader)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: defaultRequestTimeout}
	clientOptions := []xelonsdk.ClientOption{
		xelonsdk.WithClientID(config.ClientID),
		xelonsdk.WithHTTPClient(httpClient),
		xelonsdk.WithUserAgent(fmt.Sprintf("cluster-autoscaler-xelon/%s ca/%s", BuildVersion, version.ClusterAutoscalerVersion)),
	}
	if config.BaseURL != "" {
		clientOptions = append(clientOptions, xelonsdk.WithBaseURL(config.BaseURL))
	}
	client := xelonsdk.NewClient(config.Token, clientOptions...)
	provider, err := newXelonCloudProvider(client.Kubernetes, config, spec.Name, spec.MinSize, spec.MaxSize, limiter)
	if err != nil {
		return nil, err
	}
	klog.Infof("Built Xelon Cluster Autoscaler provider version=%s revision=%s upstream=%s", BuildVersion, BuildRevision, version.ClusterAutoscalerVersion)
	return provider, nil
}

func openCloudConfig(path string) (io.ReadCloser, error) {
	if path == "" {
		return nil, fmt.Errorf("--cloud-config is required for Xelon")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Xelon cloud config %q: %w", path, err)
	}
	return file, nil
}
