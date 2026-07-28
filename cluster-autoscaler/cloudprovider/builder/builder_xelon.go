//go:build xelon
// +build xelon

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

package builder

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/xelon"
	coreoptions "k8s.io/autoscaler/cluster-autoscaler/core/options"
	"k8s.io/client-go/informers"
)

// AvailableCloudProviders contains the provider in the Xelon-only binary.
var AvailableCloudProviders = []string{cloudprovider.XelonProviderName}

// DefaultCloudProvider for the Xelon-only binary is Xelon.
const DefaultCloudProvider = cloudprovider.XelonProviderName

func buildCloudProvider(opts *coreoptions.AutoscalerOptions, discovery cloudprovider.NodeGroupDiscoveryOptions, limiter *cloudprovider.ResourceLimiter, _ informers.SharedInformerFactory) cloudprovider.CloudProvider {
	if opts.CloudProviderName == cloudprovider.XelonProviderName {
		return xelon.BuildXelon(opts, discovery, limiter)
	}
	return nil
}
