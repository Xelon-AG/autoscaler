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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Xelon-AG/xelon-sdk-go/xelon"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

var _ cloudprovider.NodeGroup = (*NodeGroup)(nil)

const xelonProviderIDPrefix = "xelon://"

// NodeGroup implements cloudprovider.NodeGroup interface.
type NodeGroup struct {
	client    nodeGroupClient
	clusterID string
	id        string
	nodePool  *xelon.ClusterPool
	minSize   int
	maxSize   int
}

func (ng *NodeGroup) MaxSize() int {
	return ng.maxSize
}

func (ng *NodeGroup) MinSize() int {
	return ng.minSize
}

// TargetSize returns the current target size of the node group. It is possible
// that the number of nodes in Kubernetes is different at the moment but should
// be equal to Size() once everything stabilizes.
func (ng *NodeGroup) TargetSize() (int, error) {
	if ng.nodePool == nil {
		klog.V(2).InfoS("Empty node pool in the node group", "node_group", ng)
		return 0, errors.New("node pool instance is not created")
	}
	return len(ng.nodePool.Nodes), nil
}

func (ng *NodeGroup) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("delta must be postivie, have: %d", delta)
	}

	currentSize := len(ng.nodePool.Nodes)
	targetSize := currentSize + delta
	if targetSize > ng.maxSize {
		return fmt.Errorf("size increase is too large, current: %d, desired: %d, max: %d",
			currentSize, targetSize, ng.MaxSize())
	}

	ctx := context.Background()
	for i := 0; i < delta; i++ {
		_, _, err := ng.client.AddClusterNode(ctx, ng.clusterID, ng.Id())
		if err != nil {
			return err
		}
	}

	return nil
}

func (ng *NodeGroup) AtomicIncreaseSize(_ int) error {
	return cloudprovider.ErrNotImplemented
}

func (ng *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	ctx := context.Background()
	for _, node := range nodes {
		nodeID := toNodeID(node.Spec.ProviderID)
		klog.V(2).InfoS("Deleting Xelon cluster node", "node_id", nodeID)
		_, _, err := ng.client.DeleteClusterNode(ctx, ng.clusterID, nodeID)
		if err != nil {
			return fmt.Errorf("failed to delete node from cluster: %s, node: %s, error: %s",
				ng.clusterID, node.Name, err)
		}
	}
	return nil
}

func (ng *NodeGroup) DecreaseTargetSize(_ int) error {
	// requests for new nodes are always fulfilled so we cannot
	// decrease the size without actually deleting nodes
	return cloudprovider.ErrNotImplemented
}

func (ng *NodeGroup) Id() string {
	return ng.id
}

func (ng *NodeGroup) Debug() string {
	return fmt.Sprintf("id: %s (min:%d max:%d)", ng.Id(), ng.MinSize(), ng.MaxSize())
}

func (ng *NodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	if ng.nodePool == nil {
		return nil, errors.New("node pool instance is not created")
	}
	klog.V(4).InfoS("Mapping node pool nodes to instances", "nodes", ng.nodePool.Nodes)
	instances := toInstances(ng.nodePool.Nodes)
	klog.V(4).InfoS("Mapped instances", "instances", instances)
	return instances, nil
}

func (ng *NodeGroup) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (ng *NodeGroup) Exist() bool {
	return true
}

func (ng *NodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (ng *NodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (ng *NodeGroup) Autoprovisioned() bool {
	return false
}

func (ng *NodeGroup) GetOptions(_ config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func toInstances(nodes []xelon.ClusterPoolNode) []cloudprovider.Instance {
	instances := make([]cloudprovider.Instance, 0, len(nodes))
	for _, node := range nodes {
		instances = append(instances, toInstance(node))
	}
	return instances
}

func toInstance(node xelon.ClusterPoolNode) cloudprovider.Instance {
	klog.V(4).InfoS("toInstance", "node", node, "provider_id", toProviderID(node.LocalVMID))
	return cloudprovider.Instance{
		Id:     toProviderID(node.LocalVMID),
		Status: toInstanceStatus(node.Status),
	}
}

func toInstanceStatus(nodeState string) *cloudprovider.InstanceStatus {
	if nodeState == "" {
		return nil
	}

	st := &cloudprovider.InstanceStatus{}
	switch nodeState {
	case "Created":
		st.State = cloudprovider.InstanceCreating
	case "Deployed":
		st.State = cloudprovider.InstanceRunning
	case "Deleting":
		st.State = cloudprovider.InstanceDeleting
	default:
		st.ErrorInfo = &cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    "no-code-xelon",
			ErrorMessage: nodeState,
		}
	}

	return st
}

// toProviderID returns a provider ID from the given node ID.
func toProviderID(nodeID string) string {
	return fmt.Sprintf("%s%s", xelonProviderIDPrefix, nodeID)
}

// toNodeID returns a node ID from the given provider ID.
func toNodeID(providerID string) string {
	return strings.TrimPrefix(providerID, xelonProviderIDPrefix)
}
