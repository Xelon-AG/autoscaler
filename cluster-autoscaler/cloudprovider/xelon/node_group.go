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

package xelon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"

	xelonsdk "github.com/Xelon-AG/xelon-sdk-go/xelon"
)

var (
	_ cloudprovider.NodeGroup                                   = (*NodeGroup)(nil)
	_ cloudprovider.NodeGroupWithProviderConfirmedUpcomingNodes = (*NodeGroup)(nil)
)

const (
	defaultRequestTimeout        = 30 * time.Second
	defaultPollInterval          = 2 * time.Second
	defaultReconciliationTimeout = 5 * time.Minute
)

type kubernetesService interface {
	GetNodePool(context.Context, string, string) (*xelonsdk.KubernetesClusterNodePool, *xelonsdk.Response, error)
	CreateNode(context.Context, string, string) (*xelonsdk.Response, error)
	DeleteNode(context.Context, string, string) (*xelonsdk.Response, error)
}

type mutationKind string

const (
	mutationAdd    mutationKind = "add"
	mutationDelete mutationKind = "delete"
)

type pendingMutation struct {
	kind           mutationKind
	baselineIDs    map[string]struct{}
	expectedTarget int
	deletedID      string
}

// NodeGroup is the single explicitly configured XKS worker pool.
type NodeGroup struct {
	client    kubernetesService
	clusterID string
	poolID    string
	minSize   int
	maxSize   int

	requestTimeout        time.Duration
	pollInterval          time.Duration
	reconciliationTimeout time.Duration

	mutationMu sync.Mutex
	pending    *pendingMutation
}

func newNodeGroup(client kubernetesService, clusterID, poolID string, minSize, maxSize int) *NodeGroup {
	return &NodeGroup{
		client:                client,
		clusterID:             clusterID,
		poolID:                poolID,
		minSize:               minSize,
		maxSize:               maxSize,
		requestTimeout:        defaultRequestTimeout,
		pollInterval:          defaultPollInterval,
		reconciliationTimeout: defaultReconciliationTimeout,
	}
}

// MaxSize returns the configured maximum size of the worker pool.
func (group *NodeGroup) MaxSize() int { return group.maxSize }

// MinSize returns the configured minimum size of the worker pool.
func (group *NodeGroup) MinSize() int { return group.minSize }

// TargetSize returns the current desired size reported by XKS.
func (group *NodeGroup) TargetSize() (int, error) {
	snapshot, err := group.fetchSnapshot(context.Background())
	if err != nil {
		return 0, err
	}
	return snapshot.publicTargetSize()
}

// IncreaseSize adds one worker to the pool and reconciles the mutation result.
func (group *NodeGroup) IncreaseSize(delta int) error {
	if delta != 1 {
		return fmt.Errorf("Xelon v0 supports IncreaseSize delta 1 only, got %d", delta)
	}

	group.mutationMu.Lock()
	defer group.mutationMu.Unlock()
	if err := group.resolvePending(); err != nil {
		return err
	}
	baseline, err := group.fetchPublicSnapshot(context.Background())
	if err != nil {
		return err
	}
	if baseline.targetSize+delta > group.maxSize {
		return fmt.Errorf("Xelon worker pool %q cannot grow from %d to %d: maximum is %d", group.poolID, baseline.targetSize, baseline.targetSize+delta, group.maxSize)
	}

	pending := &pendingMutation{
		kind:           mutationAdd,
		baselineIDs:    baseline.workerIDs(),
		expectedTarget: baseline.targetSize + 1,
	}
	group.pending = pending
	ctx, cancel := context.WithTimeout(context.Background(), group.reconciliationTimeout)
	defer cancel()
	requestCtx, requestCancel := context.WithTimeout(ctx, group.requestTimeout)
	_, mutationErr := group.client.CreateNode(requestCtx, group.clusterID, group.poolID)
	requestCancel()
	return group.reconcile(ctx, pending, mutationErr)
}

// AtomicIncreaseSize reports that atomic scale-up is unsupported.
func (group *NodeGroup) AtomicIncreaseSize(int) error {
	return cloudprovider.ErrNotImplemented
}

// DeleteNodes removes the single requested worker after verifying its identity.
func (group *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	if len(nodes) != 1 {
		return fmt.Errorf("Xelon v0 supports deleting exactly one node, got %d", len(nodes))
	}
	localVMID, err := parseProviderID(nodes[0].Spec.ProviderID)
	if err != nil {
		return fmt.Errorf("refuse to delete Kubernetes node %q: %w", nodes[0].Name, err)
	}

	group.mutationMu.Lock()
	defer group.mutationMu.Unlock()
	if err := group.resolvePending(); err != nil {
		return err
	}
	baseline, err := group.fetchPublicSnapshot(context.Background())
	if err != nil {
		return err
	}
	if baseline.targetSize-1 < group.minSize {
		return fmt.Errorf("Xelon worker pool %q cannot shrink from %d to %d: minimum is %d", group.poolID, baseline.targetSize, baseline.targetSize-1, group.minSize)
	}
	selected, found := baseline.workersByLocalID[localVMID]
	if !found {
		return fmt.Errorf("refuse to delete Kubernetes node %q: LocalVMID %q has no worker in pool %q", nodes[0].Name, localVMID, group.poolID)
	}
	if selected.status != workerStateCreated && selected.status != workerStateDeployed {
		return fmt.Errorf("refuse to delete XKS worker %q in unsupported state %q", selected.id, selected.status)
	}

	pending := &pendingMutation{
		kind:           mutationDelete,
		baselineIDs:    baseline.workerIDs(),
		expectedTarget: baseline.targetSize - 1,
		deletedID:      selected.id,
	}
	group.pending = pending
	ctx, cancel := context.WithTimeout(context.Background(), group.reconciliationTimeout)
	defer cancel()
	requestCtx, requestCancel := context.WithTimeout(ctx, group.requestTimeout)
	_, mutationErr := group.client.DeleteNode(requestCtx, group.clusterID, selected.id)
	requestCancel()
	return group.reconcile(ctx, pending, mutationErr)
}

// ForceDeleteNodes reports that forced deletion is unsupported.
func (group *NodeGroup) ForceDeleteNodes([]*apiv1.Node) error {
	return cloudprovider.ErrNotImplemented
}

// DecreaseTargetSize reports that reducing the target without deleting a node is unsupported.
func (group *NodeGroup) DecreaseTargetSize(int) error {
	return cloudprovider.ErrNotImplemented
}

// Id returns the XKS worker pool identifier.
func (group *NodeGroup) Id() string { return group.poolID }

// Debug returns a concise description of the worker pool and its size bounds.
func (group *NodeGroup) Debug() string {
	return fmt.Sprintf("Xelon worker pool %s (min:%d max:%d)", group.poolID, group.minSize, group.maxSize)
}

// Nodes returns the instances currently represented in the XKS worker pool.
func (group *NodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	snapshot, err := group.fetchSnapshot(context.Background())
	if err != nil {
		return nil, err
	}
	return snapshot.publicInstances()
}

// ProviderConfirmedUpcomingNodes returns the number of nodes whose creation
// has been accepted and is still in progress according to the provider.
//
// 'Created' represents backend-confirmed in-flight capacity.
//
// XKS guarantees that 'Created' is transient: a worker eventually transitions
// to 'Deployed' or to a non-capacity state. Therefore 'Created' can be used as
// authoritative evidence of upcoming capacity across autoscaler restarts.
//
// Do not generalize this to an arbitrary TargetSize/registered-node gap.
func (group *NodeGroup) ProviderConfirmedUpcomingNodes() (int, error) {
	snapshot, err := group.fetchSnapshot(context.Background())
	if err != nil {
		return 0, err
	}
	return snapshot.publicProviderConfirmedUpcomingNodes()
}

// TemplateNodeInfo reports that scale-up from zero is unsupported.
func (group *NodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Exist reports that the explicitly configured worker pool exists.
func (group *NodeGroup) Exist() bool { return true }

// Create reports that node group autoprovisioning is unsupported.
func (group *NodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Delete reports that node group autoprovisioning is unsupported.
func (group *NodeGroup) Delete() error { return cloudprovider.ErrNotImplemented }

// Autoprovisioned reports that the worker pool is statically configured.
func (group *NodeGroup) Autoprovisioned() bool { return false }

// GetOptions reports that per-node-group autoscaling options are unsupported.
func (group *NodeGroup) GetOptions(config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (group *NodeGroup) fetchSnapshot(parent context.Context) (*poolSnapshot, error) {
	ctx, cancel := context.WithTimeout(parent, group.requestTimeout)
	defer cancel()
	pool, _, err := group.client.GetNodePool(ctx, group.clusterID, group.poolID)
	if err != nil {
		return nil, fmt.Errorf("read XKS worker pool %q: %w", group.poolID, err)
	}
	snapshot, err := newPoolSnapshot(pool, group.poolID)
	if err != nil {
		return nil, fmt.Errorf("validate XKS worker pool %q: %w", group.poolID, err)
	}
	return snapshot, nil
}

func (group *NodeGroup) fetchPublicSnapshot(parent context.Context) (*poolSnapshot, error) {
	snapshot, err := group.fetchSnapshot(parent)
	if err != nil {
		return nil, err
	}
	if snapshot.classificationErr != nil {
		return nil, fmt.Errorf("classify XKS worker pool %q: %w", group.poolID, snapshot.classificationErr)
	}
	return snapshot, nil
}

func (group *NodeGroup) resolvePending() error {
	if group.pending == nil {
		return nil
	}
	snapshot, err := group.fetchSnapshot(context.Background())
	if err == nil {
		if matched, _ := group.pending.matches(snapshot); matched {
			group.pending = nil
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("Xelon worker pool %q has unresolved %s mutation; reconciliation read failed: %w", group.poolID, group.pending.kind, err)
	}
	return fmt.Errorf("Xelon worker pool %q has unresolved %s mutation; refusing another mutation", group.poolID, group.pending.kind)
}

func (group *NodeGroup) reconcile(ctx context.Context, pending *pendingMutation, mutationErr error) error {
	ticker := time.NewTicker(group.pollInterval)
	defer ticker.Stop()
	var lastReadErr error
	for {
		snapshot, err := group.fetchSnapshot(ctx)
		if err == nil {
			matched, matchErr := pending.matches(snapshot)
			if matched {
				group.pending = nil
				return nil
			}
			lastReadErr = matchErr
		} else {
			lastReadErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("Xelon %s mutation remains ambiguous after reconciliation: %w", pending.kind, errors.Join(mutationErr, lastReadErr, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func (pending *pendingMutation) matches(snapshot *poolSnapshot) (bool, error) {
	if snapshot.classificationErr != nil {
		return false, snapshot.classificationErr
	}
	if snapshot.targetSize != pending.expectedTarget {
		return false, fmt.Errorf("target size is %d, want %d", snapshot.targetSize, pending.expectedTarget)
	}
	currentIDs := snapshot.workerIDs()
	switch pending.kind {
	case mutationAdd:
		if len(currentIDs) != len(pending.baselineIDs)+1 {
			return false, fmt.Errorf("add changed worker count from %d to %d, want %d", len(pending.baselineIDs), len(currentIDs), len(pending.baselineIDs)+1)
		}
		for id := range pending.baselineIDs {
			if _, found := currentIDs[id]; !found {
				return false, fmt.Errorf("add removed baseline worker %q", id)
			}
		}
		return true, nil
	case mutationDelete:
		if _, found := currentIDs[pending.deletedID]; found {
			return false, fmt.Errorf("selected worker %q is still present", pending.deletedID)
		}
		if len(currentIDs) != len(pending.baselineIDs)-1 {
			return false, fmt.Errorf("delete changed worker count from %d to %d, want %d", len(pending.baselineIDs), len(currentIDs), len(pending.baselineIDs)-1)
		}
		for id := range pending.baselineIDs {
			if id == pending.deletedID {
				continue
			}
			if _, found := currentIDs[id]; !found {
				return false, fmt.Errorf("delete removed unrelated worker %q", id)
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf("unknown mutation kind %q", pending.kind)
	}
}
