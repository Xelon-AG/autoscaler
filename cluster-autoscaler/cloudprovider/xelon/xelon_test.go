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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	xelonsdk "github.com/Xelon-AG/xelon-sdk-go/xelon"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func TestSnapshotStateModel(t *testing.T) {
	tests := []struct {
		name          string
		workers       []xelonsdk.KubernetesClusterNode
		wantTarget    int
		wantInstances []string
		wantError     string
	}{
		{
			name: "created without LocalVMID is counted and omitted",
			workers: []xelonsdk.KubernetesClusterNode{
				{ID: "created", Status: workerStateCreated},
				{ID: "deployed", LocalVMID: "vm-deployed", Status: workerStateDeployed},
			},
			wantTarget:    2,
			wantInstances: []string{"xelon://vm-deployed"},
		},
		{
			name: "created with LocalVMID is published",
			workers: []xelonsdk.KubernetesClusterNode{
				{ID: "created", LocalVMID: "vm-created", Status: workerStateCreated},
			},
			wantTarget:    1,
			wantInstances: []string{"xelon://vm-created"},
		},
		{
			name:      "deployed without LocalVMID fails closed",
			workers:   []xelonsdk.KubernetesClusterNode{{ID: "worker", Status: workerStateDeployed}},
			wantError: "has no LocalVMID",
		},
		{
			name:      "deleting fails closed",
			workers:   []xelonsdk.KubernetesClusterNode{{ID: "worker", LocalVMID: "vm", Status: "Deleting"}},
			wantError: `unsupported state "Deleting"`,
		},
		{
			name:      "unknown state fails closed",
			workers:   []xelonsdk.KubernetesClusterNode{{ID: "worker", LocalVMID: "vm", Status: "Surprising"}},
			wantError: `unsupported state "Surprising"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := newPoolSnapshot(&xelonsdk.KubernetesClusterNodePool{ID: "pool", Nodes: test.workers}, "pool")
			if err != nil {
				t.Fatal(err)
			}
			target, targetErr := snapshot.publicTargetSize()
			instances, instancesErr := snapshot.publicInstances()
			if test.wantError != "" {
				joined := errors.Join(targetErr, instancesErr)
				if joined == nil || !strings.Contains(joined.Error(), test.wantError) {
					t.Fatalf("snapshot errors=%v; want %q", joined, test.wantError)
				}
				return
			}
			if targetErr != nil || instancesErr != nil {
				t.Fatalf("snapshot errors: target=%v instances=%v", targetErr, instancesErr)
			}
			if target != test.wantTarget {
				t.Fatalf("TargetSize=%d; want %d", target, test.wantTarget)
			}
			ids := make([]string, 0, len(instances))
			for _, instance := range instances {
				ids = append(ids, instance.Id)
			}
			if !reflect.DeepEqual(ids, test.wantInstances) {
				t.Fatalf("instance IDs=%v; want %v", ids, test.wantInstances)
			}
		})
	}
}

func TestSnapshotRejectsAmbiguousIdentity(t *testing.T) {
	tests := []struct {
		name    string
		workers []xelonsdk.KubernetesClusterNode
		want    string
	}{
		{name: "empty worker ID", workers: []xelonsdk.KubernetesClusterNode{{LocalVMID: "vm", Status: workerStateCreated}}, want: "empty identifier"},
		{name: "duplicate worker ID", workers: []xelonsdk.KubernetesClusterNode{{ID: "same", Status: workerStateCreated}, {ID: "same", Status: workerStateCreated}}, want: "duplicated"},
		{name: "duplicate LocalVMID", workers: []xelonsdk.KubernetesClusterNode{{ID: "one", LocalVMID: "same", Status: workerStateCreated}, {ID: "two", LocalVMID: "same", Status: workerStateDeployed}}, want: "maps to both"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newPoolSnapshot(&xelonsdk.KubernetesClusterNodePool{ID: "pool", Nodes: test.workers}, "pool")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newPoolSnapshot error=%v; want %q", err, test.want)
			}
		})
	}
}

func TestProviderIDParsing(t *testing.T) {
	if got, err := parseProviderID("xelon://vm-1"); err != nil || got != "vm-1" {
		t.Fatalf("parseProviderID=%q, %v; want vm-1", got, err)
	}
	for _, invalid := range []string{"", "aws://vm-1", "xelon://", "xelon:// vm-1", "xelon://vm 1"} {
		if _, err := parseProviderID(invalid); err == nil {
			t.Errorf("parseProviderID(%q) succeeded", invalid)
		}
	}
}

func TestNodeGroupForNodeUsesPoolMembership(t *testing.T) {
	service := newFakeKubernetesService([]xelonsdk.KubernetesClusterNode{
		{ID: "worker", LocalVMID: "worker-vm", Status: workerStateDeployed},
	})
	provider := testProvider(t, service, 1, 3)

	group, err := provider.NodeGroupForNode(testNode("worker", "xelon://worker-vm"))
	if err != nil || group == nil || group.Id() != "pool" {
		t.Fatalf("NodeGroupForNode(worker)=%v, %v", group, err)
	}
	group, err = provider.NodeGroupForNode(testNode("control-plane", "xelon://control-plane-vm"))
	if err != nil || group != nil {
		t.Fatalf("NodeGroupForNode(control-plane)=%v, %v; want nil", group, err)
	}
	group, err = provider.NodeGroupForNode(testNode("foreign", "aws://instance"))
	if err != nil || group != nil {
		t.Fatalf("NodeGroupForNode(foreign)=%v, %v; want nil", group, err)
	}
}

func TestIncreaseSizeReconcilesLostResponseWithoutRetry(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(2))
	service.create = func(service *fakeKubernetesService) error {
		service.pool.Nodes = append(service.pool.Nodes, xelonsdk.KubernetesClusterNode{ID: "worker-3", Status: workerStateCreated})
		return errors.New("response lost")
	}
	group := testProvider(t, service, 1, 3).nodeGroup

	if err := group.IncreaseSize(1); err != nil {
		t.Fatal(err)
	}
	if service.createCalls != 1 {
		t.Fatalf("CreateNode calls=%d; want exactly one", service.createCalls)
	}
	if target, err := group.TargetSize(); err != nil || target != 3 {
		t.Fatalf("TargetSize=%d, %v; want 3", target, err)
	}
}

func TestIncreaseSizeRejectsUnsupportedDeltaBeforeMutation(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(2))
	group := testProvider(t, service, 1, 3).nodeGroup
	if err := group.IncreaseSize(2); err == nil {
		t.Fatal("IncreaseSize(2) succeeded")
	}
	if service.createCalls != 0 {
		t.Fatalf("CreateNode calls=%d; want zero", service.createCalls)
	}
}

func TestDeleteNodesResolvesLocalVMIDToExactWorkerID(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(3))
	service.delete = func(service *fakeKubernetesService, workerID string) error {
		service.removeWorker(workerID)
		return errors.New("response lost")
	}
	group := testProvider(t, service, 1, 3).nodeGroup

	if err := group.DeleteNodes([]*apiv1.Node{testNode("kubernetes-name-is-not-identity", "xelon://vm-2")}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(service.deleteIDs, []string{"worker-2"}) {
		t.Fatalf("DeleteNode IDs=%v; want worker-2", service.deleteIDs)
	}
	remaining := service.workerIDs()
	if !reflect.DeepEqual(remaining, []string{"worker-1", "worker-3"}) {
		t.Fatalf("remaining workers=%v", remaining)
	}
}

func TestDeleteNodesFailsBeforeDestructiveCall(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(2))
	group := testProvider(t, service, 1, 3).nodeGroup
	tests := []struct {
		name  string
		nodes []*apiv1.Node
	}{
		{name: "empty list"},
		{name: "multiple nodes", nodes: []*apiv1.Node{testNode("one", "xelon://vm-1"), testNode("two", "xelon://vm-2")}},
		{name: "wrong provider", nodes: []*apiv1.Node{testNode("foreign", "aws://vm-1")}},
		{name: "missing mapping", nodes: []*apiv1.Node{testNode("missing", "xelon://not-in-pool")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := group.DeleteNodes(test.nodes); err == nil {
				t.Fatal("DeleteNodes succeeded")
			}
		})
	}
	if len(service.deleteIDs) != 0 {
		t.Fatalf("DeleteNode IDs=%v; want no destructive calls", service.deleteIDs)
	}
}

func TestDeleteNodesHonorsMinimumBeforeDestructiveCall(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(1))
	group := testProvider(t, service, 1, 3).nodeGroup

	if err := group.DeleteNodes([]*apiv1.Node{testNode("worker-1", "xelon://vm-1")}); err == nil || !strings.Contains(err.Error(), "minimum is 1") {
		t.Fatalf("DeleteNodes error=%v; want minimum-size rejection", err)
	}
	if len(service.deleteIDs) != 0 {
		t.Fatalf("DeleteNode IDs=%v; want no destructive calls", service.deleteIDs)
	}
}

func TestAmbiguousMutationLatchesGroupUnsafe(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(2))
	service.create = func(*fakeKubernetesService) error { return errors.New("response lost before observable transition") }
	group := testProvider(t, service, 1, 3).nodeGroup
	group.pollInterval = time.Millisecond
	group.reconciliationTimeout = 5 * time.Millisecond
	group.requestTimeout = 5 * time.Millisecond

	if err := group.IncreaseSize(1); err == nil || !strings.Contains(err.Error(), "remains ambiguous") {
		t.Fatalf("first IncreaseSize error=%v", err)
	}
	if err := group.IncreaseSize(1); err == nil || !strings.Contains(err.Error(), "unresolved add mutation") {
		t.Fatalf("second IncreaseSize error=%v", err)
	}
	if service.createCalls != 1 {
		t.Fatalf("CreateNode calls=%d; want no retry", service.createCalls)
	}
}

func TestDecreaseTargetSizeIsExplicitlyUnsupported(t *testing.T) {
	service := newFakeKubernetesService(deployedWorkers(2))
	group := testProvider(t, service, 1, 3).nodeGroup
	if !errors.Is(group.DecreaseTargetSize(-1), cloudprovider.ErrNotImplemented) {
		t.Fatal("DecreaseTargetSize did not return ErrNotImplemented")
	}
}

func TestReadConfig(t *testing.T) {
	config, err := readConfig(strings.NewReader(`{"token":"token","client_id":"client","cluster_id":"cluster","base_url":"https://example.test/api/v2/"}`))
	if err != nil || config.ClusterID != "cluster" {
		t.Fatalf("readConfig=%#v, %v", config, err)
	}
	for _, invalid := range []string{
		`{"client_id":"client","cluster_id":"cluster"}`,
		`{"token":"token","client_id":"client","cluster_id":"cluster","unknown":true}`,
		`{"token":"token","client_id":"client","cluster_id":"cluster","base_url":"https://example.test/api/v2"}`,
	} {
		if _, err := readConfig(strings.NewReader(invalid)); err == nil {
			t.Errorf("readConfig(%s) succeeded", invalid)
		}
	}
}

type fakeKubernetesService struct {
	mu          sync.Mutex
	pool        xelonsdk.KubernetesClusterNodePool
	getErr      error
	create      func(*fakeKubernetesService) error
	delete      func(*fakeKubernetesService, string) error
	createCalls int
	deleteIDs   []string
}

func newFakeKubernetesService(workers []xelonsdk.KubernetesClusterNode) *fakeKubernetesService {
	return &fakeKubernetesService{pool: xelonsdk.KubernetesClusterNodePool{ID: "pool", Nodes: append([]xelonsdk.KubernetesClusterNode(nil), workers...)}}
}

func (service *fakeKubernetesService) GetNodePool(context.Context, string, string) (*xelonsdk.KubernetesClusterNodePool, *xelonsdk.Response, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.getErr != nil {
		return nil, nil, service.getErr
	}
	copy := service.pool
	copy.Nodes = append([]xelonsdk.KubernetesClusterNode(nil), service.pool.Nodes...)
	return &copy, nil, nil
}

func (service *fakeKubernetesService) CreateNode(context.Context, string, string) (*xelonsdk.Response, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.createCalls++
	if service.create != nil {
		return nil, service.create(service)
	}
	return nil, fmt.Errorf("unexpected CreateNode call")
}

func (service *fakeKubernetesService) DeleteNode(_ context.Context, _ string, workerID string) (*xelonsdk.Response, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.deleteIDs = append(service.deleteIDs, workerID)
	if service.delete != nil {
		return nil, service.delete(service, workerID)
	}
	return nil, fmt.Errorf("unexpected DeleteNode call")
}

func (service *fakeKubernetesService) removeWorker(workerID string) {
	workers := service.pool.Nodes[:0]
	for _, current := range service.pool.Nodes {
		if current.ID != workerID {
			workers = append(workers, current)
		}
	}
	service.pool.Nodes = workers
}

func (service *fakeKubernetesService) workerIDs() []string {
	service.mu.Lock()
	defer service.mu.Unlock()
	ids := make([]string, 0, len(service.pool.Nodes))
	for _, current := range service.pool.Nodes {
		ids = append(ids, current.ID)
	}
	return ids
}

func deployedWorkers(count int) []xelonsdk.KubernetesClusterNode {
	workers := make([]xelonsdk.KubernetesClusterNode, 0, count)
	for i := 1; i <= count; i++ {
		workers = append(workers, xelonsdk.KubernetesClusterNode{
			ID:        fmt.Sprintf("worker-%d", i),
			LocalVMID: fmt.Sprintf("vm-%d", i),
			Status:    workerStateDeployed,
		})
	}
	return workers
}

func testProvider(t *testing.T, service kubernetesService, minSize, maxSize int) *xelonCloudProvider {
	t.Helper()
	provider, err := newXelonCloudProvider(service, &Config{ClusterID: "cluster"}, "pool", minSize, maxSize, cloudprovider.NewResourceLimiter(map[string]int64{}, map[string]int64{}))
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func testNode(name, providerID string) *apiv1.Node {
	return &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: apiv1.NodeSpec{ProviderID: providerID}}
}
