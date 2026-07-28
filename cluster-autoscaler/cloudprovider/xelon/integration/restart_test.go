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

package integration

import (
	"sync/atomic"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	clusterstateutils "k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	coretest "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/observers/loopstart"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	processorstest "k8s.io/autoscaler/cluster-autoscaler/processors/test"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	kubeutils "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	catest "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
)

// TestCA135RestartPreservesIdentitylessUpcomingCapacity is the central v0
// compatibility gate. The backend target includes one Created worker, while
// provider Nodes() deliberately omits it until LocalVMID is available.
func TestCA135RestartPreservesIdentitylessUpcomingCapacity(t *testing.T) {
	now := time.Now()
	node1 := readyNode("xelon://vm-1", "xelon://vm-1", now)
	node2 := readyNode("xelon://vm-2", "xelon://vm-2", now)
	kubernetesNodes := []*apiv1.Node{node1, node2}

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("cluster/pool", 1, 10, 3)
	provider.AddNode("cluster/pool", node1)
	provider.AddNode("cluster/pool", node2)

	beforeRestart := newRegistry(t, provider)
	if err := beforeRestart.UpdateNodes(kubernetesNodes, nil, now); err != nil {
		t.Fatal(err)
	}
	assertOneUpcomingAndNoSyntheticInstance(t, beforeRestart)

	afterRestart := newRegistry(t, provider)
	if err := afterRestart.UpdateNodes(kubernetesNodes, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertOneUpcomingAndNoSyntheticInstance(t, afterRestart)

	identityAvailable := readyNode("xelon://vm-3", "xelon://vm-3", now)
	provider.AddNode("cluster/pool", identityAvailable)
	provider.GetNodeGroup("cluster/pool").(*testprovider.TestNodeGroup).SetTargetSize(3)
	afterRestart = newRegistry(t, provider)
	if err := afterRestart.UpdateNodes(kubernetesNodes, nil, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	upcoming, _ := afterRestart.GetUpcomingNodes()
	if upcoming["cluster/pool"] != 1 {
		t.Fatalf("upcoming after identity publication=%d; want 1", upcoming["cluster/pool"])
	}
	unregistered := afterRestart.GetUnregisteredNodes()
	if len(unregistered) != 1 || unregistered[0].Node.Spec.ProviderID != "xelon://vm-3" {
		t.Fatalf("unregistered instances=%v; want exactly xelon://vm-3", unregistered)
	}
}

// TestCA135RestartDoesNotRequestFourthWorker runs a complete CA loop after a
// restart. The pending pod fits the one target-size-gap worker, so a call to
// IncreaseSize would be an observable duplicate scale-up defect.
func TestCA135RestartDoesNotRequestFourthWorker(t *testing.T) {
	now := time.Now()
	node1 := readyNode("xelon://vm-1", "xelon://vm-1", now)
	node2 := readyNode("xelon://vm-2", "xelon://vm-2", now)
	nodes := []*apiv1.Node{node1, node2}

	var scaleUpCalls atomic.Int32
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(_ string, _ int) error {
		scaleUpCalls.Add(1)
		return nil
	}).Build()
	provider.AddNodeGroup("cluster/pool", 1, 10, 3)
	provider.AddNode("cluster/pool", node1)
	provider.AddNode("cluster/pool", node2)

	scheduled1 := catest.BuildTestPod("scheduled-1", 800, 0)
	scheduled1.Spec.NodeName = node1.Name
	scheduled2 := catest.BuildTestPod("scheduled-2", 800, 0)
	scheduled2.Spec.NodeName = node2.Name
	pending := catest.BuildTestPod("pending", 800, 0, catest.MarkUnschedulable())
	pods := []*apiv1.Pod{scheduled1, scheduled2, pending}

	daemonSetLister, err := kubeutils.NewTestDaemonSetLister(nil)
	if err != nil {
		t.Fatal(err)
	}
	listers := kubeutils.NewListerRegistry(
		kubeutils.NewTestNodeLister(nodes),
		kubeutils.NewTestNodeLister(nodes),
		kubeutils.NewTestPodLister(pods),
		kubeutils.NewTestPodDisruptionBudgetLister(nil),
		daemonSetLister,
		nil, nil, nil, nil,
	)

	autoscalingOptions := config.AutoscalingOptions{
		NodeGroupDefaults:              config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute},
		EstimatorName:                  estimator.BinpackingEstimatorName,
		ScaleDownEnabled:               false,
		MaxNodesTotal:                  10,
		MaxCoresTotal:                  config.DefaultMaxClusterCores,
		MaxMemoryTotal:                 config.DefaultMaxClusterMemory,
		MaxTotalUnreadyPercentage:      45,
		OkTotalUnreadyCount:            3,
		MaxNodeGroupBinpackingDuration: time.Second,
	}
	processors, templateRegistry := processorstest.NewTestProcessors(autoscalingOptions)
	fakeClient := fake.NewSimpleClientset(node1, node2)
	autoscalingContext, err := coretest.NewScaleTestAutoscalingContext(
		autoscalingOptions, fakeClient, listers, provider, nil, nil, templateRegistry,
	)
	if err != nil {
		t.Fatal(err)
	}
	estimatorBuilder, err := estimator.NewEstimatorBuilder(
		estimator.BinpackingEstimatorName,
		estimator.NewThresholdBasedEstimationLimiter(nil),
		estimator.NewDecreasingPodOrderer(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	deleteOptions := options.NewNodeDeleteOptions(autoscalingOptions)
	autoscaler := core.NewStaticAutoscaler(
		autoscalingOptions,
		autoscalingContext.FrameworkHandle,
		autoscalingContext.ClusterSnapshot,
		&autoscalingContext.AutoscalingKubeClients,
		processors,
		loopstart.NewObserversList(nil),
		provider,
		autoscalingContext.ExpanderStrategy,
		estimatorBuilder,
		backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour),
		autoscalingContext.DebuggingSnapshotter,
		autoscalingContext.RemainingPdbTracker,
		nil,
		deleteOptions,
		rules.Default(deleteOptions),
		nil,
		resourcequotas.TrackerOptions{
			QuotaProvider:            resourcequotas.NewCloudQuotasProvider(provider),
			CustomResourcesProcessor: processors.CustomResourcesProcessor,
		},
		nil,
	)

	if err := autoscaler.RunOnce(now); err != nil {
		t.Fatal(err)
	}
	if got := scaleUpCalls.Load(); got != 0 {
		t.Fatalf("IncreaseSize calls=%d; want zero while one target-size-gap worker is upcoming", got)
	}

	provider.GetNodeGroup("cluster/pool").(*testprovider.TestNodeGroup).SetTargetSize(2)
	if err := autoscaler.RunOnce(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := scaleUpCalls.Load(); got != 1 {
		t.Fatalf("control IncreaseSize calls=%d; want one without target-size-gap capacity", got)
	}
}

func readyNode(name, providerID string, now time.Time) *apiv1.Node {
	node := catest.BuildTestNode(name, 1000, 1000)
	node.Spec.ProviderID = providerID
	catest.SetNodeReadyState(node, true, now.Add(-2*time.Minute))
	return node
}

func newRegistry(t *testing.T, provider *testprovider.TestCloudProvider) *clusterstate.ClusterStateRegistry {
	t.Helper()
	client := &fake.Clientset{}
	recorder, err := clusterstateutils.NewStatusMapRecorder(client, "kube-system", kube_record.NewFakeRecorder(10), false, "xelon-v0-test")
	if err != nil {
		t.Fatal(err)
	}
	return clusterstate.NewClusterStateRegistry(
		provider,
		clusterstate.ClusterStateRegistryConfig{MaxTotalUnreadyPercentage: 45, OkTotalUnreadyCount: 3},
		recorder,
		backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour),
		nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute}),
		asyncnodegroups.NewDefaultAsyncNodeGroupStateChecker(),
	)
}

func assertOneUpcomingAndNoSyntheticInstance(t *testing.T, registry *clusterstate.ClusterStateRegistry) {
	t.Helper()
	upcoming, registered := registry.GetUpcomingNodes()
	if upcoming["cluster/pool"] != 1 {
		t.Fatalf("upcoming=%d; want exactly one target-size-gap worker", upcoming["cluster/pool"])
	}
	if len(registered["cluster/pool"]) != 0 {
		t.Fatalf("registered upcoming nodes=%v; want none", registered["cluster/pool"])
	}
	if len(registry.GetUnregisteredNodes()) != 0 {
		t.Fatalf("identity-less Created capacity leaked into Nodes(): %v", registry.GetUnregisteredNodes())
	}
}
