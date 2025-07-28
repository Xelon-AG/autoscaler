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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Xelon-AG/xelon-sdk-go/xelon"
	"k8s.io/autoscaler/cluster-autoscaler/version"
	"k8s.io/klog/v2"
)

const (
	defaultBaseURL = "https://hq.xelon.ch/api/service/"

	defaultNodeGroupMinSize = 1
	defaultNodeGroupMaxSize = 100
)

type nodeGroupClient interface {
	List(ctx context.Context) ([]xelon.KubernetesCluster, *xelon.Response, error)
	ListClusterPools(ctx context.Context, kubernetesClusterID string) ([]xelon.ClusterPool, *xelon.Response, error)
	AddClusterNode(ctx context.Context, kubernetesClusterID, clusterPoolID string) (*xelon.SuccessResponse, *xelon.Response, error)
	DeleteClusterNode(ctx context.Context, kubernetesClusterID, clusterNodeID string) (*xelon.SuccessResponse, *xelon.Response, error)
}

// Manager handles Xelon communication and data caching of node groups.
type Manager struct {
	client    nodeGroupClient
	clusterID string
	poolID    string

	nodeGroups []*NodeGroup
	minSize    int
	maxSize    int
}

// Config is the configuration for the Xelon cloud provider.
type Config struct {
	// BaseURL is the base URL endpoint for Xelon HQ. Default is https://hq.xelon.ch/api/service/.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// ClientID is the client ID for IP ranges.
	ClientID string `json:"client_id" yaml:"client_id"`

	// ClusterID is the id associated with the Kubernetes cluster where
	// Xelon Cluster Autoscaler is running.
	ClusterID string `json:"cluster_id" yaml:"cluster_id"`

	// ClusterPoolID is the id associated with the cluster pool where
	// Xelon Cluster Autoscaler is running.
	ClusterPoolID string `json:"cluster_pool_id" yaml:"cluster_pool_id"`

	// NodeGroup is the configuration for node group.
	NodeGroup *NodeGroupConfig `json:"node_group" yaml:"node_group"`

	// Token is the access token for Xelon HQ.
	Token string `json:"token" yaml:"token"`
}

type NodeGroupConfig struct {
	MinSize int `json:"min_size" yaml:"min_size"`
	MaxSize int `json:"max_size" yaml:"max_size"`
}

func newManager(configReader io.Reader) (*Manager, error) {
	cfg := &Config{}
	if configReader != nil {
		body, err := io.ReadAll(configReader)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(body, cfg)
		if err != nil {
			return nil, err
		}
	} else {
		cfg.BaseURL = os.Getenv("XELON_BASE_URL")
		cfg.ClientID = os.Getenv("XELON_CLIENT_ID")
		cfg.ClusterID = os.Getenv("XELON_KUBERNETES_CLUSTER_ID")
		cfg.ClusterPoolID = os.Getenv("XELON_CLUSTER_POOL_ID")
		cfg.Token = os.Getenv("XELON_TOKEN")
	}

	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	if cfg.ClientID == "" {
		return nil, errors.New("xelon client ID was not provided")
	}
	if cfg.ClusterID == "" {
		return nil, errors.New("xelon Kubernetes cluster ID was not provided")
	}
	if cfg.ClusterPoolID == "" {
		return nil, errors.New("xelon cluster pool ID was not provided")
	}
	if cfg.Token == "" {
		return nil, errors.New("xelon token was not provided")
	}

	if cfg.NodeGroup == nil {
		klog.V(2).InfoS("Use default min/max size for node group",
			"min_size", defaultNodeGroupMinSize,
			"max_size", defaultNodeGroupMaxSize)
		cfg.NodeGroup = &NodeGroupConfig{
			MinSize: defaultNodeGroupMinSize,
			MaxSize: defaultNodeGroupMaxSize,
		}
	}

	var opts []xelon.ClientOption
	opts = append(opts, xelon.WithBaseURL(cfg.BaseURL))
	opts = append(opts, xelon.WithClientID(cfg.ClientID))
	opts = append(opts, xelon.WithUserAgent(fmt.Sprintf("cluster-autoscaler-xelon/v%s", version.ClusterAutoscalerVersion)))
	xelonClient := xelon.NewClient(cfg.Token, opts...)

	return &Manager{
		client:    xelonClient.Kubernetes,
		clusterID: cfg.ClusterID,
		poolID:    cfg.ClusterPoolID,

		nodeGroups: make([]*NodeGroup, 0),
		minSize:    cfg.NodeGroup.MinSize,
		maxSize:    cfg.NodeGroup.MaxSize,
	}, nil
}

// Refresh refreshes the cache holding the node groups. This is called by the CA
// based on the `--scan-interval`. By default, it's 10 seconds.
func (m *Manager) Refresh() error {
	klog.V(4).InfoS("Getting Xelon cluster pool",
		"cluster_id", m.clusterID,
		"pool_id", m.poolID,
	)
	clusterPools, _, err := m.client.ListClusterPools(context.Background(), m.clusterID)
	if err != nil {
		return fmt.Errorf("failed to list Xelon cluster pools: %s", err)
	}
	klog.V(4).InfoS("Got Xelon cluster pool", "data", clusterPools)

	var nodeGroups []*NodeGroup
	for _, clusterPool := range clusterPools {
		if clusterPool.ID == m.poolID {
			cp := clusterPool
			klog.V(4).InfoS("Found cluster pool", "pool", clusterPool, "pool_id", m.poolID)
			nodeGroups = append(nodeGroups, &NodeGroup{
				client:    m.client,
				clusterID: m.clusterID,
				id:        m.poolID,
				nodePool:  &cp,
				minSize:   m.minSize,
				maxSize:   m.maxSize,
			})
		}
	}

	m.nodeGroups = nodeGroups

	if len(m.nodeGroups) == 0 {
		klog.V(2).InfoS("Cluster-autoscaler disabled, because no node pools are configured.")
	}

	return nil
}
