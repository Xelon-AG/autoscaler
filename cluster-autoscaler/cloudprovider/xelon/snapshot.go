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
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	xelonsdk "github.com/Xelon-AG/xelon-sdk-go/xelon"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

const xelonProviderIDPrefix = "xelon://"

const (
	workerStateCreated  = "Created"
	workerStateDeployed = "Deployed"
)

var errForeignProviderID = errors.New("provider ID does not use the Xelon scheme")

type worker struct {
	id        string
	localVMID string
	status    string
}

type poolSnapshot struct {
	workersByID       map[string]worker
	workersByLocalID  map[string]worker
	targetSize        int
	instances         []cloudprovider.Instance
	classificationErr error
}

func newPoolSnapshot(pool *xelonsdk.KubernetesClusterNodePool, configuredPoolID string) (*poolSnapshot, error) {
	if pool == nil {
		return nil, fmt.Errorf("XKS returned an empty worker pool")
	}
	if pool.ID == "" {
		return nil, fmt.Errorf("XKS worker pool has an empty identifier")
	}
	if pool.ID != configuredPoolID {
		return nil, fmt.Errorf("XKS returned worker pool %q, want %q", pool.ID, configuredPoolID)
	}

	snapshot := &poolSnapshot{
		workersByID:      make(map[string]worker, len(pool.Nodes)),
		workersByLocalID: make(map[string]worker, len(pool.Nodes)),
		instances:        make([]cloudprovider.Instance, 0, len(pool.Nodes)),
	}
	var classificationErrors []error
	for _, apiWorker := range pool.Nodes {
		current := worker{id: apiWorker.ID, localVMID: apiWorker.LocalVMID, status: apiWorker.Status}
		if current.id == "" {
			return nil, fmt.Errorf("XKS worker has an empty identifier")
		}
		if _, found := snapshot.workersByID[current.id]; found {
			return nil, fmt.Errorf("XKS worker identifier %q is duplicated", current.id)
		}
		snapshot.workersByID[current.id] = current
		if current.localVMID != "" {
			if existing, found := snapshot.workersByLocalID[current.localVMID]; found {
				return nil, fmt.Errorf("XKS LocalVMID %q maps to both workers %q and %q", current.localVMID, existing.id, current.id)
			}
			snapshot.workersByLocalID[current.localVMID] = current
		}

		switch current.status {
		case workerStateCreated:
			snapshot.targetSize++
			if current.localVMID != "" {
				snapshot.instances = append(snapshot.instances, instanceForWorker(current, cloudprovider.InstanceCreating))
			}
		case workerStateDeployed:
			snapshot.targetSize++
			if current.localVMID == "" {
				classificationErrors = append(classificationErrors, fmt.Errorf("deployed XKS worker %q has no LocalVMID", current.id))
				continue
			}
			snapshot.instances = append(snapshot.instances, instanceForWorker(current, cloudprovider.InstanceRunning))
		default:
			classificationErrors = append(classificationErrors, fmt.Errorf("XKS worker %q has unsupported state %q", current.id, current.status))
		}
	}
	snapshot.classificationErr = errors.Join(classificationErrors...)
	sort.Slice(snapshot.instances, func(i, j int) bool {
		return snapshot.instances[i].Id < snapshot.instances[j].Id
	})
	return snapshot, nil
}

func instanceForWorker(current worker, state cloudprovider.InstanceState) cloudprovider.Instance {
	return cloudprovider.Instance{
		Id: toProviderID(current.localVMID),
		Status: &cloudprovider.InstanceStatus{
			State: state,
		},
	}
}

func (snapshot *poolSnapshot) publicTargetSize() (int, error) {
	if snapshot.classificationErr != nil {
		return 0, snapshot.classificationErr
	}
	return snapshot.targetSize, nil
}

func (snapshot *poolSnapshot) publicInstances() ([]cloudprovider.Instance, error) {
	if snapshot.classificationErr != nil {
		return nil, snapshot.classificationErr
	}
	return append([]cloudprovider.Instance(nil), snapshot.instances...), nil
}

func (snapshot *poolSnapshot) workerIDs() map[string]struct{} {
	ids := make(map[string]struct{}, len(snapshot.workersByID))
	for id := range snapshot.workersByID {
		ids[id] = struct{}{}
	}
	return ids
}

func toProviderID(localVMID string) string {
	return xelonProviderIDPrefix + localVMID
}

func parseProviderID(providerID string) (string, error) {
	if !strings.HasPrefix(providerID, xelonProviderIDPrefix) {
		return "", errForeignProviderID
	}
	localVMID := strings.TrimPrefix(providerID, xelonProviderIDPrefix)
	if localVMID == "" {
		return "", fmt.Errorf("Xelon provider ID has an empty LocalVMID")
	}
	if strings.TrimSpace(localVMID) != localVMID || strings.IndexFunc(localVMID, unicode.IsSpace) >= 0 {
		return "", fmt.Errorf("Xelon provider ID contains whitespace")
	}
	return localVMID, nil
}
