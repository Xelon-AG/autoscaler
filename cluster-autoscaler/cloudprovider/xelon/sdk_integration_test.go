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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	coreoptions "k8s.io/autoscaler/cluster-autoscaler/core/options"
)

func TestBuildXelonUsesModernKubernetesService(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.Method+" "+request.URL.Path)
		if request.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("Authorization=%q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("X-User-Id") != "client" {
			t.Errorf("X-User-Id=%q", request.Header.Get("X-User-Id"))
		}
		if !strings.Contains(request.Header.Get("User-Agent"), "cluster-autoscaler-xelon/") {
			t.Errorf("User-Agent=%q", request.Header.Get("User-Agent"))
		}
		if request.Method != http.MethodGet || request.URL.Path != "/kubernetes/cluster/pools/pool" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"identifier":"pool","nodes":[{"identifier":"worker","localvmid":"vm","status":"Deployed"}]}`)
	}))
	defer server.Close()

	configPath := filepath.Join(t.TempDir(), "cloud-config.json")
	contents := fmt.Sprintf(`{"base_url":%q,"token":"token","client_id":"client","cluster_id":"cluster"}`, server.URL+"/")
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := &coreoptions.AutoscalerOptions{AutoscalingOptions: config.AutoscalingOptions{CloudConfig: configPath}}
	provider, err := buildXelon(opts, cloudprovider.NodeGroupDiscoveryOptions{NodeGroupSpecs: []string{"1:3:pool"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if provider.Name() != cloudprovider.XelonProviderName {
		t.Fatalf("provider name=%q", provider.Name())
	}
	if len(requests) != 1 || requests[0] != "GET /kubernetes/cluster/pools/pool" {
		t.Fatalf("requests=%v; want modern KubernetesService GetNodePool route", requests)
	}
}

func TestBuildXelonRejectsUnsupportedDiscoveryBeforeAPIAccess(t *testing.T) {
	opts := new(coreoptions.AutoscalerOptions)
	tests := []cloudprovider.NodeGroupDiscoveryOptions{
		{},
		{NodeGroupSpecs: []string{"1:3:one", "1:3:two"}},
		{NodeGroupSpecs: []string{"1:3:pool"}, NodeGroupAutoDiscoverySpecs: []string{"xelon:anything"}},
		{NodeGroupSpecs: []string{"0:3:pool"}},
	}
	for _, discovery := range tests {
		if _, err := buildXelon(opts, discovery, nil); err == nil {
			t.Errorf("buildXelon(%+v) succeeded", discovery)
		}
	}
}
