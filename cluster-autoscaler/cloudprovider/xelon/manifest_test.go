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
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestExampleManifestV0SafetyContract(t *testing.T) {
	data, err := os.ReadFile("examples/cluster-autoscaler.yaml")
	if err != nil {
		t.Fatal(err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var deployment *appsv1.Deployment
	var policyRules []namedPolicyRules
	deploymentCount := 0
	serviceAccountFound := false
	for {
		object := &unstructured.Unstructured{}
		if err := decoder.Decode(object); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode manifest: %v", err)
		}
		if len(object.Object) == 0 {
			continue
		}
		if object.GetKind() == "Secret" {
			t.Fatal("deployment manifest must not contain inline credentials in a Secret")
		}
		switch object.GetKind() {
		case "Deployment":
			deploymentCount++
			deployment = &appsv1.Deployment{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, deployment); err != nil {
				t.Fatalf("decode Deployment: %v", err)
			}
		case "ClusterRole":
			clusterRole := &rbacv1.ClusterRole{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, clusterRole); err != nil {
				t.Fatalf("decode ClusterRole: %v", err)
			}
			policyRules = append(policyRules, namedPolicyRules{name: "ClusterRole/" + clusterRole.Name, rules: clusterRole.Rules})
		case "Role":
			role := &rbacv1.Role{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, role); err != nil {
				t.Fatalf("decode Role: %v", err)
			}
			policyRules = append(policyRules, namedPolicyRules{name: "Role/" + role.Name, rules: role.Rules})
		case "ServiceAccount":
			if object.GetName() == "xelon-cluster-autoscaler" && object.GetNamespace() == "kube-system" {
				serviceAccountFound = true
			}
		}
	}

	if deploymentCount != 1 {
		t.Fatalf("manifest has %d Deployments; want 1", deploymentCount)
	}
	if !serviceAccountFound {
		t.Error("manifest has no kube-system/xelon-cluster-autoscaler ServiceAccount")
	}
	if len(policyRules) == 0 {
		t.Fatal("manifest has no RBAC policy rules")
	}
	assertV0Deployment(t, deployment)
	for _, policy := range policyRules {
		assertNoDangerousPolicyRules(t, policy.name, policy.rules)
	}
}

type namedPolicyRules struct {
	name  string
	rules []rbacv1.PolicyRule
}

func assertV0Deployment(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("replicas=%v; want 1", deployment.Spec.Replicas)
	}

	pod := deployment.Spec.Template.Spec
	if pod.ServiceAccountName != "xelon-cluster-autoscaler" {
		t.Errorf("serviceAccountName=%q; want xelon-cluster-autoscaler", pod.ServiceAccountName)
	}
	if value, ok := pod.NodeSelector["node-role.kubernetes.io/control-plane"]; !ok || value != "" {
		t.Errorf("nodeSelector=%v; want node-role.kubernetes.io/control-plane with an empty value", pod.NodeSelector)
	}
	wantControlPlaneToleration := corev1.Toleration{
		Key:      "node-role.kubernetes.io/control-plane",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}
	if !containsToleration(pod.Tolerations, wantControlPlaneToleration) {
		t.Errorf("tolerations=%v; want %v", pod.Tolerations, wantControlPlaneToleration)
	}
	if pod.PriorityClassName != "system-cluster-critical" {
		t.Errorf("priorityClassName=%q; want system-cluster-critical", pod.PriorityClassName)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Error("pod must run as non-root")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("pod must use the RuntimeDefault seccomp profile")
	}

	container, ok := findContainer(pod.Containers, "cluster-autoscaler")
	if !ok {
		t.Fatal("Deployment has no cluster-autoscaler container")
	}
	assertImmutableImage(t, container.Image)
	if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() {
		t.Error("container must have non-zero CPU and memory requests")
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation {
		t.Error("container must disable privilege escalation")
	}
	if security == nil || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem {
		t.Error("container must use a read-only root filesystem")
	}
	if security == nil || security.Capabilities == nil || !containsCapability(security.Capabilities.Drop, "ALL") {
		t.Error("container must drop all Linux capabilities")
	}
	assertXelonSecretEnvironment(t, container.Env)

	wantArgs := []string{
		"--cloud-provider=xelon",
		"--nodes=1:3:REPLACE_XKS_POOL_ID",
		"--max-nodes-per-scaleup=1",
		"--max-scale-down-parallelism=1",
		"--force-delete-unregistered-nodes=false",
		"--force-delete-failed-nodes=false",
	}
	for _, want := range wantArgs {
		if !containsString(container.Args, want) {
			t.Errorf("container args do not contain %q", want)
		}
	}
	for _, arg := range container.Args {
		if strings.HasPrefix(arg, "--cloud-config") {
			t.Errorf("recommended XKS manifest must use xelon-api-credentials environment variables, found %q", arg)
		}
	}
}

func assertXelonSecretEnvironment(t *testing.T, environment []corev1.EnvVar) {
	t.Helper()
	want := map[string]string{
		baseURLEnv:             "baseUrl",
		clientIDEnv:            "clientId",
		kubernetesClusterIDEnv: "kubernetesClusterId",
		tokenEnv:               "token",
	}
	for name, key := range want {
		env, ok := findEnv(environment, name)
		if !ok {
			t.Errorf("container has no %s environment variable", name)
			continue
		}
		if env.Value != "" {
			t.Errorf("%s must not contain a literal value", name)
		}
		if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
			t.Errorf("%s must come from a Secret", name)
			continue
		}
		if env.ValueFrom.SecretKeyRef.Name != "xelon-api-credentials" {
			t.Errorf("%s Secret=%q; want xelon-api-credentials", name, env.ValueFrom.SecretKeyRef.Name)
		}
		if env.ValueFrom.SecretKeyRef.Key != key {
			t.Errorf("%s Secret key=%q; want %s", name, env.ValueFrom.SecretKeyRef.Key, key)
		}
	}
}

func assertImmutableImage(t *testing.T, image string) {
	t.Helper()
	const repository = "xelonag/cluster-autoscaler-xelon:"
	if !strings.HasPrefix(image, repository) {
		t.Errorf("unexpected image repository %q", image)
		return
	}

	parts := strings.Split(image, "@sha256:")
	if len(parts) != 2 || parts[1] == "" {
		t.Errorf("image %q is not pinned by a sha256 digest", image)
		return
	}
	tag := strings.TrimPrefix(parts[0], repository)
	if tag == "" || tag == "latest" {
		t.Errorf("image %q must use an explicit non-latest tag", image)
	}

	digest := parts[1]
	if digest == "REPLACE_WITH_RELEASE_DIGEST" {
		documentation, err := os.ReadFile("README.md")
		if err != nil {
			t.Errorf("read manifest documentation: %v", err)
			return
		}
		if !bytes.Contains(documentation, []byte("REPLACE_WITH_RELEASE_DIGEST")) {
			t.Error("release digest placeholder is not documented in README.md")
		}
		return
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != 32 {
		t.Errorf("image %q has an invalid sha256 digest", image)
	}
}

func assertNoDangerousPolicyRules(t *testing.T, name string, rules []rbacv1.PolicyRule) {
	t.Helper()
	for _, rule := range rules {
		if containsString(rule.Resources, "*") || containsString(rule.Verbs, "*") {
			t.Errorf("%s must not grant wildcard resources or verbs", name)
		}
		if containsString(rule.Resources, "secrets") {
			t.Errorf("%s must not grant access to Secrets", name)
		}
		if containsString(rule.APIGroups, "extensions") {
			t.Errorf("%s must not grant access to the removed extensions API group", name)
		}
		if containsString(rule.Resources, "cronjobs") && (containsString(rule.Verbs, "create") || containsString(rule.Verbs, "patch") || containsString(rule.Verbs, "update") || containsString(rule.Verbs, "delete")) {
			t.Errorf("CronJob rule grants unnecessary write verbs: %v", rule.Verbs)
		}
	}
}

func findContainer(containers []corev1.Container, name string) (*corev1.Container, bool) {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i], true
		}
	}
	return nil, false
}

func findEnv(environment []corev1.EnvVar, name string) (*corev1.EnvVar, bool) {
	for i := range environment {
		if environment[i].Name == name {
			return &environment[i], true
		}
	}
	return nil, false
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}

func containsCapability(values []corev1.Capability, want corev1.Capability) bool {
	return slices.Contains(values, want)
}

func containsToleration(values []corev1.Toleration, want corev1.Toleration) bool {
	return slices.Contains(values, want)
}
