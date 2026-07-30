# Xelon Cluster Autoscaler v0

This is a thin Xelon-only integration based on Cluster Autoscaler `1.35.2` at
upstream commit `2d42588803c71fe9b35dcd9e3669ac6bb550ca22`. It manages exactly one
existing XKS worker pool through `xelon-sdk-go v1.14.4` and the modern
`KubernetesService` methods `GetNodePool`, `CreateNode`, and `DeleteNode`.

The first demonstration is intentionally limited to `2 -> 3 -> 2`:

- one explicit `--nodes=<min>:<max>:<pool-id>` entry;
- `min >= 1` and at least one stable, Ready, registered worker;
- one worker added or removed per provider call;
- no autodiscovery, scale from zero, worker-pool creation, or external gRPC;
- `DecreaseTargetSize`, force deletion, and restart during an unresolved
  mutation are unsupported;
- `Deleting`, `Changing resources`, `Error`, and unknown XKS worker states fail
  closed.

## Worker and identity contracts

| XKS state     | Count in `TargetSize` |                 Publish through `Nodes()` |
|---------------|----------------------:|------------------------------------------:|
| `Created`     |                   yes | only with a non-empty, unique `LocalVMID` |
| `Deployed`    |                   yes |              yes; `LocalVMID` is required |
| anything else |           fail closed |                               fail closed |

Destructive calls use this exact chain:

```text
Node.Spec.ProviderID
  -> strict xelon://<LocalVMID> parse
  -> exactly one match in the configured worker pool
  -> XKS worker identifier
  -> DeleteNode(clusterID, workerID)
```

Kubernetes node names are never used as destructive identity. A zero or
multiple match stops before the API call.

Mutations are serialized, sent once, and reconciled through bounded pool reads.
An add succeeds only when exactly one worker ID appears, the target increases by
one, and all baseline workers remain. A delete succeeds only when the selected
worker ID disappears, the target decreases by one, and all unrelated workers
remain. An unresolved outcome latches the group mutation-unsafe; it is never
blindly retried.

## Build and verify the image

Run from `cluster-autoscaler/` on a committed Xelon revision:

```bash
docker buildx build \
  --platform linux/amd64 \
  --load \
  --build-arg XELON_VERSION=1.35.2-xelon.0 \
  --build-arg XELON_REVISION="$(git rev-parse HEAD)" \
  --file cloudprovider/xelon/Dockerfile \
  --tag xelonag/cluster-autoscaler-xelon:v1.35.2-xelon.0 \
  .
```

For an arm64 XKS control plane, replace the platform with `linux/arm64`.

Verify the release, upstream CA version, and both source revisions:

```bash
docker image inspect \
  --format '{{ index .Config.Labels "org.opencontainers.image.version" }} {{ index .Config.Labels "org.opencontainers.image.revision" }} {{ index .Config.Labels "io.xelon.cluster-autoscaler.upstream.version" }} {{ index .Config.Labels "io.xelon.cluster-autoscaler.upstream.revision" }}' \
  xelonag/cluster-autoscaler-xelon:v1.35.2-xelon.0
```

The expected upstream revision is
`2d42588803c71fe9b35dcd9e3669ac6bb550ca22`.

## Publish the image

Pushing a tag matching `v*-xelon.*` runs
`.github/workflows/xelon-image-publishing.yaml`. The workflow publishes a
multi-platform `linux/amd64` and `linux/arm64` image to
`xelonag/cluster-autoscaler-xelon:<tag>`.

Release tags are immutable: the workflow stops if the Docker Hub tag already
exists or if it cannot prove that the tag is unused. A successful run reports
the multi-platform image digest in its job summary, signs that digest with
Cosign using the workflow's GitHub OIDC identity, and publishes a provenance
attestation for the same digest.

The image labels record the tagged Xelon commit as
`org.opencontainers.image.revision` and retain the pinned upstream version and
revision in the `io.xelon.cluster-autoscaler.upstream.*` labels.

Install [Cosign](https://docs.sigstore.dev/cosign/system_config/installation/),
then verify a release by using the digest reported in the workflow's job
summary. The certificate identity includes the exact release tag, so update
both placeholders together:

```bash
cosign verify \
  --certificate-identity "https://github.com/Xelon-AG/autoscaler/.github/workflows/xelon-image-publishing.yaml@refs/tags/v1.35.2-xelon.0" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "index.docker.io/xelonag/cluster-autoscaler-xelon@sha256:REPLACE_WITH_DIGEST"
```

A successful verification exits with status zero and prints the validated
signature payload. Specifying the expected workflow identity and OIDC issuer is
required; do not replace them with unrestricted regular expressions.

## Configure and deploy

The recommended XKS deployment reuses the `xelon-api-credentials` Secret used
by the [Xelon CCM chart](https://github.com/Xelon-AG/xelon-cloud-controller-manager/blob/main/charts/xelon-ccm/templates/secret.yaml).
The example manifest does not create or own this Secret. It maps the existing
CCM keys to the same environment variables used by CCM:

| Secret key            | Environment variable          | Required value            |
|-----------------------|-------------------------------|---------------------------|
| `baseUrl`             | `XELON_BASE_URL`              | `REPLACE_XELON_BASE_URL`  |
| `clientId`            | `XELON_CLIENT_ID`             | `REPLACE_XELON_CLIENT_ID` |
| `kubernetesClusterId` | `XELON_KUBERNETES_CLUSTER_ID` | `REPLACE_XKS_CLUSTER_ID`  |
| `token`               | `XELON_TOKEN`                 | `REPLACE_XELON_TOKEN`     |

Confirm that the Secret exists in `kube-system` and contains all four keys
without printing their values:

```bash
kubectl -n kube-system describe secret xelon-api-credentials
```

The token must be authorized to read the configured XKS worker pool and create
and delete its workers. Preserve the CCM-owned `cloudId` key when rotating or
updating this shared Secret. Manage shared credentials through the existing
XKS or CCM provisioning process; do not copy them into this manifest.

If a standalone cluster does not already have the Secret, prepare four
protected files named `baseUrl`, `clientId`, `kubernetesClusterId`, and `token`
outside the source checkout, set their modes to `0600`, and create it once:

```bash
kubectl -n kube-system create secret generic xelon-api-credentials \
  --from-file=baseUrl=/secure/path/xelon-api-credentials/baseUrl \
  --from-file=clientId=/secure/path/xelon-api-credentials/clientId \
  --from-file=kubernetesClusterId=/secure/path/xelon-api-credentials/kubernetesClusterId \
  --from-file=token=/secure/path/xelon-api-credentials/token
```

Do not commit the protected files. The Deployment receives these values only
through `secretKeyRef`; its service account has no RBAC permission to read
Secrets.

Alternatively, a standalone installation can use a protected JSON cloud
config:

```json
{
  "base_url": "REPLACE_XELON_BASE_URL",
  "token": "REPLACE_XELON_TOKEN",
  "client_id": "REPLACE_XELON_CLIENT_ID",
  "cluster_id": "REPLACE_XKS_CLUSTER_ID"
}
```

The base URL must use HTTP(S) and end in `/`. Create a separately managed
Secret from the file, mount its `cloud-config.json` key read-only at
`/etc/xelon/cloud-config.json`, and add this argument to the Deployment:

```bash
kubectl -n kube-system create secret generic xelon-cluster-autoscaler-cloud-config \
  --from-file=cloud-config.json=/secure/path/cloud-config.json
```

In a copy of the example manifest, remove the four `XELON_*` environment
entries and add the following container mount, argument, and pod volume:

```yaml
spec:
  template:
    spec:
      containers:
        - name: cluster-autoscaler
          args:
            - --cloud-config=/etc/xelon/cloud-config.json
          volumeMounts:
            - name: cloud-config
              mountPath: /etc/xelon/cloud-config.json
              subPath: cloud-config.json
              readOnly: true
      volumes:
        - name: cloud-config
          secret:
            secretName: xelon-cluster-autoscaler-cloud-config
```

The snippet is illustrative; retain the image, complete argument list,
resources, and security contexts from the example manifest.

The resulting container argument is:

```text
--cloud-config=/etc/xelon/cloud-config.json
```

When `--cloud-config` is set, the file is authoritative. An unreadable or
invalid file stops startup; the provider never fills missing file fields from
environment variables. When the argument is absent, all configuration comes
from the environment.

Copy `examples/cluster-autoscaler.yaml`, then replace:

- `REPLACE_XKS_POOL_ID`;
- `REPLACE_WITH_RELEASE_DIGEST` with the `sha256` digest reported by the
  release workflow.

Keep the version tag and digest together in the image reference. Kubernetes
pulls by digest, so the deployed release remains immutable even if a registry
tag is changed later. The manifest uses one replica for v0 and grants the
standard Cluster Autoscaler Kubernetes permissions only; it grants no access
to Secrets through the Kubernetes API and no Xelon-specific Kubernetes API
permissions.

The deployment must retain all four v0 safety flags:

```text
--max-nodes-per-scaleup=1
--max-scale-down-parallelism=1
--force-delete-unregistered-nodes=false
--force-delete-failed-nodes=false
```

The first flag guides CA's estimator but is not a provider guarantee. The Xelon
provider independently rejects `IncreaseSize(delta != 1)` and any delete batch
whose length is not one.

Apply the manifest and follow the logs:

```bash
kubectl apply -f cloudprovider/xelon/examples/cluster-autoscaler.yaml
kubectl -n kube-system rollout status deployment/xelon-cluster-autoscaler
kubectl -n kube-system logs -f deployment/xelon-cluster-autoscaler
```

Before inducing load, confirm that both existing workers are Ready and have
unique `xelon://...` provider IDs. Then create a controlled unschedulable
workload requiring one additional worker. Preserve the CA and XKS lifecycle
logs demonstrating target `2 -> 3`, node registration, exact-node selection,
and target `3 -> 2` after scale-down.

## Roll back or uninstall

For an image-only rollback, restore the previous Deployment revision and wait
for its single replica:

```bash
kubectl -n kube-system rollout undo deployment/xelon-cluster-autoscaler
kubectl -n kube-system rollout status deployment/xelon-cluster-autoscaler
```

Also restore `REPLACE_WITH_RELEASE_DIGEST` in your saved manifest to the prior
known-good digest; otherwise a later `kubectl apply` will deploy the newer
image again. If RBAC, arguments, the pool ID, or other manifest fields changed,
reapply the complete previously reviewed manifest instead of relying only on
`rollout undo`.

To roll back shared credentials or Xelon configuration, restore the previous
`xelon-api-credentials` values through the XKS or CCM process that owns the
Secret, then restart and verify the Deployment:

```bash
kubectl -n kube-system rollout restart deployment/xelon-cluster-autoscaler
kubectl -n kube-system rollout status deployment/xelon-cluster-autoscaler
```

To uninstall v0, delete only the manifest-owned resources. Do not delete the
shared `xelon-api-credentials` Secret; Xelon CCM may still depend on it.

```bash
kubectl delete -f cloudprovider/xelon/examples/cluster-autoscaler.yaml
```

If the standalone file mode was used, the separately managed
`xelon-cluster-autoscaler-cloud-config` Secret may be deleted after confirming
that no other workload uses it.

## Tests

```bash
GOCACHE=/tmp/xelon-autoscaler-go-cache \
  go test -tags xelon \
  ./cloudprovider/xelon \
  ./cloudprovider/xelon/integration \
  ./cloudprovider/builder
```

The integration package contains the CA 1.35.2 restart gate: target size three,
two Kubernetes/provider instances, a fresh CA state registry, one upcoming
worker, and no duplicate `IncreaseSize` call.

For the complete pinned upstream tree under Go 1.26, run:

```bash
GOCACHE=/tmp/xelon-autoscaler-go-cache \
  go test -vet=off -tags xelon ./...
```

The `-vet=off` exception applies only to the complete upstream tree: CA 1.35.2
contains pre-existing format-string findings in unrelated providers and core
packages under Go 1.26. The focused Xelon packages pass `go vet` normally and
must not use this exception.

Pinned upstream FAQ:

<https://github.com/kubernetes/autoscaler/blob/cluster-autoscaler-1.35.2/cluster-autoscaler/FAQ.md>
