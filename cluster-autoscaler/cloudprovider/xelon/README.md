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

| XKS state | Count in `TargetSize` | Publish through `Nodes()` |
| --- | ---: | ---: |
| `Created` | yes | only with a non-empty, unique `LocalVMID` |
| `Deployed` | yes | yes; `LocalVMID` is required |
| anything else | fail closed | fail closed |

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

## Configure and deploy

Copy `examples/cluster-autoscaler.yaml`, then replace:

- `REPLACE_XELON_TOKEN`;
- `REPLACE_XELON_CLIENT_ID`;
- `REPLACE_XKS_CLUSTER_ID`;
- `REPLACE_XKS_POOL_ID`;
- the image reference if necessary.

The deployment must retain both defensive limits:

```text
--max-nodes-per-scaleup=1
--max-scale-down-parallelism=1
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
