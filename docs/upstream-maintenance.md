# Maintaining the Xelon Cluster Autoscaler fork

This document is the canonical guide for maintaining, updating, releasing, and
handing over the Xelon Cluster Autoscaler fork.

## Fork-maintenance principle

This repository is a downstream fork of Kubernetes Cluster Autoscaler. It is
maintained as a small Xelon-specific patch set on a stable upstream release:

```text
stable upstream Cluster Autoscaler
+ small Xelon-specific patch set
= Xelon Cluster Autoscaler release
```

In terms of the contents of that patch set:

```text
upstream Cluster Autoscaler release
+ Xelon cloud-provider changes
= Xelon Cluster Autoscaler release
```

Keep Xelon changes small, isolated, and reviewable. The fork must not become an
independently diverging autoscaler. Preserve upstream history and make every
downstream difference easy to identify and justify.

Production releases must use stable upstream Cluster Autoscaler tags. Never
base a production release on upstream `master` or on an alpha, beta, or
release-candidate revision.

## Versioning policy

Xelon releases use this format:

```text
v<upstream-cluster-autoscaler-version>-xelon.<revision>
```

Examples:

```text
v1.35.2-xelon.0
v1.35.2-xelon.1
v1.35.3-xelon.0
v1.36.0-xelon.0
```

The `.0` suffix is the first Xelon release on an upstream baseline. Xelon-only
features, fixes, deployment changes, or build changes increment the suffix.
Changing the upstream Cluster Autoscaler version resets the suffix to `.0`.

```text
v1.35.2-xelon.0
→ Xelon-only feature or fix
→ v1.35.2-xelon.1
```

```text
v1.35.2-xelon.2
→ update upstream CA to 1.35.3
→ v1.35.3-xelon.0
```

Release tags are immutable. Never move, recreate, or overwrite a release tag.

Once a Xelon release has been published from a branch, that published branch
history is append-only. Never rebase or force-push a published active or
maintenance line. Continue maintenance through normal commits and merges so
existing release tags, source commits, images, signatures, and provenance keep
a stable relationship.

## Kubernetes and Cluster Autoscaler compatibility

The Cluster Autoscaler minor version should match the Kubernetes minor
version:

```text
Kubernetes 1.35 → Cluster Autoscaler 1.35.x
Kubernetes 1.36 → Cluster Autoscaler 1.36.x
```

Within each supported minor, track stable upstream patch releases. Publish a
Xelon release only after the combined upstream release and Xelon patch set pass
Xelon validation.

## Branch model

### `xelon/master`

After the rewrite is promoted, `xelon/master` is the active Xelon development
branch. It is protected: direct pushes are not allowed, and changes are
reviewed through pull requests.

### `xelon/release-<minor>`

A branch such as `xelon/release-1.35` maintains an older supported minor. It is
created when active development moves to the next Kubernetes and Cluster
Autoscaler minor. Stable upstream patch releases are merged into the branch
that owns that Kubernetes minor: `xelon/master` while the minor is active, or
the corresponding maintenance branch after active development has moved on.

### `legacy/xelon-master`

This branch preserves the historical pre-rewrite implementation. Do not use it
as the base for new development.

### `rewrite/xelon-v0`

This is the temporary stabilization branch for the initial rewrite. After the
first release, it is replaced by `xelon/master` and deleted as described in
[Initial rewrite promotion](#initial-rewrite-promotion).

### Short-lived branches

Create normal, focused branches from the applicable active or maintenance
branch. Examples include:

```text
feat/xelon-...
fix/xelon-...
ci/xelon-...
docs/xelon-...
sync/ca-1.35.3
```

## Same-minor upstream patch update

The following runbook updates Cluster Autoscaler `1.35.2` to `1.35.3` without
changing the supported Kubernetes minor.

### 1. Verify remotes

Confirm that the remotes identify these repositories:

```text
origin   → Xelon-AG/autoscaler
upstream → kubernetes/autoscaler
```

Use `git remote -v` to verify the actual fetch and push URLs before continuing.

### 2. Fetch both repositories

```bash
git fetch origin --prune
git fetch upstream --tags --prune
```

### 3. Set and verify the exact upstream tag

```bash
tag=cluster-autoscaler-1.35.3

git show-ref --verify "refs/tags/$tag"
git rev-parse "$tag^{commit}"
git ls-remote upstream \
  "refs/tags/$tag" \
  "refs/tags/$tag^{}"
```

Upstream release tags may be annotated tag objects rather than commits.
`^{commit}` peels an annotated tag to the commit it identifies and also fails
if the name cannot resolve to a commit. This makes the merge and ancestry
checks operate on the intended release commit. Compare the local result with
the remote tag and its peeled `^{}` entry before proceeding.

### 4. Verify expected ancestry

```bash
git merge-base --is-ancestor \
  'cluster-autoscaler-1.35.2^{commit}' \
  "$tag^{commit}"
```

A zero exit status confirms that the new patch release descends from the
current upstream baseline. Stop and investigate any unexpected ancestry.

### 5. Create a synchronization branch

Create the synchronization branch from the branch that owns the Kubernetes
minor. For `1.35`, that is `xelon/master` while `1.35` is active and
`xelon/release-1.35` after active development has moved to a newer minor.

```bash
target_branch=xelon/master
# If 1.35 is already a maintenance line:
# target_branch=xelon/release-1.35

git switch --create sync/ca-1.35.3 "origin/$target_branch"
```

### 6. Merge the exact upstream release

```bash
git merge --no-ff --no-commit "$tag^{commit}"
```

Using the peeled tag makes the selected stable upstream release, rather than a
moving branch, the merge target.

### 7. Resolve only genuine downstream conflicts

Preserve both upstream intent and Xelon intent. Do not blindly select all of
`ours` or all of `theirs`. Treat `go.mod`, `go.sum`, provider and binary builder
wiring, CI, and build files especially carefully because a superficially
simple resolution can change the distribution.

Do not use an upstream update for unrelated refactoring or cleanup. A clean
textual merge does not prove semantic compatibility: upstream interfaces,
defaults, or behavior may have changed without causing a conflict. If a
resolution is unclear, stop and restore the pre-merge state with:

```bash
git merge --abort
```

### 8. Review the resulting downstream delta

```bash
git diff --stat "$tag^{commit}"
git diff --name-status "$tag^{commit}"
```

Inspect the full diff as well. Every remaining difference from the upstream
tag must be explainable as part of the Xelon distribution.

### 9. Verify and commit

Run all applicable [release gates](#release-gates), then create the merge
commit:

```bash
git commit \
  -m "chore(upstream): merge cluster-autoscaler 1.35.3"
```

### 10. Push and open a pull request

```bash
git push -u origin sync/ca-1.35.3
```

Open the pull request back to the same branch that owns the Kubernetes minor:

```text
base:    <target branch for Kubernetes 1.35>
compare: sync/ca-1.35.3
```

For example, the base is `xelon/master` while `1.35` is active and
`xelon/release-1.35` after it becomes a maintenance line.

The review must cover both conflict resolutions and the complete downstream
delta from the new upstream tag.

### 11. Release

After approval, merge, and successful release verification, publish the first
Xelon release on the new baseline:

```text
v1.35.3-xelon.0
```

## New Kubernetes and Cluster Autoscaler minor update

A move from `1.35` to `1.36` establishes a new compatibility line and is not a
routine same-minor merge.

### 1. Preserve the previous line

```bash
git branch xelon/release-1.35 xelon/master
git push origin xelon/release-1.35
```

Protect the maintenance branch according to the repository policy.

### 2. Select and verify the exact stable upstream release

Select an exact, stable upstream `cluster-autoscaler-1.36.x` tag. Verify and
peel the tag as described in the same-minor runbook. Do not use `master`, a
prerelease, or another moving ref.

### 3. Merge the exact upstream release into the active line

Preserve the published Xelon history. Create a focused synchronization branch
from `xelon/master`, then merge the peeled upstream release commit:

```bash
git switch --create sync/ca-1.36.x origin/xelon/master
git merge --no-ff --no-commit 'cluster-autoscaler-1.36.x^{commit}'
```

Resolve only genuine downstream conflicts, run the required verification, then
commit and open a pull request back to `xelon/master`. Do not rebase,
force-push, or replace the published `xelon/master` history.

### 4. Review the Xelon delta against the new upstream baseline

Review the resulting tree against the selected upstream release. The remaining
downstream differences should still be limited to the pieces required for the
Xelon distribution:

- Xelon cloud provider;
- provider and binary wiring;
- Xelon tests;
- deployment manifest;
- Dockerfile;
- Xelon CI and release workflow;
- required SDK and Go toolchain changes;
- maintenance documentation.

Every remaining difference must be explainable. The new minor update is a
compatibility operation, not an opportunity to replay, reorder, or rewrite
already-published Xelon history.

### 5. Adapt to upstream changes

Make adaptations only for real upstream interface or behavior changes. Do not
combine unrelated feature work, cleanup, or broad refactoring with the minor
update.

### 6. Verify the combined result

Run every [release gate](#release-gates), including the complete real XKS
lifecycle test. Review the full downstream delta from the selected upstream
tag.

### 7. Release the new line

The first release on the new upstream minor resets the Xelon suffix:

```text
v1.36.x-xelon.0
```

## Xelon-only releases

A Xelon feature or fix that does not change the upstream baseline increments
only the Xelon suffix:

```text
v1.35.2-xelon.0
→ v1.35.2-xelon.1
```

Use a focused feature or fix branch, open a pull request, run normal CI, and
complete release verification. The release must not silently include an
unreviewed upstream baseline change. Verify that the recorded upstream version
and commit remain unchanged.

### Forward-porting Xelon fixes

When a Xelon fix affects more than one supported Kubernetes minor, implement it
on the oldest affected supported line first. Then forward-port the logical fix
to each newer supported line with `git cherry-pick` or an equivalent adapted
commit when upstream differences require changes.

Validate the fix independently on every target line. Do not merge an entire
older maintenance branch into a newer Kubernetes minor just to propagate a
Xelon fix; older branches also contain minor-specific upstream history and
release metadata that do not belong on newer lines.

Upstream patch updates and Xelon fix propagation are separate flows:

```text
upstream 1.35.x patch → xelon/release-1.35

Xelon fix on 1.35 → cherry-pick/adapt → 1.36 → newer supported lines
```

## Release gates

Every release must verify:

- focused Xelon provider tests;
- race tests;
- focused `go vet` and lint;
- an Xelon-tagged binary build;
- deployment YAML parsing;
- a Docker image build;
- multi-platform image publishing for `linux/amd64` and `linux/arm64`;
- provenance labels containing both the upstream and Xelon revisions;
- that the deployment manifest image tag exactly matches the Git release tag;
- that there are no unexplained downstream files;
- that the release tag and image are immutable.

Unrelated upstream Go 1.26 vet or lint findings must not be "fixed" as part of
Xelon maintenance unless they are directly relevant to the Xelon change. Keep
any explicitly documented upstream-wide exception separate from the focused
Xelon checks.

For every new Kubernetes and Cluster Autoscaler minor, and for any patch
release that affects autoscaling behavior, require this real XKS lifecycle
test:

```text
2 workers
→ unschedulable workload
→ scale to 3
→ new node registers and becomes Ready
→ reduce workload
→ exact selected node is deleted
→ return to 2 healthy workers
```

Retain enough test evidence to connect the exact source revision, image digest,
XKS worker transitions, Kubernetes node registration, selected scale-down
node, and final healthy state.

## Release artifacts

Pull requests may validate Docker builds, but they must not publish images.
Official images are published only from controlled release tags. The Docker
image tag must exactly match the Git release tag, and the final image digest
must be retained as release verification evidence. Release images are
immutable.

Example:

```text
Git tag:
v1.35.2-xelon.0

Image:
xelonag/cluster-autoscaler-xelon:v1.35.2-xelon.0
```

Record this metadata for every release:

```text
Cluster Autoscaler version
upstream commit
Xelon commit
Xelon SDK version
Go version
container image digest
```

The deployment manifest is also part of the release contract at the stable
repository path
`cluster-autoscaler/cloudprovider/xelon/examples/cluster-autoscaler.yaml`.
Before creating a release tag, its image reference must use that exact release
tag. Customers can then pin the manifest through:

```text
https://raw.githubusercontent.com/Xelon-AG/autoscaler/<release-tag>/cluster-autoscaler/cloudprovider/xelon/examples/cluster-autoscaler.yaml
```

The recorded digest is the release image's verifiable identity. The tagged
manifest intentionally uses the immutable release image tag to avoid a
circular dependency between the Git commit/tag and the image digest. Never
rebuild or replace an existing release image under the same tag, and never
move or recreate the corresponding Git tag.

## Initial rewrite promotion

> **One-time migration operation:** this section applies only to the initial
> promotion of the reviewed rewrite. It is not a routine maintenance process.

```text
rewrite/xelon-v0
→ xelon/master
```

Fully review and verify the rewrite before promotion. Preserve the previous
implementation on `legacy/xelon-master`. The final rewrite commit intentionally
replaces `xelon/master`; coordinate this repository administration operation
with maintainers and verify the exact source and destination commits.

After promotion, protect `xelon/master`, require pull requests, and start all
future active-line branches from it. Delete temporary review and rewrite
branches only after the first release from the promoted branch succeeds. This
one-time operation may require repository-administrator controls, but guarded
force-push commands are deliberately not presented as routine instructions.

## Developer handover

A handover must identify the active branch, supported release branches, exact
upstream version and commit, current Xelon release tag and image digest, Xelon
SDK and Go versions, deliberate downstream deviations, and any open maintenance
work. Include the latest release-gate results and, when required, real XKS
lifecycle evidence. Direct the receiving developer to this guide rather than
copying part of the process into a separate checklist that can drift.

## Contributor rules

### Do

- Branch from the correct active or release branch.
- Keep changes focused.
- Use pull requests.
- Preserve upstream history and attribution.
- Verify the exact upstream baseline.
- Keep the Xelon delta small.
- Document deliberate deviations.

### Do not

- Push directly to `xelon/master`.
- Use `legacy/xelon-master` for new work.
- Base production releases on upstream `master`.
- Rebase or force-push a published active or maintenance line.
- Overwrite release tags.
- Mix unrelated upstream cleanup into Xelon changes.
- Publish release images from arbitrary branches or pull requests.
- Silently weaken validation.

## Final maintenance questions

Before approving an update or release, answer:

```text
Which upstream release are we based on?
Which Xelon changes are added?
Why does every downstream difference exist?
Which verification proves the combined result works?
```
