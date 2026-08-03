# Deployment & GitOps

## Image build pipeline

Each of the 5 services has its own CI workflow
(`.github/workflows/<service>.yaml`), triggered on push to `main` (path-
filtered to that service's directory) or manually via `workflow_dispatch`.
On a successful trigger, the `build-and-deploy` job:

1. Builds the Docker image locally (not pushed yet)
2. Runs a **Trivy** vulnerability scan, blocking the pipeline on any
   CRITICAL-severity, fixable CVE
3. Generates a **CycloneDX SBOM** and uploads it as a workflow artifact
4. Pushes the image to **GHCR** (`ghcr.io/<owner>/<service>`), tagged with
   both the commit SHA and `latest`
5. Signs the pushed image with **Cosign** (keyless/OIDC — no signing key is
   stored as a secret)
6. Updates the corresponding `gitops/apps/<service>/base/deployment.yaml`
   with the new image digest, and opens a **pull request** with that
   change (see below) rather than pushing straight to `main`

This only runs after a push lands on `main` (i.e. after a PR is merged), so
it only ever builds, scans, and signs a commit that already passed the
`ci.yml` checks during review.

## GitOps update pull requests

The GitOps-manifest update is opened as a real PR
(`chore(gitops): update <service> image digest`, branch
`gitops/<service>-<sha>`) using the `peter-evans/create-pull-request`
action, with the PR body including the source commit, the exact image
digest, a link to the workflow run, and the Trivy/SBOM results.

**Known limitation:** a PR opened with the default `GITHUB_TOKEN` does not
trigger other workflows on itself (this is a deliberate GitHub Actions
safeguard against recursive workflow runs, not a bug). That means this PR
won't automatically get a `ci.yml` check run the way a human-authored PR
would. It's still safe to review and merge manually. To get automatic
checks on these GitOps PRs, add a fine-grained personal access token (repo
contents + pull-requests, read/write) as the `MATCHSENSE_BOT_TOKEN`
repository secret — the workflow already falls back to it automatically
(`secrets.MATCHSENSE_BOT_TOKEN || secrets.GITHUB_TOKEN`) if it's present.

Because the GitOps PR only ever touches
`gitops/apps/<service>/base/deployment.yaml`, and each service's workflow
only triggers on changes under `services/<service>/**`, merging a GitOps PR
does not re-trigger that service's own build — no infinite loop.

## ArgoCD reconciliation

Each service has an ArgoCD `Application` manifest under
`gitops/argocd-apps/`, pointing at `gitops/apps/<service>/overlays/dev` with
automated sync, prune, and self-heal enabled. Once a GitOps PR is merged,
ArgoCD picks up the new image digest on its next sync and rolls the
deployment.

## Kustomize overlays

Each service has a `base/` (Deployment manifest) and an `overlays/dev/`
(currently just references `base/` — a placeholder for future
environment-specific overlays).

## Kyverno enforcement

Cluster-wide admission policies (`gitops/platform/kyverno/policies/`):

- `disallow-latest-tag` — every image reference must be pinned (by tag or
  digest), never `:latest`
- `disallow-privileged-containers` — no privileged containers
- `require-resource-limits` — every container must declare resource limits
- `verify-signed-images` — the image's Cosign signature must verify against
  the expected CI identity before the workload is admitted

## Rollback procedure

Since deployments are digest-pinned and driven by GitOps PRs, rolling back
is a normal git operation:

1. Find the last-known-good digest — either from a previous merged GitOps
   PR, or via `git log -- gitops/apps/<service>/base/deployment.yaml`
2. Revert the merge commit, or open a new PR pinning the manifest back to
   that digest
3. Merge; ArgoCD reconciles the rollback the same way it reconciles a
   forward deploy

There's no automated rollback trigger today — this is a manual, git-driven
process.
