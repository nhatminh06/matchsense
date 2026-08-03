# Security

This documents the controls that actually exist in this repository's CI/CD
and deployment pipeline. It is not a compliance or certification claim.

## Secret scanning

Gitleaks runs on every push and pull request (`ci.yml`'s `gitleaks` job,
full git history via `fetch-depth: 0`), so a committed secret fails CI
before it can be reviewed or merged.

## Image scanning

Every service image is scanned with Trivy before being pushed, blocking on
any CRITICAL-severity, fixable vulnerability
(`aquasecurity/trivy-action`, pinned to a specific release commit — see
each service's `.github/workflows/<service>.yaml`).

## Software Bill of Materials (SBOM)

A CycloneDX SBOM is generated for every built image and uploaded as a
90-day workflow artifact, so the exact dependency set for any deployed
image is reconstructable after the fact.

## Image signing & verification

Images are signed with **Cosign** using keyless/OIDC signing — no signing
key is stored as a secret; the signature is tied to the GitHub Actions
workload identity. A Kyverno policy
(`gitops/platform/kyverno/policies/verify-signed-images.yaml`) verifies
that signature at admission time, so an unsigned or incorrectly-signed
image cannot run in the cluster even if someone pushed it to GHCR directly.

## Digest pinning

`disallow-latest-tag` (Kyverno) requires every deployment to reference an
image by tag or digest, never `:latest`; in practice, the GitOps pipeline
always pins by digest (see [deployment.md](deployment.md)), so what's
running is exactly what was scanned and signed — no possibility of a tag
being silently repointed to different bits after the fact.

## Admission policies (Kyverno)

- `disallow-latest-tag`
- `disallow-privileged-containers`
- `require-resource-limits`
- `verify-signed-images`

## Least-privilege CI permissions

Workflows declare an explicit, minimal `permissions:` block rather than
relying on the (broader) repository default; the `build-and-deploy` jobs
that need to push images/open PRs/sign are the only ones granted
`contents: write` / `packages: write` / `id-token: write`. Everything
`pull_request`-triggered (`ci.yml`, `pr-title.yml`, `dependency-review.yml`)
runs with read-only `contents` permission.

## Container hardening

All 5 services' Docker images run as a non-root user (`USER app`, uid
10001) in their final stage.

## Responsible vulnerability reporting

See [SECURITY.md](../SECURITY.md) at the repository root.
