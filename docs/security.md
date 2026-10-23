# Security

This documents the controls that actually exist in this repository's CI/CD
and deployment pipeline. It is not a compliance or certification claim.

## Secret scanning

Gitleaks runs on every push and pull request (`ci.yml`'s `gitleaks` job,
full git history via `fetch-depth: 0`), so a committed secret fails CI
before it can be reviewed or merged.

## Grafana credentials

Grafana has no deployable default password. Generate a unique value with
`openssl rand -base64 32`, store it in a password manager, and never commit it.
For Docker Compose, copy `.env.example` to the Git-ignored `.env`, then set
`GRAFANA_ADMIN_PASSWORD` there. Compose fails before startup if it is missing
or empty, and all published development ports listen only on `127.0.0.1`.

Kubernetes expects a Secret that is managed outside Git. Create it before the
Grafana ArgoCD Application synchronizes:

```bash
read -rsp 'Grafana admin password: ' GRAFANA_PASSWORD; echo
printf %s "$GRAFANA_PASSWORD" | kubectl -n matchsense create secret generic grafana-admin \
  --from-file=admin-password=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
unset GRAFANA_PASSWORD
```

The Deployment's non-optional Secret reference fails closed: if the Secret or
`admin-password` key is absent, the Grafana container does not start. Recover
by creating the Secret with the command above and allowing ArgoCD/Kubernetes to
retry; do not add a plaintext Secret manifest or restore a default password.
Access Grafana without exposing a NodePort:

```bash
kubectl -n matchsense port-forward svc/grafana 3000:3000
```

`GF_SECURITY_ADMIN_PASSWORD` initializes a new Grafana database; changing the
environment variable does not overwrite the admin credential already stored
in an existing database. To rotate an existing installation, first reset the
database credential using Grafana's CLI, then update the external Secret to the
same value and restart Grafana during an approved maintenance window. Supply
the password interactively so it is not stored in Git or shell history:

For Docker Compose:

```bash
read -rsp 'New Grafana admin password: ' GRAFANA_PASSWORD; echo
printf '%s\n' "$GRAFANA_PASSWORD" | docker compose exec -T grafana \
  sh -c 'read -r password; grafana cli admin reset-admin-password "$password"'
# Update GRAFANA_ADMIN_PASSWORD in the ignored .env to the same value.
unset GRAFANA_PASSWORD
docker compose up -d --force-recreate grafana
```

For Kubernetes:

```bash
read -rsp 'New Grafana admin password: ' GRAFANA_PASSWORD; echo
printf '%s\n' "$GRAFANA_PASSWORD" | kubectl -n matchsense exec -i deployment/grafana -- \
  sh -c 'read -r password; grafana cli admin reset-admin-password "$password"'
printf %s "$GRAFANA_PASSWORD" | kubectl -n matchsense create secret generic grafana-admin \
  --from-file=admin-password=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
unset GRAFANA_PASSWORD
kubectl -n matchsense rollout restart deployment/grafana
```

The CLI reset, Secret update, and rollout commands modify a running cluster and
must be run manually after approval. Password rotation and Secret creation do
not change the provisioned dashboard or the Prometheus/Loki data sources: those
remain mounted from the existing repository ConfigMaps/files. Back up
persistent Grafana data before rotating production credentials.

## Remaining risks

- **High:** `event-api` and `query-api` have no authentication. Keep them on
  trusted development networks; add authenticated ingress/API authorization
  before exposing them to untrusted clients.
- **Medium:** Prometheus remains a Kubernetes NodePort and can disclose labels,
  topology, and operational metrics. Bind it locally in kind today; move it to
  ClusterIP plus authenticated port-forward/ingress in a follow-up.
- **Medium:** There are no application NetworkPolicies, so compromised pods can
  make broad east-west connections. Add default-deny ingress/egress and narrow
  service allowances after mapping required Kafka, Redis, DNS, and telemetry
  flows.

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

Grafana Alloy also runs as a non-root user with a read-only root filesystem,
no added capabilities, and no privilege escalation. Log collectors never
mount the Docker socket. Kubernetes collection uses namespace-scoped read-only
RBAC for `pods` and `pods/log`; it has no ClusterRole, DaemonSet, privileged
container, or node `hostPath` mount. See [observability.md](observability.md#log-collection)
for the Docker Desktop limitation and safe alternatives.

## Responsible vulnerability reporting

See [SECURITY.md](../SECURITY.md) at the repository root.
