# Troubleshooting

## Local (Docker Compose)

**Services log "connection refused" to Kafka/Redis on first boot.**
Expected — Kafka in particular takes a few seconds longer to become ready
than the app services. Each service retries internally
(`ml-predictor` retries its Kafka connection up to 30 times; the Go
services reconnect via their client libraries' own retry behavior).

**Grafana dashboard is empty.** `match-simulator` needs a few seconds after
startup to begin emitting events, and `event-processor`/`ml-predictor` need
at least one full event round-trip before Redis has anything for
`query-api` to serve or Prometheus has anything to plot.

**`docker compose build` fails on a Go service with a missing
`services/common` error.** The 4 Go services build from the repository
root as their Docker context (not their own service directory), because
their Dockerfiles `COPY services/common`. Make sure you're running
`docker compose build` (or `build.context: .` is intact in
`docker-compose.yaml`) rather than `docker build` directly inside a service
folder.

**Grafana admin password.** Defaults to `matchsense` locally. Override with
`GRAFANA_ADMIN_PASSWORD` — required for anything beyond pure local
development.

## CI

**A GitOps update PR isn't getting a `ci.yml` check run.** Expected with
the default `GITHUB_TOKEN` — see the "Known limitation" note in
[deployment.md](deployment.md#gitops-update-pull-requests). Add a
`MATCHSENSE_BOT_TOKEN` secret to get automatic checks on these PRs.

**`hadolint` failing in CI on an unpinned `apk add` package version.**
Intentional — see the comment in `ci.yml`'s `dockerfile-lint` job. The
Alpine base images used here are already digest/tag-pinned; pinning
individual `apk` package versions on top would go stale and block routine
base-image bumps.

## Kubernetes / ArgoCD

**A workload isn't being admitted to the cluster.** Check the Kyverno
policy report first — the most common cause is an unsigned image (should
never happen via the normal CI pipeline, since every pushed image is
signed) or a missing resource-limits block.
