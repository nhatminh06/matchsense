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

**An application container fails to start with a syslog connection error.**
The Docker daemon cannot reach Alloy at `tcp://127.0.0.1:1514`. First verify
`docker compose up -d loki alloy` succeeds. Native Linux Docker Engine uses the
default address. Docker Desktop may require
`ALLOY_SYSLOG_ADDRESS=tcp://host.docker.internal:1514`; support varies by
Desktop backend. Follow [the compatibility guidance](observability.md#log-collection)
and do not mount `docker.sock` as a workaround.

**Loki has no application logs.** Check `docker compose logs alloy loki`, then
query `{service_name="event-api"}` in Grafana Explore. On Kubernetes, also run
`kubectl -n matchsense auth can-i get pods/log --as=system:serviceaccount:matchsense:alloy`
and check the Alloy pod. The collector intentionally ignores pods outside the
`matchsense` namespace and applications outside its documented allowlist.

**`docker compose build` fails on a Go service with a missing
`services/common` error.** The 4 Go services build from the repository
root as their Docker context (not their own service directory), because
their Dockerfiles `COPY services/common`. Make sure you're running
`docker compose build` (or `build.context: .` is intact in
`docker-compose.yaml`) rather than `docker build` directly inside a service
folder.

**Compose says `GRAFANA_ADMIN_PASSWORD` is required.** Copy `.env.example` to
the ignored `.env`, generate a password with `openssl rand -base64 32`, store
it in a password manager, and assign it to `GRAFANA_ADMIN_PASSWORD`. Empty
values are intentionally rejected.

**Compose says `EVENT_API_KEY` is required, or event ingestion returns 401.**
Generate a key with `openssl rand -hex 32`, store it in a password manager, and
set the same value in the ignored `.env` used by Compose clients. Kubernetes
uses the external `event-api-auth` Secret with key `api-key`; if it is missing,
both event-api and the simulator intentionally remain pending rather than
starting without authentication.

**Event ingestion returns 429.** The event-api token bucket is per instance and
defaults to 20 requests/second with a burst of 40. The simulator retries 429
responses using its existing bounded backoff. Adjust `EVENT_API_RATE_LIMIT` and
`EVENT_API_RATE_BURST` only after measuring legitimate ingestion traffic.

**A changed Grafana password is not accepted.** Grafana uses
`GF_SECURITY_ADMIN_PASSWORD` only when initializing a new database; changing
the environment or Secret does not reset an existing database credential. Use
the CLI rotation procedure in [security.md](security.md#grafana-credentials)
and update the external Secret to match. This preserves provisioned dashboards
and Prometheus/Loki data sources.

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

**Grafana is pending with a missing `grafana-admin` Secret.** This is the
intended fail-closed state. Create the Secret outside Git using the procedure
in [security.md](security.md#grafana-credentials), verify the
`admin-password` key exists, and let Kubernetes retry. Do not weaken the
reference, commit a Secret manifest, or add a fallback password. Once running,
connect with:

```bash
kubectl -n matchsense port-forward svc/grafana 3000:3000
```
