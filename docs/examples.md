# Runnable examples & use cases

Every example below is validated end-to-end — workers execute real workflows
against a real cluster and assert results.

## 1 · Temporal as a service

### Dev stack — `nix run .#up`

One `temporalio/auto-setup` container (all four Temporal services), Postgres,
web UI, admin-tools, and the SFTP test server. Use it when you just need a
cluster on `localhost:7233`.

### Prod-topology stack — `nix run .#prod-up`

The stack to study when you want to see how Temporal *deploys* in production
(ports overlap with the dev stack — run one at a time):

| Piece | What it demonstrates |
|---|---|
| nginx :7233 → `temporal-frontend` ×2 | gRPC load balancing across frontends (the NLB role in AWS) |
| split `history` / `matching` / `worker` services | the real multi-role topology; auto-setup runs schema migration on the history node |
| Postgres + Elasticsearch | SQL persistence + advanced visibility (enables `WorkflowId STARTS_WITH …` queries and custom search attributes) |
| `temporal-namespace-setup` one-shot | infrastructure-as-config: registers `team-app` (72h retention), `team-data` (168h), and the custom search attributes on boot |
| `temporal-app-worker-python` | a **long-lived containerized worker** pinned to the `team-app` namespace — the prod pattern where workers are deployed services |
| UI :8080 behind **Dex OIDC** (:5556) | SSO login flow, end to end, no signup (`admin@example.com` / `password`) |
| Prometheus :9090 + Grafana :8085 | server metrics scraped from every Temporal service |
| worker heartbeats enabled | the UI's Workers page shows live worker CPU/memory/SDK telemetry |

Multi-tenancy use case: run any example with `TEMPORAL_NAMESPACE=team-app` and
watch it execute in an isolated namespace, handled by the containerized worker,
invisible from `default`.

Clean-slate guarantee: `docker compose -f docker-compose.prod.yml down -v &&
nix run .#prod-up` boots everything from zero (schema, ES index, namespaces,
search attributes) — validated.

### AWS deployment (CDK) — `infra/`

Import-or-create stacks: every dependency (VPC, ECS cluster, database, hosted
zone) is reused when passed via context, created when omitted. Validate
without an AWS account:

```sh
nix run .#infra-test         # 16 assertion tests (create/import/DNS/autoscaling modes)
nix run .#synth              # clean synth of both stacks
nix run .#validate-emulator  # deploy both stacks to a local AWS emulator
```

## 2 · SDK examples — `examples/`

The same workflow in four languages: a `GreetingWorkflow` executes a
`compose_greeting` activity on its own task queue; `run.sh` starts a worker,
executes the workflow, asserts the result, and exits non-zero on mismatch.

| Language | SDK | Run |
|---|---|---|
| Python | `temporalio` | `./examples/python/run.sh` |
| Go | `go.temporal.io/sdk` | `./examples/go/run.sh` |
| TypeScript | `@temporalio/*` | `./examples/typescript/run.sh` |
| C# | `Temporalio` (NuGet) | `./examples/csharp/run.sh` |

All four at once, with a cluster health gate: `nix run .#examples`.

What each example demonstrates beyond "hello": deterministic workflow code
vs side-effectful activities, task-queue routing, and the env contract every
client honors:

```sh
TEMPORAL_ADDRESS=localhost:7233     # any cluster
TEMPORAL_NAMESPACE=team-app         # any namespace
```

The Python example doubles as the prod-stack's containerized worker
(`examples/python/Dockerfile`) — same code, deployed as a service.

## 3 · The ETL — `etl/`

The full real-world example: SFTP ingestion → per-route dbt-Spark transforms
→ Iceberg catalog → curated S3, orchestrated by Temporal with parallel
fan-out, quarantine, cross-route consolidation, and a canonical data model
that generates the normalization. **Covered in depth in [etl.md](etl.md).**

The variations at a glance (all validated):

| Run | Command | What it proves |
|---|---|---|
| Single transform, prod-shaped | `./etl/run.sh` | light workers + external Spark service (Connect), EMR submit rehearsal, S3-direct data plane |
| Persistent catalog | `ICEBERG_REST_URI=http://localhost:8181 ./etl/run.sh` | dbt models materialize as Iceberg tables with snapshot history |
| **The complex batch** | `ICEBERG_REST_URI=… ./etl/ingest/run.sh` | 4-file parallel fan-out, 3 routes, header aliasing, quarantine, canonical-model tests, cross-route consolidation |
| Remote-Spark opt-out | `SPARK_CONNECT_URI="" ./etl/run.sh` | in-process fallback on the isolated heavy queue |
| Namespace isolation | `TEMPORAL_NAMESPACE=team-data ./etl/run.sh` | the data team's pipeline in its own tenancy boundary |
