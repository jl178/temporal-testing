# temporal-testing

End-to-end [Temporal](https://temporal.io) example:

- **Local runtime** — full Temporal cluster (server, Postgres, web UI, admin tools) via Docker Compose.
- **AWS infrastructure** — CDK stacks with import-or-create abstractions: ECS Fargate + Aurora Serverless v2, internal NLB for gRPC, ALB for the UI, optional Route 53 DNS. Every major dependency (VPC, ECS cluster, database, hosted zone) can be **passed in if it already exists, or is created if not**.
- **Emulator validation** — the stacks deploy against a FOSS local AWS emulator (LocalStack was archived/paywalled in March 2026; this repo uses its successors).
- **Example workflows** — the same greeting workflow (workflow + activity + worker + starter) in **Python, Go, TypeScript, and C#**, each validated end-to-end against the local cluster.

## Prerequisites

- Docker (with Compose v2)
- [Nix](https://nixos.org) with flakes — provides the entire toolchain (Node 20, Go, Python 3.12, .NET 8, Temporal CLI, AWS CLI):

```sh
nix develop          # dev shell with all tools
direnv allow         # or use direnv (.envrc included)
```

Without Nix, install manually: Node 20+, Go 1.24+, Python 3.12+, .NET 8 SDK, [Temporal CLI](https://docs.temporal.io/cli), AWS CLI v2.

## Quickstart

```sh
nix run .#up          # start Temporal (server :7233, UI http://localhost:8080)
nix run .#examples    # run all 4 language examples e2e (starts the cluster if needed)
nix run .#down        # stop everything
```

## Prod-mimicking topology

`docker-compose.yml` is the all-in-one dev stack (`auto-setup` runs all four Temporal services in one container). `docker-compose.prod.yml` mimics how production actually looks:

- **history / matching / frontend ×2 / worker** each in their own container (`temporalio/server`, with `auto-setup` doing one-time schema setup on the history node)
- **nginx** load-balancing gRPC across the two frontends on :7233 (the role an NLB plays in AWS)
- **Elasticsearch** advanced visibility alongside Postgres persistence
- **Prometheus** (:9090) scraping every service + **Grafana** (:8085, anonymous admin)
- a **long-lived containerized app worker** (`examples/python` image) polling `greeting-tasks-python` — the prod pattern where workers are deployed services, not hand-started processes

```sh
nix run .#down && nix run .#prod-up   # ports 7233/8080 overlap with the dev stack
cd examples/python && ./.venv/bin/python starter.py   # no local worker needed —
                                                      # the containerized worker picks it up
nix run .#prod-down
```

Versions are pinned to the newest images with a full set available: server/auto-setup **1.29.1** (auto-setup lags server releases; 1.31.x has no auto-setup image yet) and UI **2.53.1**.

**Multi-team usage:** the prod stack creates per-team **namespaces** — `team-app` (72h retention) and `team-data` (168h, for the ETL pipeline) — via a one-shot `temporal-namespace-setup` container; the containerized worker is pinned to `team-app`. Namespaces are Temporal's tenancy boundary: per-team retention, search attributes, rate limits, and (once auth is on) per-namespace RBAC from JWT claims. Every example and the ETL pipeline honors `TEMPORAL_NAMESPACE` (default `default`):

```sh
TEMPORAL_NAMESPACE=team-app  python examples/python/starter.py   # handled by the containerized worker
TEMPORAL_NAMESPACE=team-data ./etl/run.sh                        # data team's pipeline in its own namespace
```

**Auth:** both compose files run unauthenticated (normal for local dev). For OAuth/OIDC later: the server takes a JWT authorizer + claim mapper pointed at your IdP's JWKS (`TEMPORAL_AUTH_AUTHORIZER=default`, `TEMPORAL_JWT_KEY_SOURCE1=...`), and the UI has built-in OIDC login (`TEMPORAL_AUTH_ENABLED`, `TEMPORAL_AUTH_PROVIDER_URL`, client id/secret). Commented stubs for both are in `docker-compose.prod.yml`; on AWS the same env vars go on the ECS task definitions with Cognito/Okta/Auth0 as the IdP.

Each example's `run.sh` is self-contained: installs dependencies, starts a worker, executes a workflow on its own task queue, and asserts the result (`Hello, Temporal!`). Individual runs:

```sh
./examples/python/run.sh
./examples/go/run.sh
./examples/typescript/run.sh
./examples/csharp/run.sh
```

Point any of them at another cluster with `TEMPORAL_ADDRESS=host:7233`.

## AWS deployment (CDK)

```
infra/
├── bin/app.ts                     # context-driven wiring
├── lib/constructs/
│   ├── temporal-network.ts        # VPC       — import or create
│   ├── temporal-database.ts       # Postgres  — import, or create Aurora Serverless v2
│   ├── temporal-cluster.ts        # ECS cluster — import or create; server + UI Fargate services
│   ├── temporal-dns.ts            # hosted zone — import or create; ui./grpc. alias records
│   └── temporal-worker.ts         # optional Fargate worker service construct
└── lib/stacks/                    # TemporalNetworkStack, TemporalStack
```

**Architecture (create-everything mode):** 2-AZ VPC → Aurora Serverless v2 PostgreSQL (credentials in Secrets Manager) → `temporalio/auto-setup` on Fargate (all four Temporal services in one container; creates/migrates schema on boot) registered in Cloud Map (`temporal-frontend.temporal.local`) and fronted by an **internal NLB** on :7233 → `temporalio/ui` on Fargate behind an **ALB**. `auto-setup` is a dev/example topology — production deployments should run the four Temporal services separately.

Everything that "might already exist" is a context flag — provided values are imported, omitted ones are created:

```sh
npx cdk deploy --all                                   # create everything
npx cdk deploy --all \
  -c vpcId=vpc-0123456789 \                            # reuse a VPC
  -c ecsClusterName=my-cluster \                       # reuse an ECS cluster
  -c dbEndpoint=mydb.cluster-x.us-east-1.rds.amazonaws.com \
  -c dbSecretArn=arn:aws:secretsmanager:...:secret:x \ # reuse a Postgres + creds
  -c dbSecurityGroupId=sg-0123 \                       # let the stack open DB ingress
  -c domainName=temporal.example.com \                 # enable DNS records
  -c hostedZoneId=Z... -c zoneName=example.com \       # reuse a hosted zone
  -c publicUi=false \                                  # keep the UI ALB internal
  -c serviceDiscovery=false \                          # skip Cloud Map, use the NLB
  -c natGateways=0                                     # no NAT (emulators/cost)
```

Validate without an AWS account:

```sh
nix run .#infra-test    # 13 assertion tests: create-everything / import-existing / DNS on-off / no-cloud-map
nix run .#synth         # clean cdk synth of both stacks
```

## Emulator validation

LocalStack archived its repos and paywalled the Community edition in March 2026. Two FOSS successors were evaluated:

| Emulator | Result |
|---|---|
| **[LocalEmu](https://github.com/localemu/localemu)** (Apache-2.0 LocalStack fork) | ✅ Both stacks deploy fully via CloudFormation (53 resources incl. Aurora, ECS services, ELBs, Cloud Map, IAM). Control-plane emulation: resources exist in CFN state but don't run. |
| **[MiniStack](https://github.com/ministackorg/ministack)** (MIT) | ⚠️ Network stack deploys; direct RDS/ECS APIs run **real Docker containers** (validated a live Postgres via the RDS API), but its CFN engine lacks `AWS::RDS::DBSubnetGroup`, `AWS::ServiceDiscovery::PrivateDnsNamespace`, `AWS::EC2::SecurityGroupIngress`. |

```sh
pip install localemu && localemu start    # gateway on :4566
nix run .#validate-emulator               # synth + deploy both stacks + report
```

The script deploys the synthesized templates with the plain CloudFormation API (no `cdk bootstrap` — emulator CFN engines mishandle the toolkit stack; the bootstrap-version SSM parameter is seeded directly). Real *runtime* validation is the Docker Compose path above — the emulators only prove out the infrastructure templates.

## Batch ETL pipeline (Temporal + EMR Serverless + dbt-Spark)

`etl/` is a Temporal-orchestrated, **generic** dbt-Spark pipeline. A `DbtSparkJob` spec describes any workload — S3 inputs to land as Spark tables, a dbt project + CLI args/vars, outputs to export — and `spark_job.py` is a generic runner that executes any such spec (the orders → `daily_revenue` demo is just the default instance). The workflow:

1. **Extract** — activity seeds raw orders CSV into S3 (demo step; real sources land their own).
2. **Submit** — activity packages the runner + dbt project tarball + spec to S3, creates/starts an EMR Serverless application, starts a job run pointing at them, and polls to a terminal state (with activity heartbeats).
3. **Transform** — activity runs the *identical spec* through `spark_job.py` locally: real Spark, **dbt** (`dbt-spark` `session` method) building `stg_orders` → `daily_revenue`, outputs exported to S3.
4. **Validate** — activity checks every declared output landed and is non-empty; the starter asserts row counts and totals.

```sh
pip install localemu && localemu start    # emulator on :4566 (S3 + EMR Serverless)
nix run .#up                              # Temporal cluster
./etl/run.sh                              # worker + workflow; prints ETL PIPELINE: PASS
```

**Table metadata / catalog** — the spec's optional `catalog` section picks between two modes:

- *Default (no catalog)*: Spark's ephemeral in-memory catalog. Each run is self-contained; table metadata dies with the job; the only durable artifacts are the declared S3 outputs.
- *Persistent catalog*: dbt models materialize as **Iceberg** tables in an external catalog — locally an [Iceberg REST catalog](https://github.com/tabular-io/iceberg-rest-image) container (the Glue Data Catalog stand-in), on real AWS `{"type": "glue"}` for the actual Glue Data Catalog. Tables persist across runs with snapshot history and are queryable by name from other engines (Athena/Trino on AWS).

```sh
nix run .#catalog-up                          # Iceberg REST catalog on :8181, warehouse in s3://etl-data/warehouse
ICEBERG_REST_URI=http://localhost:8181 ./etl/run.sh
curl -s localhost:8181/v1/namespaces          # -> raw, analytics (persisted table metadata)
```

Honest scoping: FOSS emulators only implement the EMR Serverless **control plane** — job runs transition to `SUCCESS` but execute nothing — so the pipeline exercises the real submit/poll orchestration against the emulator while the identical transformation runs as local Spark compute. Pointed at real AWS (unset `AWS_ENDPOINT_URL`, real role ARN), the same submission path actually executes `spark_job.py` on EMR Serverless.

## File-ingestion pipeline (SFTP → S3 → parse → dispatch)

`etl/ingest/` chains a file-delivery front end onto the transform pipeline:

```
SFTP ──▶ s3://etl-data/landing/ ──▶ s3://etl-data/staged/ ──▶ s3://etl-data/curated/
     land            rules-based parse         │  dbt-Spark child workflow
                                          classify ──▶ registry ──▶ transform-spec-1
```

`FileIngestWorkflow` (task queue `file-ingest`) runs, per discovered file:

1. **`land_sftp_file`** — streams the file from SFTP into the landing zone (heartbeats make big transfers crash-resumable; a per-file workflow ID doubles as the exactly-once guard).
2. **`parse_file`** — **file hygiene only**: permissive read (every column lands as a string — typing is semantic), mechanical header sanitization (`"Order ID"` → `order_id`), parquet out. Structurally broken files (bad encoding, unparseable CSV) go to `quarantine/` and are recorded per-file without failing the batch. Everything *semantic* — renames, casts, trims, dedupes — deliberately does **not** live here: it belongs to the route's **dbt staging models** (`stg_orders` does the `cast`/`trim`/`lower`), where transformations are versioned, tested, and documented. "Given rules/macros" = the route's dbt project; Jinja is the macro system.
3. **`classify_file`** — reads the routing field (`record_type`) from the staged data.
4. **`resolve_transform_spec`** — registry lookup: `etl/specs/registry.json` maps the route value to a spec file (`transform-spec-1.json`), which becomes a `DbtSparkJob` with the staged file wired in as its input. **Adding a new file type = adding a spec file + one registry line.**
5. **Child workflow dispatch** — the job runs as a child `EtlPipelineWorkflow` on the `etl-pipeline` queue (its own workflow ID `transform-<route>-<file>` for lineage, its own retries, optionally its own worker fleet), writing the curated output to S3.

```sh
./etl/ingest/run.sh      # seeds a vendor file over SFTP, runs the full chain,
                         # prints INGEST PIPELINE: PASS
```

The SFTP test server (`atmoz/sftp`, `demo`/`demo` on :2222) is part of the dev compose config — it comes up with `nix run .#up`, is auto-started by `run.sh` if missing, and can be managed alone with `nix run .#sftp-up` / `.#sftp-down`.

### The complex batch

`run.sh` seeds a realistic multi-source batch: three routed files with messy vendor headers (**orders**, **customers**, **payments** — each `record_type` maps to a different spec via the registry, and each spec builds only its dbt models via `--select tag:<route>`) plus one structurally broken file. The parent workflow **fans out in parallel** (`asyncio.gather` over per-file child workflows):

```
file-ingest-<id>
├── transform-orders-orders_2026-08.csv        -> analytics.daily_revenue
├── transform-customers-customers_2026-08.csv  -> analytics.dim_customers
├── transform-payments-payments_2026-08.csv    -> analytics.fct_payments
├── zz_broken.csv                              -> quarantine/ (batch continues)
└── consolidate-consolidation-<id>             -> analytics.executive_summary
```

The final **consolidation** child job runs after the fan-out completes and joins all three routes' tables (`dbt build --select tag:consolidated`) into `executive_summary` — revenue vs. collected payments per customer segment. This cross-job join is exactly what the persistent catalog is for, so consolidation only runs in catalog mode:

```sh
nix run .#catalog-up
ICEBERG_REST_URI=http://localhost:8181 ./etl/ingest/run.sh
# ...
# CONSOLIDATION: PASS (executive_summary joined 3 routes)
# INGEST PIPELINE: PASS
```

Without a catalog the same batch still runs (3 parallel transforms + 1 quarantine); the consolidation step is skipped because cross-job tables don't outlive their jobs.

### Following a batch in the UI

The workflow list is a query engine, not a tree — the production practice is to navigate with filters, not scroll. Every pipeline workflow is stamped with **custom search attributes** (`BatchId`, `Route`, `SourceFile` — registered idempotently by `run.sh` and the prod `temporal-namespace-setup` container), so in the UI search box (or `temporal workflow list --query`):

```
WorkflowType = "FileIngestWorkflow"        -- one row per batch (your landing view)
BatchId = "file-ingest-<id>"               -- one batch's entire tree
Route = "payments"                          -- one route across all batches
SourceFile = "orders_2026-08.csv"           -- everything spawned by a file
RootWorkflowId = "file-ingest-<id>"        -- built-in equivalent of BatchId
```

From any workflow's detail page, the **Relationships** tab shows its parent and children (clickable both ways), and the History tab's **Timeline** view shows the parallel fan-out as overlapping activity lanes. Search-attribute values can also be added as columns on the list page.

The parent and child workflows show up separately in the UI (`file-ingest-*` → `transform-orders-*`), which is the lineage story: per-file, per-route history, independently retryable.

## Repository layout

```
docker-compose.yml     # postgres:16 + temporalio/auto-setup + temporalio/ui + admin-tools
dynamicconfig/         # Temporal dynamic config mounted into the server
examples/{python,go,typescript,csharp}/
etl/                   # Temporal-orchestrated batch ETL: EMR Serverless + dbt-Spark
etl/ingest/            # SFTP -> landing -> parse -> classify -> dispatch (parent workflow)
etl/specs/             # transform-spec registry (route value -> DbtSparkJob template)
infra/                 # CDK v2 app (TypeScript) + jest assertion tests
scripts/               # validate-local.sh, validate-emulator.sh
flake.nix              # dev shell + task-runner apps (nix run .#<up|down|examples|infra-test|synth|validate-emulator|validate>)
```
