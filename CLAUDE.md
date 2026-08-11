# CLAUDE.md — operator manual for agents working in this repo

This repo is a **reference implementation of a business-wide Temporal
platform**: Temporal as a service (local prod-topology stack + CDK for AWS),
a generic worker platform (size/shape profiles, queue taxonomy), and a full
real-world ETL (SFTP ingestion → spec-driven dbt-Spark transforms → Iceberg
catalog). It is meant to be *copied from*: real workloads get migrated onto
this architecture (see docs/migration.md).

## Read these before changing anything

- **docs/decisions.md** — every architectural decision with its why.
  Do not re-litigate these silently; if one must change, update the record.
- **docs/gotchas.md** — symptom → cause table of every wall already hit.
  Check it FIRST when something fails weirdly; most failures here recur.
- docs/README.md — index; docs/etl.md — the pipeline in depth;
  docs/workers.md — the worker platform; docs/prod-runbook.md — path to AWS.

## Non-negotiable invariants

1. **The Temporal server never runs user code.** Workers embed and register
   implementations; starters send names; schedules are server-side data.
2. **One query engine.** All SQL is Spark through the single dbt project.
   Never introduce a second engine (no DuckDB/pandas transforms).
3. **Data through workers only under the four-test policy** (byte-shaped,
   bounded, streamed, profiled — docs/workers.md). SQL-shaped or unbounded
   ⇒ the cluster, via s3a/S3 directly. Bulk data never transits a worker.
4. **The canonical model drives normalization.** New fields/entities go in
   `etl/dbt/models/staging/schema.yml` — the staging SQL, landing gate, and
   tests are derived from it. Per-vendor naming goes in spec
   `column_aliases`, nothing else.
5. **Fleets = queue + profile + registered code** via
   `python -m worker_platform`. Workflow workers never register activities.
   Blocking (sync) activities only — never blocking calls in `async def`.
6. **Local defaults ARE the prod shape** (light workers + external Spark +
   server-side catalog). The in-process fallback (`SPARK_CONNECT_URI=""`)
   is a dev convenience, not a deployment rung.
7. **Deterministic failures are non-retryable and quarantine; transient
   failures retry bounded.** Never unbounded retries; never fail a batch
   for one bad file.
8. **Validate by running.** Nothing in this repo is claimed without an
   executed run asserting it. Keep that bar: after changes, run the
   relevant suite below and check the PASS lines.

## Validation loop (run before claiming anything works)

```sh
./etl/.venv/bin/python -m pytest etl/tests -q       # 19 unit tests
cd infra && npx jest && npx cdk synth --quiet       # 17 CDK tests (env: CDK_DEFAULT_ACCOUNT=111111111111 CDK_DEFAULT_REGION=us-east-1)
./etl/run.sh                                        # single transform e2e -> "ETL PIPELINE: PASS"
ICEBERG_REST_URI=http://localhost:8181 ./etl/ingest/run.sh  # complex batch -> "INGEST PIPELINE: PASS" + "CONSOLIDATION: PASS"
./examples/platform-demo/run.sh                     # second tenant -> "PLATFORM DEMO: PASS"
ICEBERG_REST_URI=http://localhost:8181 ./examples/order-settlement/run.sh  # lifecycle tenant -> "ORDER SETTLEMENT: PASS"
nix run .#examples                                  # 4 SDK examples
```

Dependencies the e2e paths expect: Temporal on :7233 (`nix run .#up` or
`.#prod-up`), LocalEmu on :4566 (`localemu start`, needs Python ≥3.13),
Iceberg REST on :8181 (`nix run .#catalog-up`); Spark Connect and SFTP
containers auto-start from the run scripts. CI mirrors all of this
(.github/workflows: `ci` every push, `e2e` on pipeline paths).

## Environment contract (all clients honor these)

`TEMPORAL_ADDRESS` (default localhost:7233) · `TEMPORAL_NAMESPACE` (default
default; teams: team-app, team-data) · `SPARK_CONNECT_URI` (default
sc://localhost:15002; empty = in-process fallback) · `ICEBERG_REST_URI`
(unset = ephemeral catalog) · `AWS_ENDPOINT_URL` (unset = real AWS) + the
standard AWS credential variables.

## Extension recipes (the intended change surface)

| To add… | Touch |
|---|---|
| a vendor field-name variant | 1 alias line in that route's `etl/specs/*.json` |
| a field on an entity | 1 block in `etl/dbt/models/staging/schema.yml` |
| a new file type/route | spec file + registry line + schema.yml entity (+ mart) |
| a new business metric | hand-written mart model (never generate marts) |
| a new team/workload | own image (see `examples/platform-demo/`), own queues `{domain}-{concern}`, fleets via worker_platform, own namespace |
| a human-gated lifecycle workload | signals + `wait_condition` + SLA timer + `Stage` upserts — copy `examples/order-settlement/` (docs/order-settlement.md; system map: docs/flows.md) |
| a new worker size class | only when a *class* of fleets overrides the same way — extend both matrices (`worker_platform/profiles.py` + CDK `WORKER_PROFILE_SIZES`) |
| preprocessing (decrypt/unzip) | byte-shaped activity like `preprocess_file`, under the four-test policy |

## Conventions

- Nix for system deps (flake dev shell + `nix run .#<app>` task runner —
  never ad-hoc host installs). Commit messages explain *why* and record
  what was validated. Docs are updated in the same commit as the change
  they describe. GitHub: single-branch on `main`.
