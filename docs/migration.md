# Migration playbook — onboarding real pipelines onto the platform

For migrating existing ETL (Airflow/MWAA, cron scripts, vendor drops) onto
this architecture. Work one pipeline at a time; each is independent.

## Concept mapping (Airflow → this platform)

| Airflow | Here |
|---|---|
| DAG file | a workflow function (real code: loops, try/except, awaits) |
| Task / operator | an activity |
| Sensor / poke | a heartbeating activity in a poll loop (`submit_emr_job` pattern) |
| XCom | plain return values flowing between activities |
| K8sExecutor / pools | queues + worker profiles (docs/workers.md) |
| `schedule_interval` in code | a Temporal Schedule — server-side data (`ingest/schedule.py` pattern) |
| Backfill | `Schedule.backfill` API over a past range |
| DAG bucket (S3/GCS sync) | none — code ships in worker images (docs/workers.md) |
| Retries per task | server-enforced RetryPolicy + retryable/non-retryable taxonomy |

## Per-pipeline checklist

1. **Inventory the legacy pipeline**: sources (SFTP? push? API?), file
   formats and real header variants observed, target schema, schedule,
   SLAs, downstream consumers (tables vs files), failure handling today.
2. **Define the canonical entity** in
   `etl/dbt/models/staging/schema.yml`: grain, columns with `data_type`,
   cleanup `meta.expr`, `data_tests`. This single block generates the
   staging SQL, the landing gate, and the enforcement.
3. **Write the route spec** (`etl/specs/<name>.json`): `source_table`,
   `dbt_args` selector (`tag:<route>`), `column_aliases` for every header
   variant the vendor has ever shipped, `outputs`.
4. **Register the route**: filename pattern → route → spec in
   `etl/specs/registry.json`. If files arrive compressed, nothing extra —
   `.gz` preprocess is automatic. Other byte-shaped needs (PGP, zip
   fan-out) follow the `preprocess_file` pattern under the four-test
   policy.
5. **Write marts by hand** (business logic is never generated), tagged for
   the route or `tag:consolidated` for cross-route joins (requires the
   persistent catalog).
6. **Run the batch locally** and iterate until the PASS lines:
   `ICEBERG_REST_URI=http://localhost:8181 ./etl/ingest/run.sh` (seed real
   sample files onto the SFTP container — including a known-bad one; the
   quarantine path is part of the acceptance test).
7. **Namespace + fleets**: pick/create the team namespace (retention!),
   deploy fleets as queue+profile+code (docs/workers.md); the ETL fleets
   are shared unless the pipeline needs its own isolation.
8. **Schedule it**: clone `ingest/schedule.py` semantics — interval,
   `overlap=SKIP`, created paused; `trigger` once to validate; `unpause`.
9. **Wire observability**: BatchId/Route/SourceFile land automatically;
   save the UI queries; alarm on backlog age exists per fleet (CDK).
10. **Prod**: follow docs/prod-runbook.md; the pipeline itself needs zero
    code changes — bindings only.

## Migration-specific advice

- Migrate **read paths last**: run the new pipeline in parallel writing to
  its own curated prefixes/tables, diff outputs against legacy for a few
  cycles (the inline-JSON outputs make assertions cheap), then cut
  consumers over.
- Vendors' historical files are the alias-map goldmine: run a one-off
  sweep of old headers through `sanitize_header` to pre-populate
  `column_aliases` before the first live file surprises you.
- Resist per-pipeline "just this once" engines or bespoke workers — the
  platform's value is uniformity (see docs/decisions.md D3, D9).
