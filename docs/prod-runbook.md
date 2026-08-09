# Prod runbook — from this repo to a running AWS platform

Ordered phases. Every phase ends with a validation gate. The architecture
requires **no code changes** on this path — bindings only (docs/decisions.md
D2). Items marked **[BUILD]** are known gaps: designed and documented here
but not yet implemented in this repo.

## Phase 0 — accounts & ground rules

- One AWS account per environment (quotas, blast radius, IAM are
  account-scoped — docs/workers.md cluster topology). GitHub OIDC deployer
  role per account (no long-lived keys; the ci workflow carries the
  commented example).
- Size the VPC for worker scale: every Fargate task consumes a subnet IP.

## Phase 1 — Temporal service (CDK)

```sh
cd infra && npx cdk deploy --all \
  [-c vpcId=… -c ecsClusterName=… -c dbEndpoint=… -c dbSecretArn=… ]   # import-or-create
  [-c domainName=… -c hostedZoneId=… -c zoneName=…]                    # optional DNS
```
Deploys: internal NLB (:7233 gRPC, :7243 HTTP API), Temporal server on ECS
Fargate + Aurora Serverless v2 (creds in Secrets Manager), UI behind ALB.
Recommended: a dedicated *platform* ECS cluster here; a shared *workload*
cluster for fleets.
- **[BUILD]** visibility: local prod-mimic uses Elasticsearch; the CDK
  stack currently deploys SQL visibility only. Add managed OpenSearch +
  advanced visibility config for `STARTS_WITH` queries + search attributes
  at scale.
- **[BUILD]** the CDK deploys the auto-setup single-container topology;
  for real prod, split services (the compose prod-mimic shows the shape).
- Gate: `temporal operator cluster health` via the NLB = SERVING; one-shot
  namespace+search-attribute setup applied (mirror
  docker-compose.prod.yml's namespace-setup as an ECS run-task or CDK
  custom resource — **[BUILD]**).

## Phase 2 — data plane

- S3 bucket(s) with the zone layout (landing/ curated/ quarantine/ jobs/
  warehouse/) + lifecycle rules on quarantine/ and jobs/.
- **EMR Serverless application**: batch job runs + interactive enabled
  (emr-7.13+) with pre-initialized capacity; **Glue Data Catalog enabled on
  the application** (the server-side catalog — jobs only select it).
- Specs flip two fields: `catalog: {"type": "glue"}`, `spark_remote:
  <session endpoint>`; drop `AWS_ENDPOINT_URL`. On real AWS the *batch*
  path actually executes `spark_job.py` — the local
  run-the-transform-ourselves activity is not deployed (D4).
- **[BUILD]** session lifecycle activity: create/reuse the EMR interactive
  session and feed its endpoint to jobs (locally the endpoint is static).
- Gate: run one `DbtSparkJob` via `start_job_run` against a test bucket;
  confirm Glue tables + curated output.

## Phase 3 — file delivery

- **AWS Transfer Family** SFTP server → landing/ directly; S3 event →
  EventBridge → start `FileIngestWorkflow` (the worker then touches no
  bytes at all). **[BUILD]** as a CDK construct + a small starter Lambda —
  until then the SFTP-pull activity works against any reachable SFTP.
- Gate: drop a file with a vendor account; batch completes; quarantine
  path verified with a junk file.

## Phase 4 — worker fleets

- Build/push team images (ci builds them already; uncomment the ECR push).
  One `TemporalWorkerService` per fleet: image + worker_platform command +
  profile + `autoscaling` (backlog poller λ → CloudWatch → step scaling;
  BacklogAgeAlarm included). `allowGrpcTo(cluster.serverService)`.
- No heavy fleet in prod (D4). Task roles carry each fleet's AWS
  permissions (S3 prefixes, EMR submit).
- Gate: fleets visible on the task-queue page; scale-out observed under a
  seeded backlog; alarm fires when fleets are stopped.

## Phase 5 — identity & enforcement (the switch list)

| Switch | Where | State in repo |
|---|---|---|
| UI OIDC → corporate IdP | 5 env vars on the UI task (Dex stubs in docker-compose.prod.yml) | seam validated e2e locally via Dex |
| gRPC authz: JWT authorizer + claim mapper → per-namespace roles | temporal server config (stubs present) | **[BUILD]** — turn on + map IdP groups |
| mTLS (service↔service, SDK→frontend) | server + worker TLS config | **[BUILD]** |
| Payload codec / encryption for sensitive namespaces | codec server + SDK data converter | **[BUILD]** |
| Secrets | already Secrets Manager (DB); vendor SFTP/PGP keys likewise | pattern set |

- Gate: unauthenticated gRPC rejected; a team's token cannot touch another
  namespace.

## Phase 6 — operations

- Schedules created per pipeline (paused → trigger once → unpause).
- Dashboards: server metrics (Prometheus locally ⇄ CloudWatch/AMP),
  backlog + age per queue, schedule-to-start latency. Alarm exists per
  fleet; add paging targets (SNS) — **[BUILD]**.
- Worker Versioning (build IDs) once in-flight workflow logic starts
  changing — **[BUILD]**.
- Nightly restore test of Aurora snapshots; `down -v && up` clean-slate
  drill is already proven locally.

## Honest status

Everything above the [BUILD] markers is implemented and validated against
local bindings (including CI e2e on fresh runners). Nothing here has been
executed against a real AWS account yet — the first real deploy should
walk these phases in order and treat each gate as blocking.
