# Decision log

The *why* behind the architecture, in the order the decisions were earned.
Change a decision → update its record in the same commit.

**D1 — Temporal orchestrates; the server never runs user code.** Workers
embed + register implementations; starters/schedules send names + args.
There is no DAG-bucket equivalent (contrast MWAA/Composer, whose scheduler
must parse your files). Consequence: deploying code = shipping worker
images; deploy-order independence; queues buffer through downtime.

**D2 — Local ≙ prod via bindings, with real components over mocks.** Every
concern has an invariant contract (URI, config field, zone) with a local
and a prod binding: SFTP container ⇄ Transfer Family, Samba container ⇄
FSx/on-prem SMB share (Transfer speaks no SMB — that source keeps the
policy-compliant worker-streamed landing even in prod), LocalEmu ⇄ S3,
Spark Connect container ⇄ EMR Serverless session, Iceberg REST ⇄ Glue,
Dex ⇄ corporate IdP, compose prod-mimic ⇄ CDK ECS/NLB/Aurora. Prefer a
real engine locally (an actual Spark server) over control-plane mocks.
Emulator limits are documented honestly (LocalEmu runs EMR's API, not its
compute; MiniStack's CFN lacks types — scripts/validate-emulator.sh).

**D3 — One query engine.** All SQL is Spark through one dbt project.
DuckDB-per-worker was considered for small files and rejected: dual
engines mean dialect drift, divergent materialization semantics, a doubled
test matrix, and split catalog integration. A *warm* Spark service
amortizes the per-job overhead that motivated the second engine.

**D4 — Launcher pattern by default; heavy fleets only when the activity IS
the compute.** Workers submit-and-poll external compute (EMR batch) or act
as thin SQL clients (Spark Connect session). `spark_remote` is the local
default so local = prod. The `compute-large` lane + in-process fallback
exist as (a) offline dev path, (b) the template for app-native compute
(transcode/ML/render) that has no managed service.

**D5 — Data through workers under the four-test policy.** The early
absolute ("workers never touch data") was re-derived once the platform had
real admission control: byte-shaped ∧ bounded ∧ streamed ∧ profiled ⇒
allowed (SFTP stream, gunzip preprocess). Query-shaped or unbounded ⇒
cluster. See docs/workers.md.

**D6 — The canonical data model drives normalization.** One entity per
route in `schema.yml`: staging SQL is *generated* from it
(`build_staging`: data_type→cast, meta.expr→cleanup), the ingest landing
gate is *derived* from it, its data_tests are *enforced* every build
(violation ⇒ child fails non-retryably ⇒ file quarantines). Marts stay
hand-written — aggregations are business logic; generating them is where
mini-frameworks go wrong.

**D7 — Per-vendor knowledge lives only in specs.** `column_aliases` maps
name variants onto canonical names (mechanical sanitization already
collapses case/punctuation). Routing is filename-pattern (registry), never
content sniffing — routing by content would force workers to open files.

**D8 — Child workflow per file.** Isolation, per-file lineage
(`transform-<route>-<file>` + BatchId/Route/SourceFile search attributes),
independent retry/reset, and per-route queue targeting. The documented
production pattern for per-item pipelines.

**D9 — Profiles are templates, not shared pools.** A worker runs the code
its image contains, so a company-wide shared "large pool" would be one
image with every team's deps. Instead: 5 sizes × 3 shapes
(`worker_platform/profiles.py` ⇄ CDK `WORKER_PROFILE_SIZES`, same names);
teams instantiate profiles with their own images. `-mem` halves admission
(each task holds real memory); `-cpu` is box-only. Extend the matrix only
when a class of fleets overrides the same way.

**D10 — Fleet split + sync-on-threadpool.** Workflow workers never
register activities (replay must never queue behind I/O); blocking calls
are sync `def` on thread pools, never inside `async def` (the Python
event-loop footgun). Heavy admission is resource-tuned with hard bounds +
a server-enforced per-queue rate cap.

**D11 — Failure taxonomy.** Deterministic (contract violation, dbt error,
bad gzip) ⇒ `ApplicationError(non_retryable)` ⇒ quarantine, batch
continues. Transient (crash, OOM, lost connection) ⇒ bounded retry.
Duplicates (stale twin of a renamed drop) ⇒ recorded, not fatal.
Heartbeats are wall-clock, never output-driven; cancelled transforms kill
their subprocess so a retry can never race a zombie.

**D12 — Iceberg catalog, configured server-side.** Table metadata persists
in a catalog (REST locally, Glue on AWS) defined on the Spark
service/application — the Glue-on-EMR-app analog. Job specs only *select*
it. Cross-job joins (consolidation) exist because of this.

**D13 — Temporal Schedules over legacy cron.** Server-side data object:
spec + action-as-names + policies. overlap=SKIP for batches; catchup
window covers cluster downtime; backfill is first-class. Cadence changes
are API calls, never code deploys.

**D14 — Namespaces are the tenancy boundary.** team-app/team-data with
their own retention; the future RBAC boundary (JWT claims → per-namespace
roles). Search attributes carry no PII.

**D15 — ECS topology: platform cluster + one shared workload cluster.**
On Fargate a cluster is a namespace, not a capacity pool. Real scale walls
are per-account quotas and subnet IPs (one per task). Environments —
ideally accounts — are the sacred boundary.

**D16 — CI split.** `ci` (unit + CDK tests + synth + image builds,
unpushed) on every push; `e2e` (the full platform on a runner, real
pipelines asserted) on pipeline-path pushes + dispatch. Single-branch on
main, change-driven only — no cron CI.

**D17 — Readable physical names where safe; tags everywhere.** CDK's
default names (`TemporalStack-ci-E2eWorkerServiceB1CFC9EB-WqL1…`) exist
for good reasons — generated names never collide across parallel deploys
and let CloudFormation replace-on-update. We keep those properties while
restoring legibility: physical names are set explicitly *prefixed with
the stack name* (ECS cluster = stack name; services/task families =
`<stack>-temporal-server`, `<stack>-<WorkerId>`; log groups =
`/ecs/<stack>/<component>`), so the `-ci` suffix isolation still holds.
Load balancers keep generated names (32-char limit makes prefixing
fragile). Everything taggable carries `project` + `deployment` tags, ECS
services propagate tags to running tasks, and CloudFormation's own
`aws:cloudformation:stack-name` tag covers the rest — console filtering
by tag beats name archaeology. Trade-off owned: renaming a named resource
forces replacement, acceptable for an example platform; teams pinning
prod stacks should treat physical names as frozen once deployed.

**D18 — Version currency is validated, not assumed.** Dependency floors
float to the latest within a major (`>=x,<next-major`); images and engine
versions are explicit pins bumped by running the audit (registry/PyPI/
`aws rds describe-db-engine-versions`) and re-running the validation
loop. Current: Temporal 1.29.7/UI 2.53.1, Spark 4.1.2 + Iceberg 1.11.0,
ES 8.19.19 (`ES_VERSION=v8`), Aurora PG 17.10, EMR Serverless 7.9.0,
dbt-core 1.12 + dbt-spark 1.11. The Rust dbt engine (Fusion) was
evaluated: its Spark adapter is beta, Thrift/Livy-only, Spark 3-only —
incompatible with our session/Connect + Spark 4 architecture; revisit at
GA. EMR's custom image installs Python 3.11 (EMR bundles EOL 3.9, which
would silently resolve an old dbt).
