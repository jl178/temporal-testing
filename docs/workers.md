# The worker platform — size profiles for the whole business

Temporal workers are the platform's compute-attachment mechanism: a worker
is a deployment that polls a **queue** with a **size profile** and registers
**code**. Those are the only three decisions any team makes:

```sh
python -m worker_platform --queue <queue> --profile <size[-cpu|-mem]> \
    --workflows <module[:Class,…]> --activities <module[:fn,…]>
```

Registration specs auto-discover every `@workflow.defn` / `@activity.defn`
in a module, or name them explicitly. Connection comes from
`TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE`.

## Who runs this command, and for how long

Nobody types it — it is a **service definition**. Locally the `run.sh`
scripts spawn the processes so demos are self-contained; in a real
deployment the line is the container CMD of a long-lived service: an ECS
service (the CDK `TemporalWorkerService` construct — one service per fleet,
autoscaled on task-queue backlog), a k8s Deployment, or systemd. The prod
compose stack shows the pattern: `temporal-app-worker-python` is exactly
such a container, running continuously.

Lifecycle semantics:

- **Serves all future executions** — a worker is not per-job; it polls its
  queue forever and picks up a workflow started next month just the same.
- **New activity/workflow types are a deploy** — the server stores no code;
  a worker executes only what it registered at startup. Ship a new image
  with the new registrations and restart the fleet. No cluster-side change.
- **Downtime loses nothing** — with every worker on a queue down, tasks
  accumulate in the queue and drain when one returns. That backlog is also
  the autoscaling signal.
- **Scaling = replicas** — more capacity is more instances of the same
  command; the pull model spreads tasks across whoever has free slots.

## How code registers — there is no DAG bucket

Coming from MWAA/Composer: there, a scheduler must *parse your DAG files*
to know the graph, so code is distributed to it (S3/GCS sync). Temporal's
server never parses or executes user code — so **there is nothing to
upload or mount**. Workflows and activities live inside each fleet's
container image like any application code; at startup the runner imports
the registered modules into an in-memory table (type name → class/function)
that lives and dies with the process.

The server only ever handles **strings**: a starter sends a workflow type
name + args; the server queues a task; a worker looks the name up in its
own table. No validation that anything implements the type — which buys
deploy-order independence (start workflows before their fleet deploys;
tasks buffer) and means a broken module can only fail its own fleet's
tasks, never a shared parse step.

Consequences: adding a workflow/activity = shipping a new image (ordinary
deploy; the queue buffers during the roll). "What's available" has no
central catalog — the answer is the task-queue page (pollers, heartbeat
telemetry) plus your deploy manifests; the runner command keeps
registration explicit and reviewable. Changing *in-flight* workflow logic
needs `workflow.patched()` or Worker Versioning (build IDs) — activities
are simpler: not replayed, so new code applies on next retry.

### Starters register nothing

Registration is a worker-only concept. A starter is just a gRPC client
that sends **names**: workflow type + task queue + id + serialized args —
the server stores strings, a worker's table resolves them. Our starters
import the workflow class purely for type-safety (the import never
executes workflow code); `client.execute_workflow("InvoiceWorkflow", …)`
with a bare string is equally valid — which is how the UI, the CLI, and
cross-language callers start workflows.

Image topology: **one image per codebase, one command per fleet, replicas
per scale.** The three billing fleets are the same image with three
different runner commands. Starters aren't a deployment artifact at all —
they're call sites (an API handler, a Lambda, a Temporal Schedule, a human
in the UI). Org pattern: a tiny per-domain *contracts* package (type
names, queue names, arg dataclasses — nothing executable) shared by the
worker repo and its callers.

## A concrete trace — one file through the fleets

The complex batch starts four fleets (default mode). Following
`payments_2026-08.csv` through them:

| Server dispatches | Queue | Picked up by | Why |
|---|---|---|---|
| workflow task: `FileIngestWorkflow` starts | `file-ingest` | small (workflows) | deciding "call land next" is microseconds of replay |
| activity `land_file` | `file-ingest` | medium (activities) | I/O stream; 1 of 16 slots |
| workflow task: advance | `file-ingest` | small | never queued behind anyone's activity — small runs none |
| activities `classify_route`, `resolve_transform_spec` | `file-ingest` | medium | metadata lookups on the thread pool |
| workflow task: child `transform-payments-…` starts | `etl-pipeline` | small (etl) | the child's coordination |
| activity `submit_emr_job` | `etl-pipeline` | medium (etl) | blocking boto3 launcher, heartbeating |
| activity `run_local_transform` | `etl-pipeline` | medium (etl) | `spark_remote` set ⇒ thin dbt client — the cluster computes |
| activity `validate_output` | `etl-pipeline` | medium (etl) | metadata checks |

In the fallback (`SPARK_CONNECT_URI=""`) one thing changes: the workflow
routes `run_local_transform` to `compute-large`, a fifth fleet (large
profile) picks it up, and the profile bites — each slot is a Spark JVM, so
admission is resource-tuned and capped at 2 while ten queued transforms
drain without ever slowing the medium fleets. **Queue = who may run it;
profile = how much runs at once.**

## Profiles are templates, not shared pools

One deliberate design point: sizes are **resource envelopes teams
instantiate**, not company-wide shared fleets that everyone's code lands in.
A Temporal worker runs the code it registers — a literal shared "large pool
for the whole business" would mean one image containing every team's heavy
dependencies (dependency conflicts, coupled deploys, cross-team blast
radius). So: each team deploys its own workers from its own image, picks a
standard profile, and names its queues `{domain}-{concern}` — the platform
standardizes the *envelope*, ownership stays with the team.

## The profiles — 5 sizes × 3 shapes

**Sizes** pick the tier; **shapes** lean the same tier toward compute or
memory. `<size>` is the balanced general shape; `<size>-cpu` and
`<size>-mem` are its variants. Choose general unless profiling says not to.

| Size | Concurrency envelope | What belongs here |
|---|---|---|
| **xsmall** | 8 wf tasks · 4 slots | Sidecar-scale coordination: low-traffic workflow-only fleets, glue, dev fleets |
| **small** | 16 wf tasks · 8 slots | **Workflow coordination** at normal traffic — replay is CPU-light and must never queue behind activities |
| **medium** | 8 wf tasks · 16 slots · 16-thread pool | **I/O-bound activities**: API/boto3/DB calls, SFTP streams, submit-and-poll launchers, dbt-as-client over Spark Connect |
| **large** | resource-tuned slots [1,2] · 4/s queue cap | **The activity IS the compute**: subprocess JVMs, ffmpeg, in-process ML. Prefer managed compute when it fits |
| **xlarge** | resource-tuned slots [1,4] · 8/s queue cap | The large class, for workloads that consistently outgrow it |

Shape semantics: `-cpu` changes only the box (more vCPU per GB); `-mem`
changes the box (more GB per vCPU) **and halves activity admission** — each
task is assumed to hold real memory.

Fargate sizing per profile (CDK `WORKER_PROFILE_SIZES`, vCPU / GB):

| Size | general | -cpu | -mem |
|---|---|---|---|
| xsmall | 0.25 / 0.5 | 0.5 / 1 | 0.25 / 2 |
| small | 0.5 / 1 | 1 / 2 | 0.5 / 4 |
| medium | 1 / 2 | 2 / 4 | 1 / 8 |
| large | 4 / 16 | 8 / 16 | 4 / 30 |
| xlarge | 8 / 32 | 16 / 32 | 8 / 60 |

Definitions live in one generated matrix per side
(`worker_platform/profiles.py` and the CDK map), with explicit
`cpu`/`memoryLimitMiB` overrides for true outliers. Rule of thumb: one
fleet overriding is normal; a *class* of fleets overriding the same way
means the matrix is missing an entry.

## How the ETL instantiates it

| Fleet | Queue | Profile | Registers |
|---|---|---|---|
| ingest coordination | `file-ingest` | small | `FileIngestWorkflow` |
| ingest activities | `file-ingest` | medium | SFTP stream, routing, quarantine copy, spec resolve |
| etl coordination | `etl-pipeline` | small | `EtlPipelineWorkflow` |
| etl activities | `etl-pipeline` | medium | EMR launcher, validation, **transform client** (dbt → Spark Connect) |
| big-compute lane | `compute-large` | large | `run_local_transform` — only in the in-process fallback (`SPARK_CONNECT_URI=""`); the default prod-shaped mode deploys no large fleet at all |

`compute-large` is deliberately generic (not `etl-*`): it's the platform's
big-compute lane, and any team's activity-is-the-compute workload would
route there — with its own fleet registering its own code.

The package lives at the repo root (`worker_platform/`) — it is a platform
library, not an ETL helper. The proof-by-second-tenant is
`examples/platform-demo/`: a billing workload (its own queues, its own
code) running on the same runner and profiles with zero ETL involvement —
`./examples/platform-demo/run.sh` prints `PLATFORM DEMO: PASS`.

## On ECS — the deployment shape

One fleet = **one ECS Fargate service**, defined by the CDK
`TemporalWorkerService` construct. The construct speaks the same profile
language as the runner: the *same* profile string sets the in-process
concurrency envelope (via the container command) and the Fargate task size
(via the `profile` prop) — so the process and its box always agree.

Task sizes come from the profile matrix above (`WORKER_PROFILE_SIZES` —
all 15 size×shape combinations are valid Fargate cpu/memory pairings).

The images themselves: `etl/Dockerfile` (the ETL team's codebase image)
and `examples/platform-demo/Dockerfile` (the billing tenant's) — built in
CI on every push for reference (not pushed to a registry); the ci workflow
also carries a commented-out example of the real ECS deploy (OIDC role →
ECR push → CDK deploy pinning the image tag, or a forced service roll).

A complete fleet definition:

```ts
const renderFleet = new TemporalWorkerService(stack, 'BillingRenderFleet', {
  ecsCluster,                                        // shared or team-owned
  image: ecs.ContainerImage.fromEcrRepository(repo, 'v42'),  // the TEAM's image
  command: [
    'python', '-m', 'worker_platform',
    '--queue', 'billing-render', '--profile', 'large',
    '--activities', 'billing.render',
  ],
  taskQueue: 'billing-render',
  temporalAddress: temporal.grpcEndpoint,            // the internal NLB
  temporalNamespace: 'team-billing',
  profile: 'large',                                  // Fargate 4 vCPU / 16 GB
  autoscaling: {
    maxCapacity: 10,
    scaleUpBacklog: 20,                              // +1 task at 20 queued, +3 at 80
    temporalHttpEndpoint: temporal.httpApiEndpoint,  // for the backlog poller
  },
});
renderFleet.allowGrpcTo(temporal.serverService);      // SG: worker -> frontend :7233
```

What the construct wires for you:

- **Task definition** — team image + the worker_platform command;
  `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_TASK_QUEUE` env;
  CloudWatch logs (1-week retention).
- **Autoscaling** — a 1-minute Lambda polls `DescribeTaskQueue` (reportStats,
  backlog stats) through the cluster's HTTP API, publishes
  `ApproximateBacklogCount` to CloudWatch, and the service step-scales on
  it: +1 at the threshold, +3 at 4×, drain to min on empty.
- **The page-worthy alarm** — `ApproximateBacklogAgeSeconds > 300` for 5
  minutes (oldest task waiting too long: fleet under-scaled or down) — the
  schedule-to-start alarm, defined with the fleet.
- **Networking** — `allowGrpcTo` opens the one security-group rule a worker
  needs. Workers make only *outbound* connections (long-poll); no inbound
  ports, no load balancer, no service discovery entry.
- **IAM** — the task role is where the fleet's AWS permissions live (S3
  prefixes, EMR submission); Temporal itself needs no AWS permissions.

**Cluster topology:** on Fargate an ECS cluster is a logical namespace,
not a capacity pool — every task is an isolated microVM, so one cluster
isolates as well as fifty. Recommended: a **platform cluster** (Temporal
server/UI — so team deploy roles can't touch it) plus **one shared
workload cluster** for all fleets; team isolation is carried by
namespaces, queues, per-fleet services, task roles, and tags. Split
further only for IAM legibility (clusters are free), and know the real
scale walls: Fargate vCPU quotas and blast radius are per **account**,
and each task consumes a subnet IP — size the VPC, not the cluster count.
Environments (dev/stage/prod) are the sacred boundary, ideally accounts.

Operating it day to day:

- **Deploy new code / new activity types** → push a new image tag, update
  the service (ECS rolling deploy). The queue buffers tasks during the
  roll; nothing is lost.
- **Scale manually** → desired count; **scale automatically** → the backlog
  policy above.
- **A fleet crashes or is scaled to zero** → tasks accumulate on its queue
  and drain when it returns; schedule-to-start latency is the alarm to set.
- **A team joins the platform** → they ship an image and instantiate this
  construct once per fleet (typically: small for workflows, medium for
  activities, large only if their activity is the compute).

## When may data flow through a worker?

The old absolute ("workers never touch data content") predates the profile
matrix. The current policy: a worker may process data only when **all four**
hold —

| Test | Meaning |
|---|---|
| **Byte-shaped** | decrypt, decompress, checksum, archive-split, render — no SQL dialect involved. Query-shaped work always goes to the external engine (the single-engine rule). |
| **Bounded** | an enforced cap (e.g. the gunzip stage's 1 GiB decompression guard); unbounded input goes to the cluster or quarantines |
| **Streamed** | chunked through disk/pipes, never whole-payload-in-RAM |
| **Profiled** | on a fleet whose shape admits it (medium for streams, large/large-mem when the item is the compute) |

Working example: the ingest pipeline's **gunzip preprocess** — a compressed
vendor drop is stream-decompressed on the medium fleet (capped, heartbeated,
bad archives quarantined non-retryably) before routing. The converse stays
firm: the landing→table read, all transformations, and bulk outputs are
cluster-side because they fail the byte-shaped or bounded test.

## Rules the runner encodes (so teams don't have to remember them)

- Sync (blocking) activities always get a thread pool — the boto3-in-async
  footgun can't happen.
- A `large` profile uses resource-based slot admission with hard bounds and
  a per-queue dispatch cap; a fixed count can't over-admit JVMs on a quiet
  host, and a scaled-out fleet can't collectively over-launch.
- Workflow-vs-activity fleet split is a deployment decision, not a code
  change: run the same modules under two invocations.
- In prod these invocations are container commands on the CDK
  `TemporalWorkerService` (one ECS service per fleet, autoscaled on queue
  backlog) — same three decisions, same profiles.
