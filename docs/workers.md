# The worker platform — size profiles for the whole business

Temporal workers are the platform's compute-attachment mechanism: a worker
is a deployment that polls a **queue** with a **size profile** and registers
**code**. Those are the only three decisions any team makes:

```sh
python -m worker_platform --queue <queue> --profile <small|medium|large> \
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

## A concrete trace — one file through the fleets

The complex batch starts four fleets (default mode). Following
`payments_2026-08.csv` through them:

| Server dispatches | Queue | Picked up by | Why |
|---|---|---|---|
| workflow task: `FileIngestWorkflow` starts | `file-ingest` | small (workflows) | deciding "call land next" is microseconds of replay |
| activity `land_sftp_file` | `file-ingest` | medium (activities) | I/O stream; 1 of 16 slots |
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

## The profiles

| Profile | Envelope | What belongs here |
|---|---|---|
| **small** | 16 workflow tasks · 8 activity slots | **Workflow coordination.** Replay is CPU-light and must never queue behind activities — workflow-only workers are always small. Also trivial pure-logic activities. |
| **medium** | 8 wf tasks · 16 activity slots · 16-thread pool | **I/O-bound activities**: API calls, boto3/S3 metadata, DB queries, SFTP streams, launchers that submit-and-poll external compute (EMR, Batch), dbt-as-client over Spark Connect. Blocking (sync) activities run on the thread pool — an event loop is never stalled. |
| **large** | resource-tuned slots (80% mem / 90% CPU targets, hard-bounded [1,2]) · 4/s server-enforced queue dispatch cap | **The activity IS the compute**: subprocess JVMs, ffmpeg, in-process ML inference, big in-memory crunches. Prefer offloading to a managed service when one fits — large fleets are for work with nowhere better to live. |

Profile definitions (one place to tune platform-wide):
`etl/worker_platform/profiles.py`.

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

## On ECS — the deployment shape

One fleet = **one ECS Fargate service**, defined by the CDK
`TemporalWorkerService` construct. The construct speaks the same profile
language as the runner: the *same* profile string sets the in-process
concurrency envelope (via the container command) and the Fargate task size
(via the `profile` prop) — so the process and its box always agree.

| Profile | Fargate task size |
|---|---|
| small | 0.25 vCPU / 512 MB |
| medium | 0.5 vCPU / 1 GB |
| large | 4 vCPU / 16 GB |

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
- **Autoscaling** — a 1-minute Lambda polls `DescribeTaskQueue` (enhanced
  backlog stats) through the cluster's HTTP API, publishes
  `ApproximateBacklogCount` to CloudWatch, and the service step-scales on
  it: +1 at the threshold, +3 at 4×, drain to min on empty.
- **Networking** — `allowGrpcTo` opens the one security-group rule a worker
  needs. Workers make only *outbound* connections (long-poll); no inbound
  ports, no load balancer, no service discovery entry.
- **IAM** — the task role is where the fleet's AWS permissions live (S3
  prefixes, EMR submission); Temporal itself needs no AWS permissions.

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
