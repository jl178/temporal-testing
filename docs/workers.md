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
