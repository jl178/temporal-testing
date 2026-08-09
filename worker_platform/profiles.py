"""Platform-standard worker profiles: 5 sizes × 3 shapes.

A profile is a RESOURCE ENVELOPE — concurrency, admission control, and
dispatch limits — that any team's worker deployment instantiates. Profiles
are templates, not shared pools: each team runs its own workers (its own
image, its own code, its own deploy cadence) and picks a profile. The same
profile name sizes the Fargate task in CDK (WORKER_PROFILE_SIZES), so the
process envelope and the box always agree.

Names: `<size>` is the general shape; `<size>-cpu` and `<size>-mem` are
compute- and memory-leaning variants of the same tier.

Size guide (what runs where):

  xsmall  Sidecar-scale coordination: low-traffic workflow-only fleets,
          cron-ish glue, dev fleets.
  small   Workflow coordination at normal traffic. Workflow tasks are
          CPU-light replay and must never queue behind activities.
  medium  I/O-bound activities: API calls, boto3/S3 metadata, DB queries,
          SFTP streams, submit-and-poll launchers, dbt-as-client over
          Spark Connect. Blocking (sync) activities run on a thread pool.
  large   The activity IS the compute: subprocess JVMs, ffmpeg, in-process
          ML inference. Resource-tuned slot admission + a server-enforced
          queue dispatch cap. Prefer offloading to a managed service when
          one fits.
  xlarge  Same class as large, bigger box, more tuner headroom — for
          workloads that consistently outgrow large (the "three teams
          overriding = new tier" rule, pre-applied).

Shape guide:

  (general)  Balanced — the default; choose it unless profiling says not to.
  -cpu       Compute-bound tasks (encoding, scoring, compression): the box
             gets more vCPU per GB; concurrency envelope unchanged.
  -mem       Memory-bound tasks (big frames, model weights, fat payloads):
             the box gets more GB per vCPU, and activity admission is
             HALVED — each task is assumed to hold real memory.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerProfile:
    name: str
    max_concurrent_workflow_tasks: int
    # Fixed-slot admission (ignored when the resource tuner is on).
    max_concurrent_activities: int
    # Thread pool for sync (blocking) activities.
    thread_pool_size: int
    # Resource-based slot admission (large tiers): observed CPU/mem, bounded.
    resource_tuner: bool = False
    tuner_min_slots: int = 1
    tuner_max_slots: int = 2
    tuner_target_mem: float = 0.8
    tuner_target_cpu: float = 0.9
    # Server-enforced dispatch cap for the whole queue (None = uncapped).
    queue_activities_per_second: float | None = None


_SIZES: dict[str, dict] = {
    "xsmall": dict(wf=8, act=4, threads=4),
    "small": dict(wf=16, act=8, threads=8),
    "medium": dict(wf=8, act=16, threads=16),
    "large": dict(wf=2, act=2, threads=4, tuner=True, tmax=2, cap=4),
    "xlarge": dict(wf=2, act=4, threads=8, tuner=True, tmax=4, cap=8),
}


def _make(size: str, shape: str | None) -> WorkerProfile:
    s = _SIZES[size]
    act = s["act"]
    tmax = s.get("tmax", 2)
    if shape == "mem":
        # Memory-leaning: each task holds real memory — admit half as many.
        act = max(1, act // 2)
        tmax = max(1, tmax // 2)
    name = size if shape is None else f"{size}-{shape}"
    return WorkerProfile(
        name=name,
        max_concurrent_workflow_tasks=s["wf"],
        max_concurrent_activities=act,
        thread_pool_size=s["threads"],
        resource_tuner=s.get("tuner", False),
        tuner_max_slots=tmax,
        queue_activities_per_second=s.get("cap"),
    )


PROFILES: dict[str, WorkerProfile] = {
    profile.name: profile
    for size in _SIZES
    for profile in (_make(size, None), _make(size, "cpu"), _make(size, "mem"))
}
