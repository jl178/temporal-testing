"""Platform-standard worker size profiles.

A profile is a RESOURCE ENVELOPE — concurrency, admission control, and
dispatch limits — that any team's worker deployment instantiates. Profiles
are templates, not shared pools: each team runs its own workers (its own
image, its own code, its own deploy cadence) and picks a size. Queues are
conventionally named {domain}-{concern} or {domain}-{profile}
(file-ingest, etl-pipeline, compute-large).

Placement guide — what runs where:

  small   Workflow coordination. Workflow tasks are CPU-light replay; they
          must never queue behind activities, so workflow-only workers are
          always small. Also fine for trivial pure-logic activities.

  medium  I/O-bound activities: API calls, boto3/S3 metadata, database
          queries, SFTP streams, job launchers that submit-and-poll external
          compute (EMR, Batch), dbt-as-client over Spark Connect. High slot
          counts are safe because nothing here is memory- or CPU-heavy;
          blocking (sync) activities run on a thread pool sized to the
          slots, so an event loop is never stalled.

  large   The activity IS the compute: subprocess JVMs, ffmpeg, in-process
          ML inference, big pandas crunches. Slots are admitted by observed
          host CPU/memory (resource tuner) inside hard bounds, and the
          queue carries a server-enforced dispatch cap so a scaled-out
          fleet can't collectively over-launch. Prefer offloading to a
          managed service (EMR, Batch) when one fits — large fleets are for
          work with nowhere better to live.
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
    # Resource-based slot admission (large): observed CPU/mem, hard-bounded.
    resource_tuner: bool = False
    tuner_min_slots: int = 1
    tuner_max_slots: int = 2
    tuner_target_mem: float = 0.8
    tuner_target_cpu: float = 0.9
    # Server-enforced dispatch cap for the whole queue (None = uncapped).
    queue_activities_per_second: float | None = None


PROFILES: dict[str, WorkerProfile] = {
    "small": WorkerProfile(
        name="small",
        max_concurrent_workflow_tasks=16,
        max_concurrent_activities=8,
        thread_pool_size=8,
    ),
    "medium": WorkerProfile(
        name="medium",
        max_concurrent_workflow_tasks=8,
        max_concurrent_activities=16,
        thread_pool_size=16,
    ),
    "large": WorkerProfile(
        name="large",
        max_concurrent_workflow_tasks=2,
        max_concurrent_activities=2,  # fallback if tuner unavailable
        thread_pool_size=4,
        resource_tuner=True,
        queue_activities_per_second=4,
    ),
}
