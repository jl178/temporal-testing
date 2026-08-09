"""Generic Temporal worker runner: queue + size profile + registrations.

    python -m worker_platform --queue etl-pipeline --profile small \
        --workflows workflow
    python -m worker_platform --queue etl-pipeline --profile medium \
        --activities activities
    python -m worker_platform --queue compute-large --profile large \
        --activities activities:run_local_transform

Registration specs are `module` (auto-discover every @workflow.defn /
@activity.defn in the module) or `module:Name,Name` (explicit). Connection
comes from TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE. Any team's worker
deployment is these three decisions: which queue, which size, which code.
"""
import argparse
import asyncio
import importlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import ResourceBasedSlotConfig, Worker, WorkerTuner

from .profiles import PROFILES

_WORKFLOW_MARKER = "__temporal_workflow_definition"
_ACTIVITY_MARKER = "__temporal_activity_definition"


def _load(spec: str, marker: str) -> list:
    module_name, _, names = spec.partition(":")
    module = importlib.import_module(module_name)
    if names:
        return [getattr(module, n) for n in names.split(",")]
    found = [
        obj
        for name, obj in vars(module).items()
        if not name.startswith("_") and hasattr(obj, marker)
    ]
    if not found:
        raise SystemExit(f"no registrations with {marker} found in {module_name}")
    return found


async def main() -> None:
    parser = argparse.ArgumentParser(prog="worker_platform")
    parser.add_argument("--queue", required=True)
    parser.add_argument("--profile", required=True, choices=sorted(PROFILES))
    parser.add_argument("--workflows", action="append", default=[],
                        help="module or module:Class,Class")
    parser.add_argument("--activities", action="append", default=[],
                        help="module or module:fn,fn")
    args = parser.parse_args()

    profile = PROFILES[args.profile]
    workflows = [w for spec in args.workflows for w in _load(spec, _WORKFLOW_MARKER)]
    activities = [a for spec in args.activities for a in _load(spec, _ACTIVITY_MARKER)]
    if not workflows and not activities:
        raise SystemExit("register at least one of --workflows/--activities")

    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )

    kwargs: dict = {
        "task_queue": args.queue,
        "workflows": workflows,
        "activities": activities,
    }
    use_tuner = profile.resource_tuner and activities
    if use_tuner:
        # A tuner owns every slot type — fixed counts can't be mixed in.
        kwargs["tuner"] = WorkerTuner.create_resource_based(
            target_memory_usage=profile.tuner_target_mem,
            target_cpu_usage=profile.tuner_target_cpu,
            workflow_config=ResourceBasedSlotConfig(
                minimum_slots=1,
                maximum_slots=profile.max_concurrent_workflow_tasks,
            ),
            activity_config=ResourceBasedSlotConfig(
                minimum_slots=profile.tuner_min_slots,
                maximum_slots=profile.tuner_max_slots,
                ramp_throttle=timedelta(seconds=1),
            ),
        )
    else:
        kwargs["max_concurrent_workflow_tasks"] = profile.max_concurrent_workflow_tasks
        if activities:
            kwargs["max_concurrent_activities"] = profile.max_concurrent_activities
    if activities:
        kwargs["activity_executor"] = ThreadPoolExecutor(
            max_workers=profile.thread_pool_size
        )
        if profile.queue_activities_per_second:
            kwargs["max_task_queue_activities_per_second"] = (
                profile.queue_activities_per_second
            )

    worker = Worker(client, **kwargs)
    kinds = []
    if workflows:
        kinds.append(f"{len(workflows)} workflow(s)")
    if activities:
        kinds.append(f"{len(activities)} activity(ies)")
    print(
        f"[worker_platform] {args.profile} worker on {args.queue!r}: "
        + ", ".join(kinds),
        flush=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
