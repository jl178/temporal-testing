"""Temporal Schedule for the ingest batch — cron, done properly.

    python -m ingest.schedule create     # hourly schedule, created PAUSED
    python -m ingest.schedule unpause    # start running hourly
    python -m ingest.schedule trigger    # fire one run now (works while paused)
    python -m ingest.schedule describe   # spec, state, recent/upcoming runs
    python -m ingest.schedule pause | delete

Each fired run starts FileIngestWorkflow with the standard env-driven config
(catalog, spark mode); Temporal appends the fire time to the workflow id, so
runs are individually addressable and the usual search attributes apply to
every child the run spawns. Overlap policy: skip — a slow batch is never
piled onto.
"""
import asyncio
import os
import sys
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
)

from ingest.activities import IngestConfig
from ingest.workflow import TASK_QUEUE, FileIngestWorkflow
from runtime_env import catalog_from_env, spark_remote_from_env

SCHEDULE_ID = "file-ingest-batch"


def _config() -> IngestConfig:
    catalog = catalog_from_env()
    return IngestConfig(
        catalog=catalog,
        spark_remote=spark_remote_from_env(),
        consolidation_spec="consolidation" if catalog else None,
    )


async def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "describe"
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    handle = client.get_schedule_handle(SCHEDULE_ID)

    if command == "create":
        await client.create_schedule(
            SCHEDULE_ID,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    FileIngestWorkflow.run,
                    _config(),
                    id="file-ingest-sched",
                    task_queue=TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
                state=ScheduleState(
                    paused=True, note="unpause for hourly ingest batches"
                ),
            ),
        )
        print(f"schedule {SCHEDULE_ID!r} created (paused, hourly, overlap=skip)")
    elif command == "trigger":
        await handle.trigger()
        print("triggered one run")
    elif command == "pause":
        await handle.pause(note="paused via ingest.schedule")
        print("paused")
    elif command == "unpause":
        await handle.unpause(note="unpaused via ingest.schedule")
        print("unpaused — running hourly")
    elif command == "delete":
        await handle.delete()
        print("deleted")
    elif command == "describe":
        desc = await handle.describe()
        state = desc.schedule.state
        print(f"{SCHEDULE_ID}: paused={state.paused} note={state.note!r}")
        print(f"recent: {[a.scheduled_at.isoformat() for a in desc.info.recent_actions[-3:]]}")
        print(f"next: {[t.isoformat() for t in desc.info.next_action_times[:2]]}")
    else:
        raise SystemExit(f"unknown command {command!r} (see module docstring)")


if __name__ == "__main__":
    asyncio.run(main())
