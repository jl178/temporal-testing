import asyncio
import os

from temporalio import activity


@activity.defn
async def compose_greeting(name: str) -> str:
    # Load-test knob (default 0): a nonzero delay makes work outpace the
    # fleet so a real backlog forms — used to exercise backlog autoscaling.
    delay = float(os.environ.get("GREET_DELAY_SECONDS", "0"))
    if delay:
        await asyncio.sleep(delay)
    return f"Hello, {name}!"
