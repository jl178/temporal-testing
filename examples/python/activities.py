from temporalio import activity


@activity.defn
async def compose_greeting(name: str) -> str:
    return f"Hello, {name}!"
