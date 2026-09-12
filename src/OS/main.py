import asyncio
import json

from OS.agents.echo import EchoAgent
from OS.domain import AgentInstance, Event
from OS.runtime.executor import EchoHandler
from OS.runtime.runtime import Runtime


async def demo() -> None:
    runtime = Runtime()
    runtime.executor.register("echo", EchoHandler())
    agent = AgentInstance("echo")
    runtime.register(agent, EchoAgent())
    runtime.submit(agent.id, Event("echo.request", {"message": "Hello, runtime!"}))
    await runtime.run_until_idle()
    print(json.dumps(agent.state, ensure_ascii=False, indent=2))
    print(f"Executions: {len(runtime.executions)}, Actions: {len(runtime.actions)}")


def main() -> None:
    asyncio.run(demo())


if __name__ == "__main__":
    main()
