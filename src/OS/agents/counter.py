from OS.runtime.agent import ExecutionContext, ExecutionResult


class CounterAgent:
    """
    数数 Agent。
    """

    async def run(self, context: ExecutionContext) -> ExecutionResult:
        count = context.agent.state.get("count", 0)
        count += 1
        return ExecutionResult(state_update={"count": count})
