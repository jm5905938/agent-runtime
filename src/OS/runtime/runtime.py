import asyncio
from datetime import datetime, timezone
from uuid import UUID

from OS.domain import AgentInstance, AgentStatus, Event, Execution, ExecutionStatus
from OS.runtime.agent import AgentRunner, ExecutionContext, ExecutionResult
from OS.runtime.lifecycle import LifecycleError, LifecycleManager
from OS.runtime.registry import AgentRegistry
from OS.runtime.statemanager import StateManager


class Runtime:
    """
    runtime核心。
    """

    def __init__(self) -> None:
        self.registry = AgentRegistry()
        self.lifecycle = LifecycleManager()
        self.state_manager = StateManager()
        self.runners: dict[UUID, AgentRunner] = {}
        self.executions: dict[UUID, Execution] = {}
        self._locks: dict[UUID, asyncio.Lock] = {}

    def register(
        self,
        agent: AgentInstance,
        runner: AgentRunner,
    ) -> None:
        """
        注册 Agent 与对应执行器。
        """

        self.registry.register(agent)
        self.runners[agent.id] = runner
        self._locks[agent.id] = asyncio.Lock()
        if agent.status == AgentStatus.CREATED:
            self.lifecycle.transition(agent, AgentStatus.ACTIVE)

    async def process(
        self,
        agent_id: UUID,
        event: Event,
    ) -> ExecutionResult:
        """
        串行处理事件
        """

        agent = self.registry.get(agent_id)
        async with self._locks[agent_id]:
            if agent.status != AgentStatus.ACTIVE:
                raise LifecycleError(f"Agent{agent_id}当前状态不允许执行")

            runner = self.runners[agent_id]
            execution = Execution(agent_id=agent_id, event_id=event.id)
            self.executions[execution.id] = execution
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.now(timezone.utc)

            try:
                result = await runner.run(ExecutionContext(agent=agent, event=event))#core
                self.state_manager.apply(agent, result)
            except asyncio.CancelledError:
                execution.status = ExecutionStatus.FAILED
                execution.error = "执行已取消"
                raise
            except Exception as error:
                execution.status = ExecutionStatus.FAILED
                execution.error = f"{type(error).__name__}: {error}"
                raise
            else:
                execution.status = ExecutionStatus.COMPLETED
                return result
            finally:
                execution.finished_at = datetime.now(timezone.utc)
