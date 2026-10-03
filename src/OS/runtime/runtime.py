import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from uuid import UUID

from OS.domain import Action, AgentInstance, AgentStatus, Event, Execution, ExecutionStatus
from OS.runtime.executor import Executor
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
        self.actions: dict[UUID, Action] = {}
        self.executor = Executor()
        self._pending: deque[tuple[UUID, Event]] = deque()
        self._pending_actions: deque[tuple[UUID, Action]] = deque()
        self._drain_lock = asyncio.Lock()
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
                result = await runner.run(ExecutionContext(agent=agent, event=event))
                actions = deepcopy(result.actions)
                action_ids = [action.id for action in actions]
                if len(set(action_ids)) != len(action_ids) or any(
                    action_id in self.actions for action_id in action_ids
                ):
                    raise ValueError("Action 标识重复")
                for action in actions:
                    action.execution_id = execution.id
                self.state_manager.apply(agent, result)
                for action in actions:
                    self.actions[action.id] = action
                    self._pending_actions.append((agent_id, action))
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

    def submit(self, agent_id: UUID, event: Event) -> None:
        """
        将事件加入内存队列。
        """

        self.registry.get(agent_id)
        self._pending.append((agent_id, event))

    async def run_until_idle(self) -> None:
        """
        处理队列中的事件与 Action，直到没有待处理工作。
        """

        async with self._drain_lock:
            while self._pending or self._pending_actions:
                if self._pending:
                    agent_id, event = self._pending[0]
                    await self.process(agent_id, event)
                    self._pending.popleft()
                else:
                    agent_id, action = self._pending_actions[0]
                    agent = self.registry.get(agent_id)
                    if agent.status != AgentStatus.ACTIVE:
                        raise LifecycleError(f"Agent {agent_id} 当前状态不允许执行 Action")
                    event = await self.executor.execute(action)
                    self._pending.append((agent_id, event))
                    self._pending_actions.popleft()
