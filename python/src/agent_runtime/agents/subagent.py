"""复用main状态机完成一次任务，终态结果由go runtime持久提交"""

from dataclasses import replace

from ..agent import BusinessError, ExecutionContext, ExecutionResult, JSONObject
from ..prompt import PromptBuilder
from .main import MainAgent


TASK_PROMPT = "你是一次性subagent，使用独立上下文完成收到的任务，最终回复提供完整结果，完成后由runtime结束实例"


class SubagentAgent(MainAgent):
    def run(self, context: ExecutionContext) -> ExecutionResult:
        if context.event.type == "subagent.request":
            state = context.agent.state
            if state and state.get("request_status") != "idle":
                raise BusinessError("subagent只接受一个任务")
            if state and state.get("messages"):
                raise BusinessError("subagent任务需要独立的空历史")
            payload = context.event.payload
            if not isinstance(payload, dict) or payload.keys() - {"message", "context"}:
                raise BusinessError("subagent.request只接受message和context")
            message = payload.get("message")
            if not isinstance(message, str) or not message.strip():
                raise BusinessError("subagent.request需要非空message")
            task_context = payload.get("context", "")
            if not isinstance(task_context, str):
                raise BusinessError("subagent.request的context需要字符串")
            if task_context:
                message += "\n\n任务上下文：\n" + task_context
            context = replace(
                context, event=replace(context.event, type="main.request", payload={"message": message})
            )
        elif context.event.type == "main.request":
            raise BusinessError("subagent只接受subagent.request任务")
        return super().run(context)

    def _prompt_builder(self, context: ExecutionContext) -> PromptBuilder:
        builder = super()._prompt_builder(context)
        system = builder.system_prompt
        return PromptBuilder((system + "\n\n" if system else "") + TASK_PROMPT, builder.max_chars)

    def _finish(
        self, context: ExecutionContext, update: JSONObject, *, result: str | None = None,
        error: str | None = None,
    ) -> ExecutionResult:
        completed = super()._finish(context, update, result=result, error=error)
        task_result = (
            {"status": "succeeded", "output": {"message": result}}
            if error is None
            else {"status": "failed", "output": {}, "error": {"kind": "business", "message": error}}
        )
        return replace(completed, task_result=task_result)
