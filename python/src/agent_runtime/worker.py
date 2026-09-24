"""常驻python worker，stdout只承载协议帧"""

import sys
import traceback
from contextlib import redirect_stdout
from typing import BinaryIO

from .agent import BusinessError, Runner
from .agents import EchoAgent
from .protocol import (
    MAX_FRAME_BYTES,
    VERSION,
    decode_frame,
    encode_frame,
    parse_request,
    result_to_wire,
)


def _error_message(error: Exception) -> str:
    message = str(error) or type(error).__name__
    return message.encode("utf-8", errors="backslashreplace").decode("utf-8")[:4096]


def serve(
    source: BinaryIO, destination: BinaryIO, runners: dict[tuple[str, str], Runner]
) -> int:
    while True:
        frame = source.readline(MAX_FRAME_BYTES + 1)
        if not frame:
            return 0
        try:
            request_id, context = parse_request(decode_frame(frame))
        except Exception as error:
            print(f"worker请求无效: {_error_message(error)}", file=sys.stderr)
            return 2
        response = {"version": VERSION, "id": request_id}
        try:
            with redirect_stdout(sys.stderr):
                definition = context.agent.definition
                runner = runners.get((definition.id, definition.version))
                if runner is None:
                    raise ValueError(f"未知definition: {definition.id}@{definition.version}")
                result = runner.run(context)
                response["result"] = result_to_wire(result, context)
                output = encode_frame(response)
        except Exception as error:
            if isinstance(error, BusinessError):
                kind = "business"
            else:
                kind = "runtime"
                traceback.print_exc(file=sys.stderr)
            response.pop("result", None)
            response["error"] = {"kind": kind, "message": _error_message(error)}
            try:
                output = encode_frame(response)
            except Exception:
                print("worker错误响应超过协议限制", file=sys.stderr)
                return 2
        destination.write(output)
        destination.flush()


def main() -> int:
    return serve(sys.stdin.buffer, sys.stdout.buffer, {("echo", "1"): EchoAgent()})


if __name__ == "__main__":
    raise SystemExit(main())
