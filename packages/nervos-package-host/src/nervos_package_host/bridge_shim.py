"""Loopback HTTP shim for generic process agents.

Provides OpenAI-compatible /v1/chat/completions and MCP /mcp endpoints that translate
to the existing wire protocol frames (model_request/tool_request) over stdio.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, BinaryIO, ClassVar, cast
from urllib.parse import urlparse

from nervos_package_host.wire import (
    HOST_PROTOCOL_VERSION,
    HostProtocolError,
    read_frame,
    write_frame,
)

_LOGGER = logging.getLogger(__name__)

MAX_REQUEST_BYTES = 1024 * 1024
DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_MAX_REQUESTS = 1000


class ShimError(Exception):
    """Shim-specific error with HTTP status code."""

    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


@dataclass(slots=True)
class ShimConfig:
    token: str
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    max_requests: int = DEFAULT_MAX_REQUESTS


def _new_pending() -> dict[str, asyncio.Future[dict[str, object]]]:
    return {}


def _new_tools_info() -> list[dict[str, object]]:
    return []


@dataclass(slots=True)
class ShimState:
    config: ShimConfig
    request_count: int = 0
    wire_in: BinaryIO | None = None
    wire_out: BinaryIO | None = None
    loop: asyncio.AbstractEventLoop | None = None
    next_request_id: int = 0
    pending: dict[str, asyncio.Future[dict[str, object]]] = field(default_factory=_new_pending)
    closed: bool = False
    tools_info: list[dict[str, object]] = field(default_factory=_new_tools_info)

    def next_id(self) -> str:
        self.next_request_id += 1
        return f"shim-{self.next_request_id}"

    def check_request_budget(self) -> None:
        if self.request_count >= self.config.max_requests:
            raise ShimError("request limit exceeded", HTTPStatus.TOO_MANY_REQUESTS)
        self.request_count += 1


class _ShimRequestHandler(BaseHTTPRequestHandler):
    server_version = "NervOS-Shim/1.0"
    sys_version = ""
    state: ClassVar[ShimState | None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        _LOGGER.debug("%s - %s", self.address_string(), format % args)

    def _require_state(self) -> ShimState:
        state = self.state
        if state is None:
            raise ShimError("shim not initialized", HTTPStatus.SERVICE_UNAVAILABLE)
        return state

    def _new_future(self, state: ShimState) -> asyncio.Future[dict[str, object]]:
        # The HTTP server thread has no running event loop; futures are created on
        # the loop captured at start() and resolved by the wire reader on that loop.
        loop = state.loop
        if loop is None:
            raise ShimError("shim event loop not started", HTTPStatus.SERVICE_UNAVAILABLE)
        return loop.create_future()

    def _wait_for_response(self, state: ShimState, request_id: str, kind: str) -> dict[str, object]:
        """Block the HTTP server thread until the wire reader resolves the request."""
        future = state.pending[request_id]
        deadline = time.monotonic() + state.config.request_timeout
        while not future.done():
            if time.monotonic() >= deadline:
                state.pending.pop(request_id, None)
                raise ShimError(f"{kind} request timed out", HTTPStatus.GATEWAY_TIMEOUT)
            time.sleep(0.005)
        try:
            return future.result()
        except Exception as e:
            raise ShimError(f"{kind} request failed: {e}", HTTPStatus.BAD_GATEWAY) from e

    def _require_auth(self) -> None:
        state = self._require_state()
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth[7:] != state.config.token:
            raise ShimError("invalid or missing bearer token", HTTPStatus.UNAUTHORIZED)

    def _require_host(self) -> None:
        host = self.headers.get("Host", "")
        expected = f"127.0.0.1:{cast('HTTPServer', self.server).server_port}"
        if host != expected:
            raise ShimError(f"invalid Host header, expected {expected}", HTTPStatus.BAD_REQUEST)

    def _read_body(self) -> bytes:
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            raise ShimError("missing Content-Length", HTTPStatus.LENGTH_REQUIRED)
        try:
            length = int(content_length)
        except ValueError as err:
            raise ShimError("invalid Content-Length", HTTPStatus.BAD_REQUEST) from err
        if length > MAX_REQUEST_BYTES:
            raise ShimError("request body too large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        body = self.rfile.read(length)
        if len(body) != length:
            raise ShimError("truncated request body", HTTPStatus.BAD_REQUEST)
        return body

    def _send_json(self, status: int, data: Mapping[str, object]) -> None:
        body = json.dumps(data, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, error: ShimError) -> None:
        self._send_json(error.status, {"error": {"message": error.message, "type": "shim_error"}})

    def do_POST(self) -> None:
        state = self.state
        if state is None or state.closed:
            self._send_error(ShimError("shim not initialized", HTTPStatus.SERVICE_UNAVAILABLE))
            return

        path = urlparse(self.path).path
        try:
            self._require_host()
            self._require_auth()
            state.check_request_budget()
            body = self._read_body()

            if path == "/v1/chat/completions":
                self._handle_chat_completions(body)
            elif path == "/mcp":
                self._handle_mcp(body)
            else:
                raise ShimError(f"unknown path: {path}", HTTPStatus.NOT_FOUND)
        except ShimError as e:
            self._send_error(e)
        except Exception:
            _LOGGER.exception("unhandled error in shim handler")
            self._send_error(ShimError("internal error", HTTPStatus.INTERNAL_SERVER_ERROR))

    def _handle_chat_completions(self, body: bytes) -> None:
        state = self._require_state()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as e:
            raise ShimError("invalid JSON", HTTPStatus.BAD_REQUEST) from e

        if not isinstance(parsed, dict):
            raise ShimError("request must be an object", HTTPStatus.BAD_REQUEST)
        request = cast("dict[str, object]", parsed)

        messages = request.get("messages")
        if not isinstance(messages, list) or not messages:
            raise ShimError("messages must be a non-empty array", HTTPStatus.BAD_REQUEST)

        normalized_messages: list[dict[str, str]] = []
        for msg in cast("list[object]", messages):
            if not isinstance(msg, dict):
                raise ShimError("each message must be an object", HTTPStatus.BAD_REQUEST)
            item = cast("dict[str, object]", msg)
            role = item.get("role")
            content = item.get("content")
            if role not in ("system", "user", "assistant"):
                error = "message role must be system, user, or assistant"
                raise ShimError(error, HTTPStatus.BAD_REQUEST)
            if isinstance(content, str):
                normalized_messages.append({"role": role, "content": content})
            elif isinstance(content, list):
                text_parts: list[str] = []
                for part in cast("list[object]", content):
                    part_item = cast("dict[str, object]", part) if isinstance(part, dict) else None
                    if part_item is None or part_item.get("type") != "text":
                        error = "only text content parts are supported"
                        raise ShimError(error, HTTPStatus.BAD_REQUEST)
                    text = part_item.get("text")
                    if isinstance(text, str):
                        text_parts.append(text)
                    else:
                        error = "only text content parts are supported"
                        raise ShimError(error, HTTPStatus.BAD_REQUEST)
                normalized_messages.append({"role": role, "content": "".join(text_parts)})
            else:
                error = "message content must be string or array of text parts"
                raise ShimError(error, HTTPStatus.BAD_REQUEST)

        if request.get("stream") is True:
            raise ShimError("streaming is not supported", HTTPStatus.BAD_REQUEST)
        if request.get("tools") is not None or request.get("functions") is not None:
            error = "tools/functions not supported; use MCP endpoint for tools"
            raise ShimError(error, HTTPStatus.BAD_REQUEST)

        model = request.get("model")
        if not isinstance(model, str) or not model:
            raise ShimError("model is required", HTTPStatus.BAD_REQUEST)

        temperature = request.get("temperature")
        if temperature is not None and not isinstance(temperature, (int, float)):
            raise ShimError("temperature must be a number", HTTPStatus.BAD_REQUEST)

        max_tokens = request.get("max_tokens")
        if max_tokens is not None and not isinstance(max_tokens, int):
            raise ShimError("max_tokens must be an integer", HTTPStatus.BAD_REQUEST)

        request_id = state.next_id()
        future = self._new_future(state)
        state.pending[request_id] = future

        frame: dict[str, object] = {
            "protocol_version": HOST_PROTOCOL_VERSION,
            "type": "model_request",
            "request_id": request_id,
            "payload": {
                "messages": normalized_messages,
                "model": model,
                "temperature": temperature,
                "max_output_tokens": max_tokens,
                "metadata": {},
            },
        }

        wire_out = state.wire_out
        assert wire_out is not None
        write_frame(wire_out, frame)

        response = self._wait_for_response(state, request_id, "model")

        stop_reason = response.get("stop_reason")
        finish_reason = "stop"
        if stop_reason == "length":
            finish_reason = "length"
        elif stop_reason == "error":
            finish_reason = "error"

        usage_raw = response.get("usage", {})
        usage = cast("dict[str, object]", usage_raw) if isinstance(usage_raw, dict) else {}
        completion: dict[str, object] = {
            "id": f"chatcmpl-{secrets.token_hex(12)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": response.get("output_text", "")},
                    "finish_reason": finish_reason,
                }
            ],
            "usage": {
                "prompt_tokens": usage.get("input_tokens", 0),
                "completion_tokens": usage.get("output_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
            },
        }
        self._send_json(HTTPStatus.OK, completion)

    def _handle_mcp(self, body: bytes) -> None:
        state = self._require_state()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as e:
            raise ShimError("invalid JSON", HTTPStatus.BAD_REQUEST) from e

        if not isinstance(parsed, dict):
            raise ShimError("request must be an object", HTTPStatus.BAD_REQUEST)
        request = cast("dict[str, object]", parsed)

        jsonrpc = request.get("jsonrpc")
        if jsonrpc != "2.0":
            raise ShimError("jsonrpc must be '2.0'", HTTPStatus.BAD_REQUEST)

        method = request.get("method")
        if not isinstance(method, str):
            raise ShimError("method must be a string", HTTPStatus.BAD_REQUEST)

        params_raw = request.get("params", {})
        if not isinstance(params_raw, dict):
            raise ShimError("params must be an object", HTTPStatus.BAD_REQUEST)
        params = cast("dict[str, object]", params_raw)

        request_id = request.get("id")
        if request_id is not None and not isinstance(request_id, (str, int)):
            raise ShimError("id must be string, number, or null", HTTPStatus.BAD_REQUEST)

        is_notification = request_id is None

        if method == "initialize":
            result: dict[str, object] = {
                "protocolVersion": "2026-07-28",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "nervos-shim", "version": "1.0.0"},
            }
            jsonrpc_result = {"jsonrpc": "2.0", "id": request_id, "result": result}
            if not is_notification:
                self._send_json(HTTPStatus.OK, jsonrpc_result)
            else:
                self.send_response(HTTPStatus.ACCEPTED)
                self.end_headers()
            return

        if method == "notifications/initialized":
            self.send_response(HTTPStatus.ACCEPTED)
            self.end_headers()
            return

        if method == "ping":
            if not is_notification:
                self._send_json(HTTPStatus.OK, {"jsonrpc": "2.0", "id": request_id, "result": {}})
            else:
                self.send_response(HTTPStatus.ACCEPTED)
                self.end_headers()
            return

        if method == "tools/list":
            tools_info = self._get_tools_info()
            tools_list: list[dict[str, object]] = [
                {
                    "name": tool.get("name"),
                    "description": tool.get("description", f"MCP tool {tool.get('name')}"),
                    "inputSchema": tool.get("input_schema", {"type": "object", "properties": {}}),
                }
                for tool in tools_info
            ]
            list_result = {"jsonrpc": "2.0", "id": request_id, "result": {"tools": tools_list}}
            if not is_notification:
                self._send_json(HTTPStatus.OK, list_result)
            else:
                self.send_response(HTTPStatus.ACCEPTED)
                self.end_headers()
            return

        if method == "tools/call":
            tool_name = params.get("name")
            arguments_raw = params.get("arguments", {})
            if not isinstance(tool_name, str) or not tool_name:
                raise ShimError("tool name is required", HTTPStatus.BAD_REQUEST)
            if not isinstance(arguments_raw, dict):
                raise ShimError("arguments must be an object", HTTPStatus.BAD_REQUEST)
            arguments = cast("dict[str, object]", arguments_raw)

            granted = self._get_granted_tools()
            if tool_name not in granted:
                error_result: dict[str, object] = {
                    "isError": True,
                    "content": [{"type": "text", "text": f"tool '{tool_name}' is not granted"}],
                }
                denied_result = {"jsonrpc": "2.0", "id": request_id, "result": error_result}
                if not is_notification:
                    self._send_json(HTTPStatus.OK, denied_result)
                else:
                    self.send_response(HTTPStatus.ACCEPTED)
                    self.end_headers()
                return

            shim_request_id = state.next_id()
            future = self._new_future(state)
            state.pending[shim_request_id] = future

            frame: dict[str, object] = {
                "protocol_version": HOST_PROTOCOL_VERSION,
                "type": "tool_request",
                "request_id": shim_request_id,
                "payload": {"name": tool_name, "arguments": arguments, "metadata": {}},
            }

            wire_out = state.wire_out
            assert wire_out is not None
            write_frame(wire_out, frame)

            response = self._wait_for_response(state, shim_request_id, "tool")

            is_error = response.get("is_error", False)
            content_raw = response.get("content", {})
            content: dict[str, object] = (
                cast("dict[str, object]", content_raw) if isinstance(content_raw, dict) else {}
            )
            text_value = content.get("text", "")
            text = text_value if isinstance(text_value, str) else str(text_value)

            call_result: dict[str, object] = {
                "isError": is_error,
                "content": [{"type": "text", "text": text}],
            }
            jsonrpc_call_result = {"jsonrpc": "2.0", "id": request_id, "result": call_result}
            if not is_notification:
                self._send_json(HTTPStatus.OK, jsonrpc_call_result)
            else:
                self.send_response(HTTPStatus.ACCEPTED)
                self.end_headers()
            return

        raise ShimError(f"unknown method: {method}", HTTPStatus.METHOD_NOT_ALLOWED)

    def _get_granted_tools(self) -> list[str]:
        state = self._require_state()
        granted: list[str] = []
        for tool in state.tools_info:
            name = tool.get("name")
            if isinstance(name, str):
                granted.append(name)
        return granted

    def _get_tools_info(self) -> list[dict[str, object]]:
        return self._require_state().tools_info


class ShimServer:
    def __init__(
        self,
        config: ShimConfig,
        wire_in: BinaryIO,
        wire_out: BinaryIO,
        message_handler: Callable[[dict[str, object]], Awaitable[None]],
    ) -> None:
        self._config = config
        self._wire_in = wire_in
        self._wire_out = wire_out
        self._message_handler = message_handler
        self._server: HTTPServer | None = None
        self._state = ShimState(config=config, wire_in=wire_in, wire_out=wire_out)
        self._reader_task: asyncio.Task[None] | None = None
        self._serve_future: asyncio.Future[None] | None = None
        _ShimRequestHandler.state = self._state

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("server not started")
        return self._server.server_port

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def state(self) -> ShimState:
        return self._state

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._state.loop = loop
        self._server = HTTPServer(("127.0.0.1", 0), _ShimRequestHandler)
        self._reader_task = asyncio.create_task(self._read_loop())
        # serve_forever blocks, so it runs on an executor thread; stop() unblocks it
        # via HTTPServer.shutdown().
        self._serve_future = loop.run_in_executor(None, self._server.serve_forever)

    async def _read_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._state.closed:
            try:
                message = await loop.run_in_executor(None, read_frame, self._wire_in)
            except (HostProtocolError, EOFError, OSError) as e:
                _LOGGER.debug("wire read ended: %s", e)
                break
            self.handle_wire_message(message)
            await self._message_handler(message)

    def handle_wire_message(self, message: dict[str, object]) -> None:
        msg_type = message.get("type")
        request_id = message.get("request_id")
        if (
            msg_type in ("model_response", "tool_response")
            and isinstance(request_id, str)
            and request_id in self._state.pending
        ):
            future = self._state.pending.pop(request_id)
            if not future.done():
                future.set_result(cast("dict[str, object]", message.get("payload", {})))
        elif msg_type == "initialize":
            payload_raw = message.get("payload", {})
            if isinstance(payload_raw, dict):
                payload = cast("dict[str, object]", payload_raw)
                tools = payload.get("tools")
                if isinstance(tools, list):
                    self._state.tools_info = cast("list[dict[str, object]]", tools)

    async def stop(self) -> None:
        self._state.closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._serve_future is not None:
            with contextlib.suppress(Exception):
                await self._serve_future
        _ShimRequestHandler.state = None


async def run_shim(
    wire_in: BinaryIO,
    wire_out: BinaryIO,
    *,
    token: str | None = None,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    max_requests: int = DEFAULT_MAX_REQUESTS,
) -> tuple[str, int]:
    """Run the shim server and return (token, port).

    This is a convenience function that starts the shim and blocks until the wire
    connection closes or an error occurs.
    """
    if token is None:
        token = secrets.token_urlsafe(32)

    config = ShimConfig(token=token, request_timeout=request_timeout, max_requests=max_requests)
    state = ShimState(config=config, wire_in=wire_in, wire_out=wire_out)
    state.loop = asyncio.get_running_loop()
    _ShimRequestHandler.state = state

    server = HTTPServer(("127.0.0.1", 0), _ShimRequestHandler)
    port = server.server_port

    async def reader_loop() -> None:
        loop = asyncio.get_running_loop()
        while not state.closed:
            try:
                message = await loop.run_in_executor(None, read_frame, wire_in)
            except (HostProtocolError, EOFError, OSError):
                break
            msg_type = message.get("type")
            request_id = message.get("request_id")
            if (
                msg_type in ("model_response", "tool_response")
                and isinstance(request_id, str)
                and request_id in state.pending
            ):
                future = state.pending.pop(request_id)
                if not future.done():
                    future.set_result(cast("dict[str, object]", message.get("payload", {})))

    reader_task = asyncio.create_task(reader_loop())
    try:
        await asyncio.get_running_loop().run_in_executor(None, server.serve_forever)
    finally:
        state.closed = True
        reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reader_task
        server.server_close()
        _ShimRequestHandler.state = None

    return token, port
