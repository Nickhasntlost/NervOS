# Generic Process Agent Bridge — Design Plan

**Status: PARTIAL** — Core milestones completed; missing items listed below.

**ADR 0042 update (function calling + streaming)**: the shim's
`/v1/chat/completions` now supports OpenAI `tools`, `tool_choice` (auto/none),
assistant `tool_calls`, `role: tool` messages, and `stream: true` replayed as SSE,
with the Worker mapping onto the core `ModelRequest`/`ModelResponse` tool fields and
the initialize frame listing the run's real granted tools. See
`docs/adr/0042-bridge-model-tools-and-streaming.md`.

**What remains**:
- Anthropic-compatible, OpenAI Responses, and Gemini endpoints (ADR 0042 Part 2)
- Bundling examples/package templates for foreign SDKs
- Linux loopback verification inside bubblewrap (`[unverified]` as in ADR 0041)
- Core representability for a tool exchange followed by a later user message
  (currently refused with `model_request_rejected`; needs a core ADR)

---

## Problem Statement

Today, NervOS runs agents only through the `nervos-sdk` Python entrypoint (`module.path:Symbol`) inside a sandboxed Python subprocess. The Worker mediates all model and tool access via a framed JSON wire protocol over stdin/stdout.

This design enables **any agent built on any SDK/language** to run as a sandboxed subprocess, reaching models and tools **only through NervOS**, without importing `nervos-sdk`. The target is Design A: a loopback shim in the package host translates OpenAI-compatible/MCP HTTP requests to the existing wire protocol frames over stdio.

---

## Milestones

### M0 — Architecture Freeze & ADR (this document + ADR 0041) ✅ COMPLETED
- Finalize transport, manifest, credentials, and security boundaries
- Answer all six design questions with evidence
- External review acceptance required before M1
- **ADR 0041 Status: ACCEPTED** with Owner decisions recorded

### M1 — Manifest Extensions & Wire Protocol Capability Flags ✅ COMPLETED
- Add `runtime.kind` (values: `python` [default], `process`) and `runtime.command` for process kind
- Relax `runtime.language` / `runtime.python` constraints — required only for `python` kind
- Add capability flag `generic-process-bridge-v1` to `host_hello` wire frame
- **Files**: `packages/nervos-core/src/nervos_core/application/package_manifest.py`, `packages/nervos-core/src/nervos_core/domain/packages.py`, `packages/nervos-package-host/src/nervos_package_host/wire.py`
- **Acceptance**: Manifest parser accepts `process` kind with `command`; wire frames advertise `generic-process-bridge-v1`

### M2 — Loopback Shim in Package Host + Linux-Only Test ✅ COMPLETED
- Package host starts loopback HTTP server on 127.0.0.1 inside sandbox serving:
  - OpenAI-compatible `/v1/chat/completions` → translates to `model_request`/`model_response` wire frames over stdio
  - MCP endpoint (`/mcp`) → translates to `tool_request`/`tool_response` wire frames over stdio
- Foreign agent runs as child in same sandbox, base URL = `http://127.0.0.1:<port>`, per-run random API key via `OPENAI_API_KEY` env
- **Linux-only test**: asserts direct outbound network call fails inside bubblewrap; skipped on non-Linux with reason "requires bubblewrap; loopback in namespace unverified"
- **Files**: `packages/nervos-package-host/src/nervos_package_host/bridge_shim.py`, `packages/nervos-package-host/src/nervos_package_host/__main__.py`, `packages/nervos-package-host/tests/test_linux_bubblewrap_loopback.py`
- **Acceptance**: Shim handles auth, Host header check, size caps, all error mappings, MCP methods, denied tool

### M3 — Tool Shim & Permission Enforcement ✅ COMPLETED
- MCP endpoint in shim translates tool calls to wire protocol `tool_request`/`tool_response`
- Permission checks, approvals, audit remain in Worker (`ToolInvocationMediator`)
- Ungranted tool → MCP result with `isError=true` (never crashes)
- `tools/list` returns real granted tools with descriptions/schemas from Worker's D2 grants via `list_grants_at_or_before_cutoff`
- **Files**: `packages/nervos-package-host/src/nervos_package_host/bridge_shim.py` (`_handle_mcp` method), `apps/worker/src/nervos_worker/package_execution.py:246-284`
- **Acceptance**: Granted tool via MCP succeeds; ungranted tool denied with `isError=true`; audit row created in Worker

### M4 — Credential Isolation & Per-Run Random API Keys ✅ COMPLETED
- Confirm no real provider keys enter sandbox (`package_host_environment()` filters to allowlist)
- Per-run random API key replaces fixed "nervos-proxy" placeholder
- **Files**: `packages/nervos-package-host/src/nervos_package_host/bridge_shim.py`, `packages/nervos-package-host/src/nervos_package_host/__main__.py`, `apps/worker/src/nervos_worker/package_execution.py`
- **Acceptance**: Sandbox process sees only per-run random token; real keys never in sandbox env

### M5 — Test Suite & Qualification ✅ COMPLETED
- **Shim unit tests**: Auth, Host check, size caps, wire protocol roundtrips, error mapping, MCP methods, denied tool
- **E2E test**: Fake foreign agent script using only stdlib, validates wire protocol frames with tools
- **Linux-only test**: Asserts direct network call fails inside bubblewrap; skipped on Windows
- **Files**: `packages/nervos-package-host/tests/test_bridge_shim.py`, `packages/nervos-package-host/tests/test_process_agent_e2e.py`, `packages/nervos-package-host/tests/test_linux_bubblewrap_loopback.py`, `apps/worker/tests/integration/test_process_agent_tools_list.py`
- **Acceptance**: All tests pass; no regressions in existing package-host/Worker tests; tools/list returns real granted tools

### M6 — Documentation & Examples ✅ COMPLETED
- Update ADR 0041 (Status: ACCEPTED, Owner decisions)
- Update plan.md (this file — milestones marked complete)
- Update `docs/implementation-status.md` with summary
- **No examples/ directory created** — none existed previously

---

## Acceptance Criteria Summary

| Milestone | Key Acceptance Criteria | Status |
|-----------|------------------------|--------|
| M0 | ADR 0041 accepted; six questions answered with file/line evidence | ✅ |
| M1 | Manifest accepts `runtime.kind` + `runtime.command`; wire protocol advertises `generic-process-bridge-v1` | ✅ |
| M2 | Shim handles auth, Host check, size caps, all error mappings, MCP methods, denied tool | ✅ |
| M3 | Granted tool via MCP works; ungranted tool denied with `isError=true`; audit row created | ⚠️ `tools/list` returns tools from initialize frame; Worker's D2 grants not yet passed to host |
| M4 | Sandbox env has no real keys; per-run random API key convention defined | ✅ |
| M5 | All tests pass (shim unit, e2e, Linux-only); no regressions | ⚠️ No full E2E with real model + tool call through complete stack |
| M6 | ADR 0041 updated; plan.md updated; implementation-status.md updated | ✅ |

---

## Risks & Open Decisions (for Human Review)

1. **Loopback in bubblewrap namespace** — `[unverified]` Whether 127.0.0.1 works inside `--unshare-all`; Linux-only test is mandatory. If it fails, Design A is rejected and stdio-only wire client is required.

2. **Windows product behavior** — **DECIDED**: Package execution stays refused unless the existing env flag `NERVOS_ALLOW_WINDOWS_SANDBOX` allows it; do not weaken any containment.

3. **Streaming model responses** — **DEFERRED**: Current `ModelResult` is non-streaming. Adding streaming requires wire protocol v2 and `ModelPort` interface change.

4. **Multimodal / images** — **DEFERRED**: `ModelMessage.content: str` cannot carry images. Requires SDK type change.

5. **Manifest `runtime` schema migration** — **COMPLETED**: `runtime.kind` defaults to `python`; existing packages unchanged. `process` kind requires `command` and forbids python fields.

6. **Capability negotiation** — **COMPLETED**: `host_hello` advertises `generic-process-bridge-v1` capability flag.

7. **Resource limits for non-Python agents** — Current rlimits (AS, NOFILE, NPROC, CPU, FSIZE) are Python-centric. May need per-language profiles in future.

8. **macOS** — No containment adapter; `create_containment()` raises `ContainmentUnavailable`. Generic bridge inherits this: macOS refuses.

---

## Architecture Diagram (Text) — Design A

```
┌─────────────────────────────────────────────────────────────────┐
│                        WORKER PROCESS                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Model        │  │ Tool         │  │ Wire Protocol        │  │
│  │ Completion   │  │ Mediator     │  │ Server (stdio)       │  │
│  │ (providers)  │  │ (grants,     │  │                      │  │
│  │              │  │  audit)      │  │  model_request       │  │
│  └──────┬───────┘  └──────┬───────┘  │  model_response      │  │
│         │                 │          │  tool_request        │  │
│         └────────┬────────┘          │  tool_response       │  │
│                  │                   │  run_request         │  │
│                  ▼                   │  run_result          │  │
│         ┌────────────────────┐       └──────────┬───────────┘  │
│         │ Containment        │                  │             │
│         │ (bubblewrap)       │                  ▼             │
│         └────────┬───────────┘       ┌────────────────────┐  │
│                  │                   │  SANDBOXED         │  │
│                  ▼                   │  PACKAGE HOST      │  │
│         ┌────────────────────┐       │  ┌──────────────┐  │  │
│         │ Package Host       │       │  │ Loopback     │  │  │
│         │ Subprocess         │       │  │ Shim         │  │  │
│         │ (nervos_package_   │       │  │ (HTTP on     │  │  │
│         │  host)             │       │  │  127.0.0.1)  │  │  │
│         └────────────────────┘       │  └──────┬───────┘  │  │
│                                      │         │          │  │
│                                      ▼         ▼          │  │
│                             ┌─────────────────────┐     │  │
│                             │  FOREIGN AGENT      │     │  │
│                             │  (OpenAI SDK,       │     │  │
│                             │   MCP client,       │     │  │
│                             │   any language)     │     │  │
│                             └─────────────────────┘     │  │
│                                      │                  │  │
│                                      ▼                  │  │
│                             ┌─────────────────────┐     │  │
│                             │  ENV:               │     │  │
│                             │  OPENAI_BASE_URL=   │     │  │
│                             │  http://127.0.0.1   │     │  │
│                             │  OPENAI_API_KEY=    │     │  │
│                             │  <per-run-random>   │     │  │
│                             └─────────────────────┘     │  │
└─────────────────────────────────────────────────────────────────┘
```

---

## File/Line Evidence Index

| Question | Answer | Evidence |
|----------|--------|----------|
| 1. Loopback HTTP in bwrap? | **[unverified]** — `--unshare-all` at `linux_bubblewrap.py:79-80`; Linux-only test added | `packages/nervos-core/src/nervos_core/infrastructure/sandbox/linux_bubblewrap.py:79-80` |
| 2. Model proxy gaps | Streaming, images, tool-call streaming missing (deferred) | `packages/nervos-sdk/src/nervos_sdk/types.py:54,73-82` |
| 3. Tool exposure | Wire protocol `tool_request`/`tool_response` exists at `wire.py:36-37`; handled at `package_execution.py:314` | `packages/nervos-package-host/src/nervos_package_host/wire.py:36-37`, `apps/worker/src/nervos_worker/package_execution.py:314` |
| 4. Manifest changes | `runtime.kind` (python/process), `runtime.command` for process | `packages/nervos-core/src/nervos_core/application/package_manifest.py:261-267`, `packages/nervos-core/src/nervos_core/domain/packages.py:36-52` |
| 5. Credentials in sandbox | **Never** — `package_host_environment()` filters to allowlist; per-run random key | `apps/worker/src/nervos_worker/package_execution.py:759-775` |
| 6. Test plan | Shim unit tests, e2e test, Linux-only test | `packages/nervos-package-host/tests/test_bridge_shim.py`, `packages/nervos-package-host/tests/test_process_agent_e2e.py`, `packages/nervos-package-host/tests/test_linux_bubblewrap_loopback.py` |

---

## Dependencies on Existing Stages

- **Stage G** (complete): Package manifest, package-host wire protocol, Worker package execution adapter
- **Stage H** (qualified on Linux): Bubblewrap containment with `--unshare-all`, resource limits
- **Stage D** (complete): Tool mediation, permission grants, audit
- **Stage F** (complete): Context snapshots, memory — generic agent receives frozen snapshot via `run_request`

---

## Out of Scope

- Streaming model responses (requires SDK + wire protocol v2)
- Multimodal inputs (images, audio)
- Windows/macOS package execution with enforced isolation (Stage H4 qualification required)
- Remote/distributed Workers
- Agent-to-agent direct communication
- Package-hosted MCP servers (Worker owns MCP gateway)
- `x-nervos-generic-bridge` extension (removed; use capability flags in `host_hello`)
- VSOCK / Unix domain sockets (removed; Design A uses loopback)
- `host.docker.internal` (removed; not applicable)
- Node example (removed; only Python reference example in scope)