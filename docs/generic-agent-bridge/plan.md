# Generic Process Agent Bridge — Design Plan

**Status: PARTIAL** — Core milestones completed; missing items listed below.

**ADR 0042 update (function calling + streaming)**: the shim's
`/v1/chat/completions` now supports OpenAI `tools`, `tool_choice` (auto/none),
assistant `tool_calls`, `role: tool` messages, and `stream: true` replayed as SSE,
with the Worker mapping onto the core `ModelRequest`/`ModelResponse` tool fields and
the initialize frame listing the run's real granted tools. See
`docs/adr/0042-bridge-model-tools-and-streaming.md`.

**What remains**:
- Linux verification run by the owner (`scripts/verify_linux_bridge.sh`,
  `docs/linux-verification-runbook.md`): loopback in bubblewrap stays `[unverified]`
  until then
- Real streaming (incremental generation; today every route replays one complete
  result)
- A per-run model allowlist for the discovery routes (they list the last-used model
  or `nervos-model`)
- Process-kind packages through the real install path: the durable definition model
  requires a `module:symbol` entrypoint (`infrastructure/database/packages.py:133`),
  so install refuses process manifests until that changes (Phase A2 stop)
- Anything else the owner's Linux run surfaces

**ADR 0045 update (tool choice + tool-aware history)**: forced `tool_choice`
(required/named) and ordered multi-turn conversations now flow core → adapters →
wire → Worker → shim on every route; the ADR 0042 refusals are gone. See
`docs/adr/0045-bridge-tool-choice-and-tool-history.md`.

**ADR 0043 update (item 3)**: resolved by "bundle ships its runtime" — a Node package
carries `bin/node` + `node_modules` as assets and launches via
`command: ["bin/node", "app.js"]`.

**Verified with real SDKs**: see the tables in `docs/implementation-status.md`
(Python 14/14; Node 15/15 across openai 7.43.0, @anthropic-ai/sdk 1.3.0,
@google/genai 2.31.0, ai 7.0.153).
