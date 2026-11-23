# Integrations

How to get the logs of each popular coding agent, agent framework, gateway, cloud platform and
telemetry pipeline into Logpak.

**OpenTelemetry is the main interface.** Point the OTLP exporter your tool, framework or gateway
already has at `logpak collect`. With `--forward`, every batch is also passed on, as received, to
the backend you use today (Langfuse, Phoenix, Datadog, an OpenTelemetry Collector). Nothing sits
in the path of your model calls, and turning Logpak off is one environment variable.

| Way in | Command | Use it when |
|---|---|---|
| **A. OpenTelemetry** (main) | `logpak collect --record-dir DIR [--forward URL]` | the tool exports OTLP with the messages: most frameworks and gateways, and the coding agents marked so below |
| **B. Recording proxy** (fallback) | `logpak server --upstream URL --record-dir DIR` | the tool exports no content over OTel, or you need the exact API request bytes |
| **C. Pack a file** | `logpak log pack FILE.lpk < lines.jsonl` | logs that already land in files or buckets (Bedrock, Vertex, exports) |

`collect` also takes plain JSON log POSTs on any path, which is how gateways with an HTTP log
plugin (Kong, LiteLLM, APISIX) send their payload logs.

**What each tool's OTel export carries.** Each section opens with one of these labels:

- **full request:** the API request body itself, conversation history included.
- **messages:** the input and output messages on each span or log record, once content capture is
  on. Each record still repeats the conversation, so this is what Logpak compresses best.
- **prompts only:** the user's prompt and the reply, not the history sent with them.
- **metadata only:** model, tokens, latency and cost. No content.
- **no OTel.**

For **full request** and **messages**, use A. For the rest, use B.

**Sources.** Keys and flags below were read from each project's official documentation in
November 2026, and each section links its pages. Settings marked *(unverified)* were not
confirmed by the docs. Check those against the linked page before you rely on them. Logpak's
CI runs only LiteLLM, Kong and Portkey ([tests/gateways/](../tests/gateways/test_gateways.py)).
Langfuse and the OpenTelemetry Collector were run end to end once, by hand (the README's
"Measured end to end"). The rest have not been run end to end.

## Contents

- [At a glance](#at-a-glance)
- [Rules that apply everywhere](#rules-that-apply-everywhere)
- [Coding agents](#coding-agents): Claude Code, Claude Agent SDK, Gemini CLI, Qwen Code, GitHub
  Copilot CLI, VS Code Copilot, Goose, Codex CLI, OpenHands, Kilo Code, Cline, Roo Code, OpenCode,
  Aider, Continue, SWE-agent, mini-swe-agent, Crush, Pi, Zed
- [Personal and autonomous agents](#personal-and-autonomous-agents): OpenClaw, Hermes Agent, nanobot,
  Open Interpreter, Letta, Agent Zero, OpenManus, AutoGPT
- [Agent workflow platforms](#agent-workflow-platforms): n8n, Dify, Langflow, Flowise, Open WebUI,
  LibreChat, AnythingLLM, Activepieces, Rivet
- [SDKs and agent frameworks](#sdks-and-agent-frameworks): OpenTelemetry GenAI, OpenLLMetry,
  OpenInference, Langfuse, LangSmith, Logfire and Pydantic AI, LangChain, LlamaIndex, OpenAI Agents
  SDK, Vercel AI SDK, Mastra, CrewAI, AutoGen, Microsoft Agent Framework, Semantic Kernel, Spring
  AI, Strands, Google ADK, smolagents, Haystack, DSPy, Agno, LangChain4j, Genkit, and provider SDKs
- [LLM gateways](#llm-gateways): LiteLLM, Kong, Portkey, Apache APISIX, Bifrost, TensorZero,
  agentgateway, Envoy AI Gateway, Higress, Traefik Hub, OpenRouter, Cloudflare AI Gateway,
  Helicone, Vercel AI Gateway, MLflow AI Gateway
- [Cloud platform logs](#cloud-platform-logs): AWS Bedrock, Azure API Management, Google Vertex AI
- [Telemetry pipelines](#telemetry-pipelines): OpenTelemetry Collector, Grafana Alloy, Fluent Bit,
  Vector, Logstash, Filebeat
- [Forwarding to an observability backend](#forwarding-to-an-observability-backend)
- [Self-hosted inference servers](#self-hosted-inference-servers)
- [Provider upstreams](#provider-upstreams)
- [Not supported](#not-supported)

## At a glance

| Tool | Over OTel | Route |
|---|---|---|
| Claude Code, Claude Agent SDK | full request | A |
| Gemini CLI, Qwen Code, GitHub Copilot CLI, VS Code Copilot, Goose | messages | A |
| OpenClaw, Hermes Agent (through NeMo Relay) | messages, once content capture is on | A |
| Dify, Langflow and Flowise (through their Phoenix integration), n8n (Agents feature) | messages | A |
| OTel GenAI, OpenLLMetry and OpenInference instrumentors; Langfuse, LangSmith, Logfire, Pydantic AI, LangChain, LlamaIndex, OpenAI Agents SDK (Python), Vercel AI SDK, Mastra, CrewAI, Microsoft Agent Framework, Semantic Kernel, Spring AI, Strands, Google ADK, smolagents, Haystack, DSPy, Agno | messages, once content capture is on | A |
| LiteLLM, Kong, APISIX, Bifrost, Envoy AI Gateway, Traefik Hub, agentgateway, OpenRouter, Cloudflare | messages or payload logs | A |
| Codex CLI, Open Interpreter | prompts only | B (A for prompts and replies) |
| OpenHands, Kilo Code, Letta, TensorZero, AutoGen | content *(unverified)* | A, B if prompts are missing |
| Cline, Open WebUI, LibreChat | metadata only | B |
| n8n (AI Agent node), AnythingLLM, Activepieces, Rivet | no OTel with content | B |
| Roo Code, OpenCode, Aider, Continue, SWE-agent, mini-swe-agent, Crush, Pi, Zed, nanobot, Agent Zero, OpenManus, AutoGPT, OpenAI Agents SDK (JS), LangChain4j, Genkit | no OTel with content | B |
| Portkey (open source), Helicone, Vercel AI Gateway, MLflow AI Gateway, Higress | no payload export | B |
| AWS Bedrock, Azure API Management, Vertex AI | provider logs | C |

## Rules that apply everywhere

**The collector (A).** Start one collector, then point exporters at `http://127.0.0.1:4318`
(OTLP/HTTP, protobuf or JSON) or `http://127.0.0.1:4317` (OTLP/gRPC without TLS).

```bash
logpak collect --record-dir ~/logpak/otel
```

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

To keep your current backend, add `--forward` with its OTLP/HTTP base URL, and leave its auth in
`OTEL_EXPORTER_OTLP_HEADERS`. Headers are forwarded and never stored. The URL and header for each
backend are in [Forwarding](#forwarding-to-an-observability-backend).

```bash
logpak collect --record-dir ~/logpak/otel --forward https://cloud.langfuse.com/api/public/otel
```

**Turn content capture on.** Most tracers leave prompts out by default. Each section names the
flag. Some tools put the content on log records rather than spans. Those also need
`OTEL_LOGS_EXPORTER=otlp`, and `collect` stores the logs on `/v1/logs` like the rest.

**Raise attribute limits.** A long agent conversation is megabytes. Where a tool truncates content
(Claude Code at 60 KB, Qwen Code at 1 MiB, VS Code Copilot by `maxAttributeSizeChars`), the
section says how to raise it.

**The base-URL rule (B).** Start `logpak server` with the provider's origin as `--upstream`.
Then take the base URL the tool uses today, swap the origin for `http://127.0.0.1:8080`, and keep
the path. `https://api.openai.com/v1` becomes `http://127.0.0.1:8080/v1`, and
`https://api.anthropic.com` becomes `http://127.0.0.1:8080`. The server appends the request's path
to the upstream's, and passes your API key headers through untouched.

```bash
logpak server --listen 127.0.0.1:8080 --upstream https://api.anthropic.com --record-dir ~/logpak/anthropic
```

```bash
logpak server --listen 127.0.0.1:8081 --upstream https://api.openai.com --record-dir ~/logpak/openai
```

Run one server per provider, each on its own port and directory. The proxy speaks plain HTTP,
including streamed (SSE) responses. It does not speak WebSocket. The default `--listen` is
`0.0.0.0:8080`, so pass `127.0.0.1` on a laptop.

**Any common compression works.** `collect` and `server --record-dir` decode gzip, deflate, brotli,
zstd and snappy before they store a body, and gRPC messages by their `grpc-encoding`. A coding
outside that list (lz4, for one) is stored as sent, which leaves Logpak nothing to delta.
`logpak log ls` names the encoding of any record still compressed.

**Size limits.** `collect` and `server` take bodies up to 64 MiB (`--max-body`). Many senders cap
lower and truncate or reject long agent prompts first. Each section notes the limits that matter.

**SaaS senders need a reachable endpoint.** OpenRouter, Cloudflare and Vercel export from their own
servers. Run `collect` on a public host behind a TLS proxy, not on loopback.

**Which logs gain most.** The 100× ratios come from logs that re-send the whole conversation in
every record: API request bodies, gateway payload logs, and spans that carry the full input
messages. A transcript that stores each message once (Claude Code's `~/.claude/projects`, for
example) has little to deduplicate, and packs about as well as zstd.

---

## Coding agents

### Claude Code

Over OTel: **full request**. Claude Code is the one coding agent that exports the whole Messages
API body.

**A.** Export logs to the collector, with raw API bodies on:

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_LOGS_EXPORTER=otlp OTEL_METRICS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_LOG_RAW_API_BODIES=1            # the full request and response JSON, as log events
export CLAUDE_CODE_OTEL_CONTENT_MAX_LENGTH=67108864   # inline bodies are cut at 60 KB otherwise
```

```bash
claude
```

- To make it permanent, put the same variables in the `env` block of `~/.claude/settings.json`.
- Traces (beta) also need `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1` and `OTEL_TRACES_EXPORTER=otlp`.
- Narrower flags exist for prompts, tool arguments and tool output: `OTEL_LOG_USER_PROMPTS=1`,
  `OTEL_LOG_TOOL_DETAILS=1` and `OTEL_LOG_TOOL_CONTENT=1`.
- `OTEL_LOG_RAW_API_BODIES=file:<dir>` writes each body untruncated to its own file instead, and
  puts a `body_ref` path in the event.
- Extended thinking is redacted from raw bodies.

**B.** For the request bytes exactly as Anthropic received them. No `/v1` in the base URL.

```bash
logpak server --listen 127.0.0.1:8080 --upstream https://api.anthropic.com --record-dir ~/logpak/claude
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude
```

- In the VS Code extension, set it in `claudeCode.environmentVariables`.
- Both API keys (`x-api-key`) and a claude.ai subscription login (`Authorization: Bearer`) work,
  because the proxy forwards those headers and `anthropic-beta` verbatim.
- Remote Control is disabled while the base URL is not an Anthropic host. The desktop app does not
  read `ANTHROPIC_BASE_URL`.

**C.** `~/.claude/projects/<project>/<session-id>.jsonl` holds one message per line, kept for 30
days (`cleanupPeriodDays`). It packs, but each message is stored only once, so expect zstd-like
ratios. The format is internal to Claude Code.

```bash
cat ~/.claude/projects/*/*.jsonl | logpak log pack claude-transcripts.lpk
```

Docs: [monitoring](https://code.claude.com/docs/en/monitoring-usage),
[LLM gateway](https://code.claude.com/docs/en/llm-gateway-connect).

### Claude Agent SDK

Over OTel: **full request**. The SDK runs the Claude Code CLI, so the Claude Code variables above
apply. Pass them in `env`:

```python
from claude_agent_sdk import ClaudeAgentOptions
otel = {"CLAUDE_CODE_ENABLE_TELEMETRY": "1", "OTEL_LOGS_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        "OTEL_LOG_RAW_API_BODIES": "1", "CLAUDE_CODE_OTEL_CONTENT_MAX_LENGTH": "67108864",
        "OTEL_LOGS_EXPORT_INTERVAL": "1000"}
options = ClaudeAgentOptions(env=otel)   # merged onto os.environ
```

```ts
const otel = { CLAUDE_CODE_ENABLE_TELEMETRY: "1", OTEL_LOGS_EXPORTER: "otlp",
  OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf", OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318",
  OTEL_LOG_RAW_API_BODIES: "1", CLAUDE_CODE_OTEL_CONTENT_MAX_LENGTH: "67108864", OTEL_LOGS_EXPORT_INTERVAL: "1000" };
const options = { env: { ...process.env, ...otel } }; // env replaces the environment: spread it
```

- Don't use the `console` exporter: stdout is the SDK's message channel.
- Short runs exit before the default export interval, hence `OTEL_LOGS_EXPORT_INTERVAL=1000`.
- **B:** add `ANTHROPIC_BASE_URL: "http://127.0.0.1:8080"` to the same `env`.

Docs: [Agent SDK observability](https://code.claude.com/docs/en/agent-sdk/observability).

### Gemini CLI

Over OTel: **messages**, in `gen_ai.input.messages` and `gen_ai.output.messages`.

**A.** It exports gRPC to `:4317` by default, and prompts are on by default. In
`.gemini/settings.json`:

```json
{ "telemetry": { "enabled": true, "target": "local", "otlpEndpoint": "http://127.0.0.1:4317",
                 "otlpProtocol": "grpc", "logPrompts": true, "traces": true } }
```

For `:4318`, set `"otlpProtocol": "http"`. The same settings exist as `GEMINI_TELEMETRY_*`
environment variables.

**B.** This only works with API-key auth (`gemini-api-key`). Login with Google goes elsewhere.

```bash
logpak server --listen 127.0.0.1:8082 --upstream https://generativelanguage.googleapis.com --record-dir ~/logpak/gemini
```

```bash
GOOGLE_GEMINI_BASE_URL=http://127.0.0.1:8082 gemini
```

For Vertex AI auth, the variable is `GOOGLE_VERTEX_BASE_URL`.

Docs: [telemetry](https://github.com/google-gemini/gemini-cli/blob/main/docs/cli/telemetry.md).

### Qwen Code

Over OTel: **messages**, once sensitive span attributes are on.

**A.** A Gemini CLI fork with the same `telemetry` block, defaulting to gRPC. In
`~/.qwen/settings.json`:

```json
{ "telemetry": { "enabled": true, "otlpEndpoint": "http://127.0.0.1:4317", "otlpProtocol": "grpc",
                 "logPrompts": true, "includeSensitiveSpanAttributes": true,
                 "sensitiveSpanAttributeMaxLength": 67108864 } }
```

`sensitiveSpanAttributeMaxLength` caps content at 1 MiB unless raised. The same settings exist as
`QWEN_TELEMETRY_*` environment variables.

**B.** `envKey` names the variable that holds the key. Qwen OAuth models cannot be redirected.

```json
{ "modelProviders": {
    "openai":    [{ "id": "<model>", "envKey": "OPENAI_API_KEY",    "baseUrl": "http://127.0.0.1:8081/v1" }],
    "anthropic": [{ "id": "<model>", "envKey": "ANTHROPIC_API_KEY", "baseUrl": "http://127.0.0.1:8080/v1" }] } }
```

Docs: [settings](https://qwenlm.github.io/qwen-code-docs/en/users/configuration/settings/).

### GitHub Copilot CLI

Over OTel: **messages**, with input and output messages, system instructions, and tool arguments
and results.

**A.** OTLP/HTTP only, no gRPC. Content capture is opt-in.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT=SPAN_ONLY
```

The reference says export is disabled for a plain `http://` endpoint *(unverified)*. If nothing
arrives, put `collect` behind a TLS proxy, or write to a file and pack it:

```bash
COPILOT_OTEL_FILE_EXPORTER_PATH=~/copilot-otel.jsonl copilot
```

```bash
logpak log pack copilot.lpk < ~/copilot-otel.jsonl
```

**B.** Only bring-your-own-key models go through the proxy. In `~/.copilot/providers.json`:

```json
{ "providers": [
    { "name": "lp-openai", "type": "openai", "baseUrl": "http://127.0.0.1:8081/v1", "apiKey": "KEY", "wireApi": "responses" },
    { "name": "lp-anthropic", "type": "anthropic", "baseUrl": "http://127.0.0.1:8080", "apiKey": "KEY" } ],
  "models": [ { "id": "my-model", "provider": "lp-openai", "wireModel": "<provider model name>" } ] }
```

```bash
copilot --model lp-openai/my-model
```

`apiKey` is taken literally; `$VAR` is not expanded.

Docs: [OpenTelemetry reference](https://docs.github.com/en/copilot/reference/enterprise-administrators/opentelemetry-reference),
[BYOK](https://docs.github.com/en/copilot/how-tos/copilot-cli/customize-copilot/use-byok-models).

### VS Code Copilot Chat

Over OTel: **messages**, with full prompts, system prompts, tool schemas, tool arguments and
results.

**A.** In VS Code settings:

```json
{ "github.copilot.chat.otel.enabled": true,
  "github.copilot.chat.otel.exporterType": "otlp-http",
  "github.copilot.chat.otel.otlpEndpoint": "http://127.0.0.1:4318",
  "github.copilot.chat.otel.captureContent": true,
  "github.copilot.chat.otel.maxAttributeSizeChars": 0 }
```

`maxAttributeSizeChars: 0` turns truncation off.

**B.** Run "Chat: Manage Language Models", then Add Models and choose Custom Endpoint. Each model
takes a full URL and an `apiType` (`chat-completions`, `responses` or `messages`), for example
`http://127.0.0.1:8081/v1/chat/completions`. Whether localhost URLs are accepted is *(unverified)*,
and an organisation policy can disable BYOK.

Docs: [monitoring agents](https://code.visualstudio.com/docs/copilot/guides/monitoring-agents),
[language models](https://code.visualstudio.com/docs/copilot/customization/language-models).

### Goose

Over OTel: **messages**, once message content capture is on.

**A.** Goose reads the standard variables and appends `/v1/traces` itself:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true
```

**B.** Use the bare origin with no path: Goose adds `v1/chat/completions` itself (`OPENAI_BASE_PATH`).

```bash
OPENAI_HOST=http://127.0.0.1:8081 goose session
```

```bash
ANTHROPIC_HOST=http://127.0.0.1:8080 goose session
```

**C.** `~/.local/state/goose/logs/llm_request.*.jsonl` holds the 10 most recent requests in full:
the input payload, response and usage. Pack them before they rotate away:

```bash
cat ~/.local/state/goose/logs/llm_request.*.jsonl | logpak log pack goose.lpk
```

Docs: [environment variables](https://goose-docs.ai/docs/guides/environment-variables),
[logs](https://goose-docs.ai/docs/guides/logs).

### OpenAI Codex CLI

Over OTel: **prompts only**. You get `codex.user_prompt`, `codex.agent_response` and tool-result
snippets, not the history sent with them.

**B (full context).** Only the user-level `~/.codex/config.toml` is read for providers. A project's
`.codex/config.toml` is ignored for these keys.

```toml
model_provider = "logpak"

[model_providers.logpak]
name = "logpak"
base_url = "http://127.0.0.1:8081/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
```

```bash
logpak server --listen 127.0.0.1:8081 --upstream https://api.openai.com --record-dir ~/logpak/codex
```

- Leave `supports_websockets` unset or false on this provider. The Responses WebSocket transport
  would not pass through the proxy.
- `openai_base_url = "http://127.0.0.1:8081/v1"` redirects the built-in provider instead. Whether
  that path also tries WebSocket, or honours ChatGPT-subscription login, is *(unverified)*. Prefer
  the custom provider with an API key.

**A (prompts and replies).**

```toml
[otel]
log_user_prompt = true
log_agent_responses = true
exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/logs", protocol = "binary" } }
```

The inline-table layout is assembled from the reference's keys *(unverified)*. Compare it with the
config-advanced example.

**C.** Session rollouts in `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` (one event per line)
*(unverified: not in the official docs; newer versions may write `.jsonl.zst`, which must be
decompressed first)*.

Docs: [config-advanced](https://learn.chatgpt.com/docs/config-file/config-advanced),
[config-reference](https://learn.chatgpt.com/docs/config-file/config-reference).

### OpenHands

Over OTel: agent steps, tool input and output, and LLM calls; whether the messages are included
is *(unverified)*.

**A.** In the Docker web app, pass these through `OH_AGENT_SERVER_ENV`; the app container does not
hand them to the agent.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
```

**B.** If the spans lack prompts. The CLI needs `--override-with-envs` for these variables.

```bash
LLM_BASE_URL=http://127.0.0.1:8081/v1 LLM_MODEL=openai/<model> LLM_API_KEY=$OPENAI_API_KEY openhands --override-with-envs
```

In the GUI, the setting is Settings, LLM, Advanced, Base URL.

Docs: [observability](https://docs.openhands.dev/sdk/guides/observability),
[CLI](https://docs.openhands.dev/openhands/usage/cli/command-reference).

### Kilo Code

Over OTel (CLI): traces and logs; whether prompts are included is *(unverified)*.

**A.** The CLI exports OTLP/HTTP whenever the endpoint is set:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
```

**B.** In the VS Code extension, choose Settings, Providers, Custom provider, and set "Base URL" to
`http://127.0.0.1:8081/v1`. The CLI's base-URL key under `provider.<id>.options` in
`~/.config/kilo/kilo.json` is *(unverified)*.

Docs: [OpenAI-compatible](https://kilo.ai/docs/ai-providers/openai-compatible),
[CLI](https://kilo.ai/docs/code-with-ai/platforms/cli).

### Cline and Roo Code

Over OTel: **metadata only** for Cline (`CLINE_OTEL_*` exports token counts and costs). **No OTel**
for Roo Code.

**B.** Both are VS Code extensions, configured in the UI:

- **Anthropic provider:** tick "Use custom base URL" and enter `http://127.0.0.1:8080`.
- **OpenAI Compatible provider:** set "Base URL" to `http://127.0.0.1:8081/v1`.

Docs: [Cline Anthropic](https://docs.cline.bot/provider-config/anthropic),
[Cline OpenTelemetry](https://docs.cline.bot/enterprise-solutions/monitoring/opentelemetry_override),
[Roo Code providers](https://roocodeinc.github.io/Roo-Code/providers/openai-compatible).

### OpenCode

Over OTel: **no OTel** in V2 (V1's experimental `openTelemetry` field is ignored).

**B.** These are V2 keys, in `~/.config/opencode/opencode.json` or a project's `opencode.json`:

```jsonc
{ "providers": {
    "anthropic": { "settings": { "baseURL": "http://127.0.0.1:8080" } },
    "logpak-openai": { "package": "@opencode/ai/providers/openai-compatible",
                       "settings": { "baseURL": "http://127.0.0.1:8081/v1" } } } }
```

- On V1, the key is `provider.<id>.options.baseURL` (see the migration page).
- With ChatGPT sign-in and no custom `baseURL`, OpenCode zstd-encodes request bodies. Logpak
  decodes zstd before it stores them.

Docs: [providers](https://opencode.ai/docs/providers/), [migrating from V1](https://opencode.ai/docs/migrate-v1/).

### Aider

Over OTel: **no OTel** (analytics go to PostHog only).

**B.** Aider goes through LiteLLM.

```bash
OPENAI_API_BASE=http://127.0.0.1:8081/v1 aider --model openai/<model>
```

```bash
ANTHROPIC_API_BASE=http://127.0.0.1:8080 aider --model anthropic/<model>
```

The Anthropic variable is LiteLLM's, not documented by Aider *(unverified)*. `--llm-history-file`
writes plain text, not JSON lines.

Docs: [OpenAI-compatible](https://aider.chat/docs/llms/openai-compat.html).

### Continue

Over OTel: **no OTel**.

**B.** In `~/.continue/config.yaml`:

```yaml
models:
  - name: via-logpak
    provider: openai
    model: <model>
    apiBase: http://127.0.0.1:8081/v1
    apiKey: <key>
    roles: [chat, edit]
```

`apiBase` with `provider: anthropic` is *(unverified)*; if you try it, use `http://127.0.0.1:8080`.

Docs: [config reference](https://docs.continue.dev/reference).

### SWE-agent and mini-swe-agent

Over OTel: **no OTel**. Both call models through LiteLLM, so B works; their trajectories (C) also
pack well.

**B.** In SWE-agent's config YAML:

```yaml
agent:
  model:
    name: openai/<model>
    api_base: http://127.0.0.1:8081/v1
```

In mini-swe-agent's config (passed straight to `litellm.completion`):

```yaml
model:
  model_name: openai/<model>
  model_kwargs:
    api_base: http://127.0.0.1:8081/v1
```

**C.** A SWE-agent `.traj` file is one JSON document. Each step's `query` is the exact input of that
step, so steps re-send the conversation and pack well. Emit one step per line:

```bash
for t in trajectories/*/*/*/*.traj; do jq -c '.trajectory[]' "$t"; done | logpak log pack swe.lpk --window 32
```

Docs: [keys and api_base](https://swe-agent.com/latest/installation/keys/),
[trajectories](https://swe-agent.com/latest/usage/trajectories/),
[mini-swe-agent](https://mini-swe-agent.com/latest/models/local_models/).

### Crush

Over OTel: **no OTel** (pseudonymous metrics only, without prompts).

**B.** In `~/.config/crush/crushrc`. Use `--type messages` for an Anthropic-compatible provider.

```bash
provider add logpak --type completions --base-url "http://127.0.0.1:8081/v1" --api-key "$OPENAI_API_KEY"
```

Docs: [README](https://github.com/charmbracelet/crush).

### Pi

Over OTel: **no OTel**.

**B.** In `~/.pi/agent/models.json`. Setting only `baseUrl` keeps the provider's models and auth.

```json
{ "providers": { "anthropic": { "baseUrl": "http://127.0.0.1:8080/v1" } } }
```

**C.** Sessions are JSON lines under `~/.pi/agent/sessions/`, one entry per line.

Docs: [models.md](https://unpkg.com/@earendil-works/pi-coding-agent@1.5.2/docs/models.md).

### Zed

Over OTel: **no OTel**.

**B.** Use `api_url` under `language_models` in Zed's settings: `http://127.0.0.1:8081/v1` for
`openai_compatible`, `http://127.0.0.1:8080` for `anthropic_compatible`. Copy the exact nesting from
the docs *(unverified)*. Zed-hosted models cannot be redirected.

Docs: [API access](https://zed.dev/docs/ai/use-api-access).

---

## Personal and autonomous agents

Long-running agents that act on your behalf across chat apps, a browser or a shell. They run for
hours and re-send their whole context on every step, which is the shape Logpak compresses best.

**In Docker?** Inside a container, `127.0.0.1` is the container itself. Run `logpak` on a network
the container can reach (`logpak collect --listen 0.0.0.0:4318,0.0.0.0:4317`, or
`logpak server --listen 0.0.0.0:8080`, on a private network), and point the agent at
`host.docker.internal` or the service name.

### OpenClaw

Over OTel: **messages**, once `captureContent` is on: input and output messages, tool content and
tool definitions. Provider-internal thinking is always left out.

**A.** Install the official plugin, then turn on diagnostics in `~/.openclaw/openclaw.json`. It
exports OTLP/HTTP protobuf only (gRPC is retired), so use `:4318`. OpenClaw appends `/v1/traces`,
`/v1/metrics` and `/v1/logs` to `endpoint`.

```bash
openclaw plugins install @openclaw/diagnostics-otel
```

```bash
openclaw plugins enable diagnostics-otel
```

```json5
{
  diagnostics: {
    enabled: true,
    otel: {
      enabled: true,
      endpoint: "http://127.0.0.1:4318",
      protocol: "http/protobuf",
      serviceName: "openclaw",
      traces: true,
      logs: true,            // off by default
      captureContent: true,  // off by default: without it, spans carry no prompts
    },
  },
}
```

The standard `OTEL_EXPORTER_OTLP_*` variables work as fallbacks.

**B.** Override a provider's `baseUrl` under `models.providers`, for example
`models.providers.anthropic.baseUrl: "http://127.0.0.1:8080"`. For a custom provider, set `api`
(`openai-completions`, `openai-responses` or `anthropic-messages`) and `baseUrl:
"http://127.0.0.1:8081/v1"`.

- ChatGPT/Codex sign-in goes to `chatgpt.com`, not `api.openai.com`, so it bypasses B.
- Responses traffic over that sign-in tries WebSocket first. Force SSE per model with
  `agents.defaults.models["openai/<model>"].params.transport: "sse"`.

**C.** `openclaw sessions export-trajectory --session-key <key>` writes `events.jsonl` (with
`prompts.json`) under `<workspace>/.openclaw/trajectory-exports/`:

```bash
logpak log pack openclaw.lpk < events.jsonl
```

Docs: [OpenTelemetry setup](https://docs.openclaw.ai/gateway/opentelemetry/setup),
[configuration](https://docs.openclaw.ai/gateway/opentelemetry/configuration),
[model providers](https://docs.openclaw.ai/concepts/model-providers).

### Hermes Agent

Over OTel: **messages**, through the built-in NeMo Relay. Hermes's own gateway monitoring also
speaks OTLP, but carries health and lifecycle data only, never prompts.

**A.** Turn on NeMo Relay's observability component, in `~/.config/nemo-relay/plugins.toml` (or the
file `HERMES_NEMO_RELAY_PLUGINS_TOML` names):

```toml
[[components]]
kind = "observability"
enabled = true

[components.config]
version = 4

[components.config.opentelemetry]
enabled = true

[[components.config.opentelemetry.endpoints]]
type = "gen_ai"                               # or "openinference", "full"
endpoint = "http://127.0.0.1:4318/v1/traces"
transport = "http_binary"
service_name = "hermes-agent"
```

- With the component on, `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318` alone also starts the
  export.
- `gen_ai` spans carry the prompt and response messages by default. `enable_full_payloads = false`
  trims repeated history; it does not remove the messages.
- Relay asks for HTTPS on remote endpoints unless the host is allowlisted. Loopback is assumed fine
  *(unverified)*.

**B.** In `~/.hermes/config.yaml`:

```yaml
model:
  provider: custom
  base_url: http://127.0.0.1:8081/v1
  key_env: OPENAI_API_KEY
```

- Hermes sends `OPENAI_API_KEY` only to `*.openai.com` or to the origin of the `OPENAI_BASE_URL`
  it is paired with. That's why the proxy entry names its key with `key_env`.
- `ANTHROPIC_BASE_URL=http://127.0.0.1:8080` redirects the Anthropic provider *(the `/v1`
  convention is unverified)*.
- Sign-in providers (Nous Portal, ChatGPT/Codex, Claude Max, xAI) call their own endpoints, and may
  bypass B *(unverified)*.

**C.** `hermes sessions export sessions.jsonl` writes one session per line, and `--format trace`
writes Claude-Code-style JSON lines. `HERMES_DUMP_REQUESTS=true` writes each API request payload to
the log files.

```bash
hermes sessions export sessions.jsonl && logpak log pack hermes.lpk < sessions.jsonl
```

Docs: [NeMo Relay](https://hermes-agent.nousresearch.com/docs/integrations/nemo-relay),
[providers](https://hermes-agent.nousresearch.com/docs/integrations/providers),
[environment variables](https://hermes-agent.nousresearch.com/docs/reference/environment-variables),
[session storage](https://hermes-agent.nousresearch.com/docs/developer-guide/session-storage).

### nanobot

Over OTel: **no OTel** (a Langfuse plugin, not OTLP).

**B.** In `~/.nanobot/config.json`, set a provider's `apiBase`, with the version path where the
endpoint needs one:

```json
{ "providers": {
    "custom":    { "apiKey": "<key>", "apiBase": "http://127.0.0.1:8081/v1" },
    "anthropic": { "apiKey": "<key>", "apiBase": "http://127.0.0.1:8080" } } }
```

Sign-in providers (`nanobot provider login openai-codex`, `github-copilot`, `xai-grok`) take no
`apiBase`.

Docs: [providers](https://github.com/HKUDS/nanobot/blob/main/docs/providers.md),
[configuration](https://github.com/HKUDS/nanobot/blob/main/docs/configuration.md).

### Open Interpreter

Over OTel: **prompts only** *(unverified: from its config schema)*. The current Open Interpreter
is a Rust fork of Codex, with Codex's configuration.

**B.** In `~/.openinterpreter/config.toml`:

```toml
model_provider = "logpak"

[model_providers.logpak]
base_url = "http://127.0.0.1:8081/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"   # or "chat"
```

Leave `supports_websockets` off on this provider.

**A (prompts).** The schema has a Codex-style `[otel]` table with `log_user_prompt` and an
`exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/logs", protocol = "binary" } }`
*(unverified)*.

Docs: [config](https://www.openinterpreter.com/docs/terminal/config).

### Letta

Over OTel: *(unverified)*.

**B.** Agents on Letta Cloud call models from Letta's servers, which need a public endpoint. With the
local backend, the model calls start on your machine:

```bash
letta --backend local connect lmstudio --base-url http://127.0.0.1:8081/v1
```

Or run `/connect` and choose the "OpenAI-compatible API" provider with the same base URL.

Docs: [models](https://docs.letta.com/configuration/models/).

### Agent Zero

Over OTel: **no OTel**.

**B.** In the web UI's Settings, pick "OpenAI Compatible" for each model slot (Main, Utility,
Embedding), and set its API base URL to `http://host.docker.internal:8081/v1`. Agent Zero runs in
Docker, so see [In Docker?](#personal-and-autonomous-agents) above.

Docs: [installation](https://github.com/agent0ai/agent-zero/blob/main/docs/setup/installation.md).

### OpenManus

Over OTel: **no OTel**.

**B.** In `config/config.toml`, the `[llm]` section's `base_url`. OpenManus calls every provider
through an OpenAI-compatible client, so the path includes `/v1/`:

```toml
[llm]
model = "<model>"
base_url = "http://127.0.0.1:8080/v1/"
api_key = "<key>"
```

Docs: [config.example.toml](https://github.com/FoundationAgents/OpenManus/blob/main/config/config.example.toml).

### AutoGPT

Over OTel: **no OTel** (traces go to PostHog).

**B.** In `autogpt_platform/backend/.env`, AutoPilot calls any OpenAI-compatible endpoint:

```bash
CHAT_USE_LOCAL=true
CHAT_BASE_URL=http://host.docker.internal:8081/v1
CHAT_API_KEY=<key>
```

The platform runs in Docker, so see [In Docker?](#personal-and-autonomous-agents) above.

Docs: [.env.default](https://github.com/Significant-Gravitas/AutoGPT/blob/master/autogpt_platform/backend/.env.default).

---

## Agent workflow platforms

Self-hosted platforms where agents are built from nodes or flows, and chat front-ends used as agent
hubs. Most run in Docker: see the [Docker note](#personal-and-autonomous-agents) for reaching
Logpak from a container.

### n8n

Over OTel: **messages** for workflows built with n8n's Agents feature (v2.33 and later). The classic
**AI Agent node** shows up only as a `node.execute` span, with no content: use B for it.

**A.** OTLP traces, in preview since v2.19. Set these on every instance, workers included:

```bash
N8N_OTEL_ENABLED=true
N8N_OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4318
N8N_OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf   # or grpc (v2.39 and later)
N8N_OTEL_TRACES_PRODUCTION_ONLY=false           # export manual and test runs too
```

Agent spans record inputs and outputs by default (`N8N_AGENTS_TRACING_RECORD_INPUTS` and
`N8N_AGENTS_TRACING_RECORD_OUTPUTS`).

**B.** In the OpenAI credential, set **Base URL** to `http://host.docker.internal:8081/v1`. In the
Anthropic credential, set **Base URL** to `http://host.docker.internal:8080` (no `/v1`).

**C.** `GET /api/v1/executions?includeData=true` returns execution data as JSON, one array per page.
Turn it into one line per execution with `jq -c '.data[]'`.

Docs: [OpenTelemetry](https://docs.n8n.io/deploy/host-n8n/keep-n8n-running/trace-executions-with-opentelemetry),
[environment variables](https://docs.n8n.io/deploy/host-n8n/configure-n8n/basic-configuration/use-environment-variables/opentelemetry).

### Dify

Over OTel: **messages**. LLM node spans carry `gen_ai.input.messages` and `gen_ai.output.messages`
(on Community and Cloud editions; on Enterprise, set `ENTERPRISE_INCLUDE_CONTENT=true`).

**A.** In the API and worker containers' environment:

```bash
ENABLE_OTEL=true
OTLP_BASE_ENDPOINT=http://host.docker.internal:4318   # Dify appends /v1/traces and /v1/metrics
OTEL_SAMPLING_RATE=1.0                                 # the default, 0.1, traces one request in ten
```

`OTEL_EXPORTER_OTLP_PROTOCOL=grpc` switches to gRPC (`:4317`).

Per app, the Monitoring page's **OpenTelemetry** tracing provider works too. Set its endpoint to
`http://host.docker.internal:4318`. Its requests pass Dify's SSRF egress policy, so a private
address has to be allowed in the `ssrf_proxy` configuration.

**B.** In the model provider settings, set the base URL field:

| Provider plugin | Field | Value |
|---|---|---|
| OpenAI | API Base URL | `http://host.docker.internal:8081` (Dify adds `/v1` when it is missing) |
| OpenAI-API-compatible | API Base URL | `http://host.docker.internal:8081/v1` |
| Anthropic | API URL | `http://host.docker.internal:8080` |

Docs: [environment variables](https://docs.dify.ai/en/self-host/configuration/environments).

### Langflow

Over OTel: **messages**, through the Phoenix integration. Langflow's own OTel traces have one
`flow.execute` span per run, with no prompts.

**A.** Point the Phoenix integration at the collector. It exports OTLP/HTTP protobuf to
`<endpoint>/v1/traces`, with OpenInference's LangChain instrumentation, which records LLM inputs and
outputs:

```bash
PHOENIX_COLLECTOR_ENDPOINT=http://127.0.0.1:4318
```

On a host other than `localhost` or `127.0.0.1` (`host.docker.internal`, say), Langflow also wants
`PHOENIX_API_KEY`. Any value works, because the collector does not check it.

**B.** In the OpenAI model component, set **OpenAI API Base** (an advanced field) to
`http://127.0.0.1:8081/v1`. In the Anthropic component, set **Anthropic API URL** to
`http://127.0.0.1:8080`.

Docs: [OpenTelemetry](https://docs.langflow.org/observability-opentelemetry),
[Arize Phoenix](https://docs.langflow.org/integrations-arize).

### Flowise

Over OTel: **messages**, through the Phoenix analytics integration. Flowise's native OTel exports
metrics only.

**A.** In the chatflow's Analytics settings, choose Phoenix. Create a "Phoenix API" credential with
**Endpoint** `http://host.docker.internal:4318` and any **API Key**. Flowise exports OTLP/HTTP
protobuf to `<endpoint>/v1/traces`, with OpenInference's LangChain instrumentation, which includes
prompts and responses.

**B.** In the ChatOpenAI node's additional parameters, set **Base Path** to
`http://host.docker.internal:8081/v1`. The ChatAnthropic node has no base-URL field.

Docs: [analytics](https://docs.flowiseai.com/using-flowise/analytics).

### Open WebUI

Over OTel: **metadata only** (HTTP, database and Redis spans, no GenAI attributes).

**B.** Point the OpenAI connection at the recording server:

```bash
OPENAI_API_BASE_URL=http://host.docker.internal:8081/v1
```

The setting is a PersistentConfig: after the first start, the value in Admin Settings, Connections
wins, so change it there (or set `ENABLE_PERSISTENT_CONFIG=False`).

Docs: [OpenTelemetry](https://docs.openwebui.com/reference/monitoring/otel).

### LibreChat

Over OTel: **metadata only** (`OTEL_TRACING_ENABLED` traces the server, database and outbound HTTP,
without prompts).

**B.** In `librechat.yaml`, add a custom endpoint. LibreChat appends `/chat/completions`:

```yaml
endpoints:
  custom:
    - name: "via-logpak"
      apiKey: "${OPENAI_API_KEY}"
      baseURL: "http://host.docker.internal:8081/v1"
      models: { default: ["<model>"] }
```

For Anthropic, `ANTHROPIC_REVERSE_PROXY=http://host.docker.internal:8080`.

Docs: [custom endpoints](https://www.librechat.ai/docs/configuration/librechat_yaml/object_structure/custom_endpoint),
[logging](https://www.librechat.ai/docs/configuration/logging).

### AnythingLLM

Over OTel: **no OTel**.

**B.** Use the Generic OpenAI provider, in the UI or in `.env`:

```bash
LLM_PROVIDER=generic-openai
GENERIC_OPEN_AI_BASE_PATH=http://host.docker.internal:8081/v1
GENERIC_OPEN_AI_API_KEY=<key>
GENERIC_OPEN_AI_MODEL_PREF=<model>
```

**C.** Admin, Workspace Chats, Export, as JSONL (or `GET /api/v1/system/export-chats?type=jsonl`
with a developer API key). Each line is a prompt and its response.

```bash
logpak log pack anythingllm.lpk < chats.jsonl
```

Docs: [.env.example](https://github.com/Mintplex-Labs/anything-llm/blob/master/docker/.env.example).

### Activepieces

Over OTel: logs only (`AP_OTEL_ENABLED=true`); whether they carry prompts is *(unverified)*.

**B.** In Admin, AI Providers, add **Other (OpenAI Compatible)** with base URL
`http://host.docker.internal:8081/v1`. The built-in OpenAI and Anthropic providers take no base URL.

Docs: [AI providers](https://www.activepieces.com/docs/admin-guide/guides/setup-ai-providers),
[OpenTelemetry](https://www.activepieces.com/docs/install/configure-operate/setup-opentelemetry).

### Rivet

Over OTel: **no OTel**.

**B.** In Settings, **OpenAI Endpoint** takes the full URL: `http://127.0.0.1:8081/v1/chat/completions`.
The Anthropic plugin's **Anthropic API Endpoint** (or `ANTHROPIC_API_ENDPOINT`) defaults to
`https://api.anthropic.com/v1`, so use `http://127.0.0.1:8080/v1`.

Docs: [repository](https://github.com/Ironclad/rivet).

---

## SDKs and agent frameworks

### Export to the collector

Every framework below exports OTLP to `collect` (A). Set the endpoint once:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

### OpenTelemetry GenAI instrumentations (Python)

The official instrumentations now live in
[opentelemetry-python-genai](https://github.com/open-telemetry/opentelemetry-python-genai/tree/main/instrumentation),
packaged as `opentelemetry-instrumentation-genai-<library>`. That covers openai, anthropic,
langchain, llama-index, crewai, dspy, agno, smolagents, openai-agents, claude-agent-sdk, bedrock
and ollama, plus `opentelemetry-instrumentation-google-genai`. The older
`opentelemetry-instrumentation-openai-v2` is deprecated.

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http opentelemetry-instrumentation-genai-openai
```

```bash
OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=span_only OTEL_LOGS_EXPORTER=otlp opentelemetry-instrument python app.py
```

- Content is off by default (`no_content`). The values are `span_only`, `event_only` and
  `span_and_event`. `true` is only understood by the deprecated packages.
- In code: `OpenAIInstrumentor().instrument()` from `opentelemetry.instrumentation.genai.openai`,
  and likewise per library.

### OpenLLMetry (Traceloop)

```bash
pip install traceloop-sdk
```

```bash
export TRACELOOP_BASE_URL=http://127.0.0.1:4318    # /v1/traces is appended; http(s) means OTLP/HTTP
```

```python
from traceloop.sdk import Traceloop
Traceloop.init(disable_batch=True)
```

- For JS, `npm install @traceloop/node-server-sdk` and call `initialize({ disableBatch: true })`
  before importing any LLM module.
- Content is on by default (`TRACELOOP_TRACE_CONTENT`). `TRACELOOP_HEADERS` replaces the API key.

Docs: [configuration](https://www.traceloop.com/docs/openllmetry/configuration).

### OpenInference (Arize)

Instrumentors exist for openai, anthropic, langchain, llama-index, crewai, dspy, haystack,
smolagents, agno, openai-agents, google-adk, google-genai, autogen-agentchat, pydantic-ai,
strands-agents, claude-agent-sdk, litellm, bedrock, mistralai, groq and vertexai. Content is on by
default.

```bash
pip install openinference-instrumentation-openai opentelemetry-sdk opentelemetry-exporter-otlp
```

```python
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from openinference.instrumentation.openai import OpenAIInstrumentor

tp = trace_sdk.TracerProvider()
tp.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter("http://127.0.0.1:4318/v1/traces")))
OpenAIInstrumentor().instrument(tracer_provider=tp)
```

`OPENINFERENCE_HIDE_INPUTS` and `OPENINFERENCE_HIDE_OUTPUTS` turn content off.

Docs: [trace config](https://openinf.dev/spec/configuration/trace-config.html),
[repository](https://github.com/Arize-ai/openinference).

### Langfuse SDK

Keep Langfuse and keep every span. Send the plain OTel exporter to `collect`, and forward to
Langfuse. Langfuse takes OTLP/HTTP only, but `collect` turns gRPC exports into OTLP/HTTP protobuf.

```bash
logpak collect --record-dir ~/logpak/otel --forward https://cloud.langfuse.com/api/public/otel
```

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(echo -n "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" | base64 -w0),x-langfuse-ingestion-version=4"
```

- The Langfuse SDK posts to `LANGFUSE_BASE_URL` + `/api/public/otel/v1/traces`. Its other calls
  (prompts, scores) also use `LANGFUSE_BASE_URL`, so leave that pointing at Langfuse and route the
  traces as above.
- `x-langfuse-ingestion-version=4` makes ingestion real time, instead of up to 10 minutes late.

Docs: [OpenTelemetry](https://langfuse.com/integrations/native/opentelemetry).

### LangSmith (LangChain, LangGraph)

```bash
pip install "langsmith[otel]" langchain
```

```bash
export LANGSMITH_OTEL_ENABLED=true LANGSMITH_TRACING=true LANGSMITH_OTEL_ONLY=true
```

To keep LangSmith too, forward to it and put the key in the exporter's headers:

```bash
logpak collect --record-dir ~/logpak/otel --forward https://api.smith.langchain.com/otel
```

```bash
export OTEL_EXPORTER_OTLP_HEADERS="x-api-key=$LANGSMITH_API_KEY"
```

Inputs and outputs are sent unless you set `LANGSMITH_HIDE_INPUTS` or `LANGSMITH_HIDE_OUTPUTS`.

Docs: [trace with OpenTelemetry](https://docs.langchain.com/langsmith/trace-with-opentelemetry).

### Pydantic Logfire and Pydantic AI

Logfire exports OTLP/HTTP protobuf only.

```python
import logfire
logfire.configure(service_name="my-agent", send_to_logfire=False)   # reads OTEL_EXPORTER_OTLP_ENDPOINT
logfire.instrument_pydantic_ai()
```

Without Logfire, install a `TracerProvider` with `OTLPSpanExporter()`, then call
`Agent.instrument_all()`. Content is included unless you pass
`InstrumentationSettings(include_content=False)`.

Docs: [alternative backends](https://pydantic.dev/docs/logfire/guides/alternative-backends/).

### LangChain and LangGraph

Any of three routes works:

- LangSmith OTel, as above.
- `opentelemetry-instrumentation-genai-langchain` with `LangChainInstrumentor().instrument()` and
  the GenAI content flag.
- `openinference-instrumentation-langchain`, with content on by default.

### LlamaIndex

```bash
pip install llama-index-observability-otel opentelemetry-exporter-otlp-proto-http
```

```python
from llama_index.observability.otel import LlamaIndexOpenTelemetry
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
LlamaIndexOpenTelemetry(span_exporter=OTLPSpanExporter("http://127.0.0.1:4318/v1/traces")).start_registering()
```

The native package's content default is *(unverified)*. The OpenInference and OTel GenAI
instrumentors document theirs.

Docs: [observability](https://github.com/run-llama/llama_index/blob/main/docs/src/content/docs/framework/module_guides/observability/index.md).

### OpenAI Agents SDK

The SDK's own tracing is not OTel. In Python, bridge it:

```bash
pip install opentelemetry-instrumentation-genai-openai-agents
```

```python
OpenAIAgentsInstrumentor().instrument(disable_openai_trace_export=True)
```

Add `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=span_only`. Alternatively, use
`openinference-instrumentation-openai-agents`. The JS SDK has no documented OTel bridge, so set the
client's base URL (B) instead.

Docs: [tracing](https://openai.github.io/openai-agents-python/tracing/).

### Vercel AI SDK

Inputs and outputs are recorded by default. Current versions:

```ts
import { registerTelemetry } from "ai";
import { OpenTelemetry } from "@ai-sdk/otel";
registerTelemetry(new OpenTelemetry());
```

- AI SDK v5: pass `experimental_telemetry: { isEnabled: true }` per call.
- Either way, register an OTLP exporter: `@vercel/otel`'s `registerOTel` in Next.js, or `NodeSDK`
  with `@opentelemetry/exporter-trace-otlp-proto`, reading `OTEL_EXPORTER_OTLP_ENDPOINT`.

Docs: [telemetry](https://ai-sdk.dev/docs/ai-sdk-core/telemetry).

### Mastra

```bash
npm install @mastra/otel-exporter @opentelemetry/exporter-trace-otlp-proto
```

```ts
new Mastra({ observability: new Observability({ configs: { otel: { serviceName: "my-agent",
  exporters: [new OtelExporter({ provider: { custom: {
    endpoint: "http://127.0.0.1:4318/v1/traces", protocol: "http/protobuf" } } })] } } }) });
```

`endpoint` is the full path. Content goes on `gen_ai.input.messages` and `gen_ai.output.messages`.
`tracingOptions: { hideInput: true }` hides it per call.

Docs: [OTel exporter](https://mastra.ai/docs/observability/tracing/exporters/otel).

### CrewAI

CrewAI's built-in OTel is its own anonymous telemetry. Use an instrumentor:
`opentelemetry-instrumentation-genai-crewai`, or `openinference-instrumentation-crewai` with
`CrewAIInstrumentor().instrument(skip_dep_check=True, tracer_provider=tp)`. To silence CrewAI's
telemetry, set `CREWAI_DISABLE_TELEMETRY=true`, not `OTEL_SDK_DISABLED`, which turns off your
export as well.

Docs: [CrewAI with Phoenix](https://docs.crewai.com/en/observability/arize-phoenix).

### AutoGen

The docs' example exports gRPC to `http://localhost:4317`, which `collect` takes as is. It uses the
OpenLLMetry `opentelemetry-instrumentation-openai` with `OpenAIInstrumentor().instrument()`, so
`TRACELOOP_TRACE_CONTENT` applies *(unverified)*.

Docs: [tracing](https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tracing.html).

### Microsoft Agent Framework

The protocol defaults to gRPC. Name the protocol for `:4318`.

```bash
pip install opentelemetry-exporter-otlp-proto-http
```

```python
from agent_framework.observability import configure_otel_providers
configure_otel_providers(otlp_endpoint="http://127.0.0.1:4318", otlp_protocol="http/protobuf",
                         enable_sensitive_data=True)
```

- `enable_sensitive_data` (env `ENABLE_SENSITIVE_DATA=true`) turns on content capture; it is off
  by default.
- .NET: `.UseOpenTelemetry(configure: c => c.EnableSensitiveData = true)` with `.AddOtlpExporter()`.

Docs: [observability](https://learn.microsoft.com/en-us/agent-framework/agents/observability).

### Semantic Kernel

Without one of these switches, no AI spans are emitted at all:

- Python: `SEMANTICKERNEL_EXPERIMENTAL_GENAI_ENABLE_OTEL_DIAGNOSTICS_SENSITIVE=true`
- C#: `AppContext.SetSwitch("Microsoft.SemanticKernel.Experimental.GenAI.EnableOTelDiagnosticsSensitive", true)`
  and `.AddSource("Microsoft.SemanticKernel*")`.

Then add an OTLP exporter in place of the docs' console one.

Docs: [telemetry](https://learn.microsoft.com/en-us/semantic-kernel/concepts/enterprise-readiness/observability/telemetry-with-console).

### Spring AI

Prompts and completions are emitted as logs, not span attributes, so export logs too. All of these
default to false:

```properties
spring.ai.chat.observations.log-prompt=true
spring.ai.chat.observations.log-completion=true
spring.ai.chat.client.observations.log-prompt=true
spring.ai.chat.client.observations.log-completion=true
spring.ai.tools.observations.include-content=true
```

The Spring Boot OTLP endpoint properties (`management.opentelemetry.tracing.export.otlp.*`) are
*(unverified)*. Check them against your Boot version.

Docs: [Spring AI observability](https://docs.spring.io/spring-ai/reference/observability/index.html).

### Strands Agents

```bash
pip install 'strands-agents[otel]'
```

```python
from strands.telemetry import StrandsTelemetry
StrandsTelemetry().setup_otlp_exporter()   # reads OTEL_EXPORTER_OTLP_ENDPOINT
```

Prompts and responses are recorded by default.

Docs: [traces](https://strandsagents.com/latest/documentation/docs/user-guide/observability-evaluation/traces/).

### Google ADK

```bash
pip install 'google-adk[otel]'
```

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
export OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=SPAN_ONLY
export OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental
```

```bash
adk api_server
```

ADK's own spans carry content unless `ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS=false`.

Docs: [traces](https://adk.dev/observability/traces/).

### smolagents

```bash
pip install 'smolagents[telemetry]'
```

Use OpenInference's `SmolagentsInstrumentor().instrument(tracer_provider=tp)` with the
`TracerProvider` from the [OpenInference](#openinference-arize) example. Content is on.

Docs: [inspect runs](https://huggingface.co/docs/smolagents/tutorials/inspect_runs).

### Haystack

Since Haystack 3.0, tracing is off until you enable it, and content separately.

```bash
pip install opentelemetry-haystack opentelemetry-exporter-otlp
```

```bash
export HAYSTACK_CONTENT_TRACING_ENABLED=true   # before importing haystack
```

```python
from haystack import tracing
from haystack_integrations.tracing.opentelemetry import OpenTelemetryTracer
tracing.enable_tracing(OpenTelemetryTracer(trace.get_tracer("my_app")))   # with an OTLP TracerProvider set
```

Docs: [tracing](https://docs.haystack.deepset.ai/docs/tracing).

### DSPy, Agno

DSPy documents MLflow only. Agno needs an instrumentor before anything is exported. For either,
use `opentelemetry-instrumentation-genai-dspy` or `-agno` with the GenAI content flag, or the
OpenInference instrumentors, which capture content by default.

### LangChain4j, Genkit

Neither has a documented generic OTLP export that includes prompts. Point the model client at a
recording server, [below](#no-content-over-otel-change-the-provider-sdks-base-url-b).

### No content over OTel? Change the provider SDK's base URL (B)

For LangChain4j, Genkit, OpenAI Agents SDK (JS), or wherever the exact request bytes matter, point
the provider SDK underneath at a recording server. This works whatever framework sits on top.

| SDK | Code | Env var |
|---|---|---|
| OpenAI Python / JS | `OpenAI(base_url="http://127.0.0.1:8081/v1")` / `new OpenAI({ baseURL: ... })` | `OPENAI_BASE_URL` |
| Anthropic Python / TS | `Anthropic(base_url="http://127.0.0.1:8080")` / `new Anthropic({ baseURL: ... })` | `ANTHROPIC_BASE_URL` |
| google-genai Python | `genai.Client(http_options=types.HttpOptions(base_url="http://127.0.0.1:8082"))` | `GOOGLE_GEMINI_BASE_URL` |
| LangChain | `ChatOpenAI(base_url=".../v1")`, `ChatAnthropic(base_url="http://127.0.0.1:8080")` | `OPENAI_API_BASE`, `ANTHROPIC_API_URL` |
| LlamaIndex | `OpenAI(api_base=".../v1")`, `Anthropic(base_url="http://127.0.0.1:8080")` | `OPENAI_API_BASE` |
| Vercel AI SDK | `createOpenAI({ baseURL: ".../v1" })`, `createAnthropic({ baseURL: "http://127.0.0.1:8080/v1" })` | `OPENAI_BASE_URL`, `ANTHROPIC_BASE_URL` |
| LiteLLM SDK | `completion(..., api_base="http://127.0.0.1:8081/v1")`; Anthropic: `api_base="http://127.0.0.1:8080"` | `OPENAI_BASE_URL`, `ANTHROPIC_API_BASE` |

The Vercel AI SDK's Anthropic provider wants `/v1` in its base URL, and the Anthropic SDK does not.
Don't share one `ANTHROPIC_BASE_URL` between them.

---

## LLM gateways

### LiteLLM proxy

**A, payload logs.** The `generic_api` callback posts each call's `StandardLoggingPayload`, messages
and response included. `log_format: single` posts one entry per request, so any request reads back
on its own.

```yaml
litellm_settings:
  callbacks: ["logpak"]
callback_settings:
  logpak:
    callback_type: generic_api
    endpoint: http://logpak-collect:4318/litellm
    log_format: single        # json_array (default) | ndjson | single
```

The environment alternative is `GENERIC_LOGGER_ENDPOINT` (tested in Logpak's CI), which posts
the default JSON arrays. Those still delta well, because each batch shares its prompts with the
one before.

**A, traces.** Prompts are on by default, in `gen_ai.input.messages`.

```yaml
litellm_settings:
  callbacks: ["otel"]
```

```bash
export OTEL_EXPORTER=otlp_http OTEL_ENDPOINT=http://logpak-collect:4318
```

**B.** Point a model's `api_base` at a recording server:

```yaml
model_list:
  - model_name: gpt-4o
    litellm_params: { model: openai/gpt-4o, api_base: "http://logpak-server:8080/v1", api_key: os.environ/OPENAI_API_KEY }
```

Docs: [generic API](https://docs.litellm.ai/docs/observability/generic_api),
[OpenTelemetry](https://docs.litellm.ai/docs/observability/opentelemetry_integration).

### Kong AI Gateway

**A.** Log payloads from `ai-proxy` and ship each entry with `http-log` (tested in Logpak's CI).

```yaml
plugins:
  - name: ai-proxy
    config:
      max_request_body_size: 16777216       # default 1 MiB; 0 = nginx's limit
      logging: { log_payloads: true, log_statistics: true }
  - name: http-log
    config:
      http_endpoint: http://logpak-collect:4318/kong
      content_type: application/json
```

- Set `KONG_NGINX_HTTP_CLIENT_BODY_BUFFER_SIZE=16m`. A body Kong spills to a temp file is answered
  400.
- `http-log` sends one object per request until `queue.max_batch_size` is above 1.
- The `opentelemetry` plugin also works. It is OTLP/HTTP protobuf only, so use
  `traces_endpoint: http://logpak-collect:4318/v1/traces`.

**B.** Set `model.options.upstream_url` to the full recording-server URL, for example
`http://logpak-server:8080/v1/chat/completions`.

Docs: [ai-proxy](https://developer.konghq.com/plugins/ai-proxy/reference/),
[http-log](https://developer.konghq.com/plugins/http-log/).

### Portkey (open-source gateway)

The open-source gateway exports no logs. Its Enterprise build's experimental OTel push is
`EXPERIMENTAL_GEN_AI_OTEL_TRACES_ENABLED`. Record in front of it with B (tested in Logpak's CI):

```bash
logpak server --listen 127.0.0.1:8090 --upstream http://localhost:8787 --record-dir ~/logpak/portkey
```

Clients then use `http://127.0.0.1:8090/v1`.

Docs: [gateway](https://github.com/Portkey-AI/gateway).

### Apache APISIX

```json
"plugins": {
  "ai-proxy": { "provider": "openai", "auth": { "header": { "Authorization": "Bearer <key>" } },
                "options": { "model": "gpt-4o" }, "logging": { "summaries": true, "payloads": true } },
  "http-logger": { "uri": "http://logpak-collect:4318/apisix", "concat_method": "new_line" }
}
```

- `logging.payloads` is off by default.
- Whether its payloads reach `http-logger` is *(unverified)*: the docs show `kafka-logger`. The
  fallback is `include_req_body: true`, and then raise `max_req_body_bytes` from its 512 KiB default.
- For B, set `override.endpoint` to the recording server.

Docs: [ai-proxy](https://apisix.apache.org/docs/apisix/plugins/ai-proxy/),
[http-logger](https://apisix.apache.org/docs/apisix/plugins/http-logger/).

### Bifrost

Content is exported by default.

```json
{ "plugins": [ { "enabled": true, "name": "otel", "config": {
    "service_name": "bifrost", "collector_url": "http://logpak-collect:4318",
    "trace_type": "genai_extension", "protocol": "http" } } ] }
```

- Use `"protocol": "grpc"` with `:4317` for gRPC.
- Whether `collector_url` needs `/v1/traces` is *(unverified)*: the docs show both forms.
- For B, set a provider's `network_config.base_url`.

Docs: [OTel](https://docs.getbifrost.ai/features/observability/otel).

### TensorZero

OTLP export is gRPC only, so use `:4317`.

```toml
[gateway]
export.otlp.traces.enabled = true
```

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://logpak-collect:4317
```

- Whether spans include inference inputs is *(unverified)*.
- For B, set a provider's `api_base`.

Docs: [export OpenTelemetry traces](https://github.com/tensorzero/tensorzero/tree/main/docs).

### agentgateway

```yaml
frontendPolicies:
  tracing:
    host: logpak-collect:4317         # gRPC by default; protocol: http is also documented
    randomSampling: true
    attributes:
      llm.prompt: 'llm.prompt'           # CEL; the prompt is not in spans by default
      llm.completion: 'llm.completion'
```

The `attributes` block combines two documented pieces *(unverified)*.

Docs: [traces](https://agentgateway.dev/docs/standalone/latest/documentation/observability/traces/setup/),
[CEL context](https://agentgateway.dev/docs/standalone/latest/reference/cel/cel-context/).

### Envoy AI Gateway (Agent Router)

Its OpenInference spans include full requests and responses by default. In Helm values:

```yaml
extProc:
  extraEnvVars:
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: http://logpak-collect:4317
    - name: OTEL_METRICS_EXPORTER
      value: none
```

The default protocol is not stated; the docs' examples use `:4317`.

Docs: [tracing](https://theagentrouter.ai/docs/capabilities/observability/tracing).

### Higress

The `ai-statistics` plugin copies chosen request fields into the access log:

```yaml
attributes:
  - key: question
    value_source: request_body
    value: messages.@reverse.0.content
    apply_to_log: true
```

That is a field, not the whole body. For full requests, put a recording server behind Higress (B).

Docs: [ai-statistics](https://higress.ai/en/docs/latest/plugins/ai/api-o11y/ai-statistics/).

### Traefik Hub AI Gateway

Set the middleware's `observability.traces.level: content` to add input and output messages,
system instructions and tool definitions to spans. Point Traefik's OTLP tracing exporter at
`collect`; its keys are *(unverified)*.

Docs: [AI observability](https://doc.traefik.io/traefik-hub/ai-gateway/ai-observability).

### OpenRouter

Broadcast exports OTLP/HTTP JSON with prompts and completions, unless the destination is in Privacy
Mode. In Settings, Observability, OpenTelemetry Collector:

- **Endpoint:** `https://collect.example.com/v1/traces`, a public host in front of `collect` on
  `:4318`.
- **Headers:** whatever your TLS proxy expects.

Or record the requests yourself (B):

```bash
logpak server --listen 127.0.0.1:8083 --upstream https://openrouter.ai --record-dir ~/logpak/openrouter
```

Clients then use `http://127.0.0.1:8083/api/v1`.

Docs: [Broadcast to an OTel Collector](https://openrouter.ai/docs/guides/features/broadcast/otel-collector).

### Cloudflare AI Gateway

- **A.** The OTel export (configured in the dashboard) sends `gen_ai.prompt_json` and
  `gen_ai.completion_json`. Point its URL at a public `collect`, with content type `protobuf` or
  `json`.
- **C.** Logpush encrypts `RequestBody` and `ResponseBody` with your RSA key, up to 1 MB per log.
  Decrypt with Cloudflare's sample, then pack the JSON lines.

Docs: [OTel](https://developers.cloudflare.com/ai-gateway/observability/otel-integration/),
[Logpush](https://developers.cloudflare.com/ai-gateway/observability/logging/logpush/).

### Helicone

Webhooks cut bodies at 10 KB, so they don't reach `collect` whole. Record in front of it (B):

```bash
logpak server --listen 127.0.0.1:8084 --upstream https://ai-gateway.helicone.ai --record-dir ~/logpak/helicone
```

Docs: [webhooks](https://docs.helicone.ai/features/webhooks).

### Vercel AI Gateway

Its traces and logs carry metadata only; the docs say "they don't include prompt or completion
content". Record the requests (B) with `--upstream https://ai-gateway.vercel.sh`, and use the client
base URL `http://127.0.0.1:8085/v1`.

Docs: [trace drains](https://vercel.com/docs/ai-gateway/observability-and-spend/trace-drains).

### MLflow AI Gateway

Usage tracking logs requests as MLflow traces. Whether payloads are included, and any export, is
*(unverified)*. Use B.

---

## Cloud platform logs

### AWS Bedrock

Model invocation logging writes gzipped JSON to S3. `input.inputBodyJson` holds the full request
inline, up to 100 KB. Larger bodies go to separate S3 objects that the record points to, and must be
joined in.

```bash
aws bedrock put-model-invocation-logging-configuration --logging-config '{
  "s3Config": {"bucketName": "my-bucket", "keyPrefix": "bedrock"}, "textDataDeliveryEnabled": true }'
```

```bash
aws s3 cp --recursive s3://my-bucket/bedrock/AWSLogs/ ./bedrock-logs/
```

```bash
find bedrock-logs -name '*.json.gz' | sort | xargs zcat | jq -c . | logpak log pack bedrock.lpk --window 32
```

`jq -c .` normalises to one record per line, whatever the files' layout. Bedrock requests are
SigV4-signed over the host name, so a base-URL proxy (B) would need the client to sign for Bedrock's
host. Use the logs.

Docs: [model invocation logging](https://docs.aws.amazon.com/bedrock/latest/userguide/model-invocation-logging.html).

### Azure API Management (Azure OpenAI)

Enable "Log LLM messages", with prompts and completions, under the API's Azure Monitor diagnostic
logs. Rows land in `ApiManagementGatewayLlmLog` (`RequestMessages`, `ResponseMessages`), split into
32 KB chunks, with a 2 MB cap per request. Export the table as JSON lines, joining chunks on
`CorrelationId`, then pack it (C).

For Azure OpenAI directly, use B with `--upstream https://<resource>.openai.azure.com` and client base
URL `http://127.0.0.1:8080/openai/v1/`. The `api-key` header passes through.

Docs: [LLM logs](https://learn.microsoft.com/en-us/azure/api-management/api-management-howto-llm-logs).

### Google Vertex AI

Request-response logging (`setPublisherModelConfig` with `loggingConfig`) writes `full_request` and
`full_response` to BigQuery. Pairs over 10 MB are not recorded.

```bash
bq query --format=json --max_rows=1000000 --nouse_legacy_sql 'SELECT * FROM `proj.ds.request_response_logging`' | jq -c '.[]' | logpak log pack vertex.lpk
```

For large tables, export to GCS as newline-delimited JSON instead, then pack the files.

Docs: [request-response logging](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/request-response-logging).

---

## Telemetry pipelines

If spans already flow through a pipeline, add Logpak as one more exporter. Nothing else in the
pipeline changes.

### OpenTelemetry Collector

```yaml
exporters:
  otlp_http/logpak:                 # formerly otlphttp, now a deprecated alias
    endpoint: http://logpak-collect:4318
    compression: gzip
service:
  pipelines:
    traces: { receivers: [otlp], exporters: [your_backend, otlp_http/logpak] }
    logs:   { receivers: [otlp], exporters: [your_backend, otlp_http/logpak] }
```

- For gRPC, use `otlp_grpc/logpak` with `endpoint: logpak-collect:4317` and
  `tls: { insecure: true }`.
- The collector's own OTLP receiver caps HTTP bodies at 20 MiB (`max_request_body_size`). Raise it,
  and gRPC's `max_recv_msg_size_mib`, for long agent prompts.
- The `file` exporter's default JSON format writes one batch per line, ready for C. Leave its
  `compression` off.

Docs: [otlphttp exporter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/otlphttpexporter/README.md),
[file exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/fileexporter/README.md).

### Grafana Alloy

```alloy
otelcol.exporter.otlphttp "logpak" {
  client { endpoint = "http://logpak-collect:4318" }
}
```

Add `otelcol.exporter.otlphttp.logpak.input` to the `output` lists of the component that feeds
your current exporter.

Docs: [otelcol.exporter.otlphttp](https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.exporter.otlphttp/).

### Fluent Bit

```yaml
pipeline:
  outputs:
    - name: http
      match: '*'
      host: logpak-collect
      port: 4318
      uri: /fluent-bit
      format: json_lines
      compress: gzip
```

Its `opentelemetry` output also works, with `port: 4318`.

Docs: [http output](https://docs.fluentbit.io/manual/data-pipeline/outputs/http).

### Vector

```yaml
sinks:
  logpak:
    type: http
    inputs: [llm_logs]
    uri: http://logpak-collect:4318/vector
    encoding: { codec: json }
    framing: { method: newline_delimited }
    batch: { max_events: 1 }          # one event per record; omit to store batches
    compression: gzip
```

Without `framing`, a batch is a JSON array.

Docs: [http sink](https://vector.dev/docs/reference/configuration/sinks/http/).

### Logstash

```
output { http { url => "http://logpak-collect:4318/logstash" http_method => "post" format => "json" http_compression => true } }
```

### Filebeat

Filebeat has no HTTP or OTLP output. Ship through Logstash, or use its `file` output and pack the
result.

---

## Forwarding to an observability backend

`logpak collect --forward URL` sends each request on as received, with the exporter's headers,
after storing it. It appends the request's path (`/v1/traces`) to `URL`. gRPC exports go on as
OTLP/HTTP protobuf. Put the backend's auth in `OTEL_EXPORTER_OTLP_HEADERS` on the exporter; it is
forwarded and never stored.

| Backend | `--forward` | `OTEL_EXPORTER_OTLP_HEADERS` |
|---|---|---|
| Langfuse (EU / US / self-hosted) | `https://cloud.langfuse.com/api/public/otel` / `https://us.cloud.langfuse.com/api/public/otel` / `http://<host>:3000/api/public/otel` | `Authorization=Basic <base64 pk:sk>,x-langfuse-ingestion-version=4` |
| Arize Phoenix (self-hosted) | `http://<host>:6006` | `Authorization=Bearer <key>` (optional locally) |
| Arize AX | `https://otlp.arize.com` | `space_id=<id>,api_key=<key>` |
| LangSmith (US / EU) | `https://api.smith.langchain.com/otel` / `https://eu.api.smith.langchain.com/otel` | `x-api-key=<key>` |
| Braintrust | `https://api.braintrust.dev/otel` | `Authorization=Bearer <key>,x-bt-parent=project_id:<id>` |
| Opik (cloud / self-hosted) | `https://www.comet.com/opik/api/v1/private/otel` / `http://<host>:5173/api/v1/private/otel` | `Authorization=<key>,projectName=<p>,Comet-Workspace=<w>` |
| W&B Weave | `https://trace.wandb.ai/otel` | `wandb-api-key=<key>` |
| MLflow (3.6+) | `http://<host>:5000` | `x-mlflow-experiment-id=<id>` |
| Logfire (US / EU) | `https://logfire-us.pydantic.dev` / `https://logfire-eu.pydantic.dev` | `Authorization=<write token>` |
| Traceloop | `https://api.traceloop.com` | `Authorization=Bearer <key>` |
| Laminar | `https://api.lmnr.ai` | `Authorization=Bearer <project key>` |
| Datadog | your site's OTLP intake | `dd-api-key=<key>` (LLM Observability: `,dd-otlp-source=llmobs`) |
| Honeycomb (US / EU) | `https://api.honeycomb.io` / `https://api.eu1.honeycomb.io` | `x-honeycomb-team=<key>` |
| Grafana Cloud | `https://otlp-gateway-<zone>.grafana.net/otlp` | `Authorization=Basic <base64 instance:token>` |
| New Relic (US / EU) | `https://otlp.nr-data.net` / `https://otlp.eu01.nr-data.net` | `api-key=<license key>` |
| Dynatrace | `https://<env>.live.dynatrace.com/api/v2/otlp` | `Authorization=Bearer <platform token>` |
| SigNoz Cloud | `https://ingest.<region>.signoz.cloud:443` | `signoz-ingestion-key=<key>` |
| ClickStack / HyperDX | `http://<host>:4318` | `authorization=<ingestion key>` |
| Elastic | your managed OTLP endpoint | `Authorization=ApiKey <key>` |
| Another OpenTelemetry Collector | `http://<host>:4318` | as configured |

Watch for these:

- **Body limits.** New Relic rejects payloads over 1 MB, and Datadog caps at 15 MiB uncompressed.
  Long agent spans can exceed both.
- **Encodings.** Dynatrace and Weave take protobuf only, so have the exporter send
  `http/protobuf`, not `http/json`.
- **Splunk Observability Cloud** uses `/v2/trace/otlp`, not `/v1/traces`, so `--forward` cannot
  reach it. Tee through an OpenTelemetry Collector instead.

---

## Self-hosted inference servers

Record what is sent to a local model with B. `--upstream` is the server's origin, and the client
keeps its usual path.

| Server | `--upstream` | Client base URL via Logpak | Its own prompt logging |
|---|---|---|---|
| vLLM | `http://localhost:8000` | `http://127.0.0.1:8080/v1` | `--enable-log-requests`, prompts only at debug level, truncated; OTel spans carry no prompt text |
| SGLang | `http://localhost:30000` | `.../v1` | `--log-requests --log-requests-level 3` (full) |
| Ollama | `http://localhost:11434` | `.../v1` | none |
| llama.cpp `llama-server` | `http://localhost:9931` (newer default; often run with `--port 8080`) | `.../v1` | `--log-prompts-dir` (debugging) |
| Hugging Face TGI | `http://localhost:3000` | `.../v1` | raise `--payload-limit` (2 MB default) for long prompts |
| LM Studio | `http://localhost:1234` | `.../v1` | `lms log stream --json` |
| LocalAI | `http://localhost:8080` (run Logpak on another port) | `.../v1` | UI tracing of recent calls |
| NVIDIA NIM | `http://localhost:8000` | `.../v1` | OTel via `NIM_ENABLE_OTEL` (1.x); content *(unverified)* |
| Triton OpenAI frontend | `http://localhost:9000` | `.../v1` | none |
| KServe | the service URL | `.../openai/v1` | none |
| text-generation-webui | `http://localhost:5000` | `.../v1` | none |

`lms log stream --json` emits JSON lines, so LM Studio's own log packs directly.

## Provider upstreams

| Provider | `--upstream` | Client base URL via Logpak | Auth header (passed through) |
|---|---|---|---|
| Anthropic | `https://api.anthropic.com` | `http://127.0.0.1:8080` | `x-api-key` or `Authorization: Bearer` |
| OpenAI | `https://api.openai.com` | `http://127.0.0.1:8080/v1` | `Authorization: Bearer` |
| Azure OpenAI | `https://<resource>.openai.azure.com` | `http://127.0.0.1:8080/openai/v1/` | `api-key` |
| Gemini (native) | `https://generativelanguage.googleapis.com` | `http://127.0.0.1:8080` | `x-goog-api-key` |
| Gemini (OpenAI-compatible) | `https://generativelanguage.googleapis.com` | `http://127.0.0.1:8080/v1beta/openai/` | `Authorization: Bearer` |
| OpenRouter | `https://openrouter.ai` | `http://127.0.0.1:8080/api/v1` | `Authorization: Bearer` |
| Groq | `https://api.groq.com` | `http://127.0.0.1:8080/openai/v1` | `Authorization: Bearer` |
| Together | `https://api.together.ai` | `http://127.0.0.1:8080/v1` | `Authorization: Bearer` |
| Fireworks | `https://api.fireworks.ai` | `http://127.0.0.1:8080/inference/v1` | `Authorization: Bearer` |
| Mistral | `https://api.mistral.ai` | `http://127.0.0.1:8080` (SDK) or `/v1` (OpenAI-style clients) | `Authorization: Bearer` |
| DeepSeek | `https://api.deepseek.com` | `http://127.0.0.1:8080` (Anthropic format: `/anthropic`) | `Authorization: Bearer` |
| xAI | `https://api.x.ai` | `http://127.0.0.1:8080/v1` | `Authorization: Bearer` |
| Cerebras | `https://api.cerebras.ai` | `http://127.0.0.1:8080/v1` | `Authorization: Bearer` |

AWS Bedrock is not on this list: its SigV4 signature covers the host name. Use its
[invocation logs](#aws-bedrock).

## Not supported

| Tool | Why | Instead |
|---|---|---|
| Cursor | Requests with your own key are "routed through Cursor's servers for final prompt building", so a local base URL is never called | none locally |
| Amp | Bring-your-own-key was removed; inference goes through Amp's servers | none |
| Codex over WebSocket | `logpak server` does not proxy WebSocket | a custom provider without `supports_websockets` |
| Splunk Observability Cloud as `--forward` | its OTLP path is not `/v1/traces` | tee through an OpenTelemetry Collector |

Missing a tool, or found a key that changed? Open an issue or a pull request against this file.
