# Deltapack

**A compression codec for LLM logs and traces. About 100× smaller, byte-exact, and any record
reads back on its own.**

Every LLM call is logged with its whole prompt, and every prompt is the previous prompt plus one
turn. A 200-step agent run writes its system prompt and tool schemas into your log 200 times, its
first tool result 199 times, its second 198 times. Most of what a gateway log, a trace store or an
audit archive holds is copies of what it already has.

```
what your log holds                                 what deltapack stores

span 1  [system][tools][turn 1]                     [system][tools][turn 1]
span 2  [system][tools][turn 1][turn 2]             + [turn 2]
span 3  [system][tools][turn 1][turn 2][turn 3]     + [turn 3]
```

Deltapack stores each record as a delta against the earlier record it shares the most with, so a
record costs about its new bytes. Here are 7,343 OTel GenAI spans from 300 public SWE-agent runs,
16 traces in flight, 337 MB of JSON lines:

| Stored as | Size | Ratio |
|---|---:|---:|
| zstd-1 in 1 MiB blocks, arrival order (how a column store compresses) | 34.6 MB | 9.7× |
| messages deduplicated per trace, then zstd-1 blocks | 14.6 MB | 23× |
| **Deltapack, one record per span** | **3.2 MB** | **105×** |

A block compressor loses here because it only sees one block. With 16 agents running, the previous
copy of a prompt usually sits in a different block, and a late prompt is larger than a block
anyway. Deltapack looks for the previous copy by record, whatever its size.

At that ratio a terabyte of spans is under 10 GB. You can afford to keep every prompt in full,
without sampling or truncating, and still pull any single request out in milliseconds.

## What the files are for

- **Replay a failure.** For requests recorded by `deltapack server --record-dir`, `deltapack log get`
  writes one byte for byte, as the provider received it: post it again, or step through what the
  agent saw.
- **Find a trace.** `deltapack log show --trace <id>` writes each of its spans as a line of JSON, its
  prompt and attributes decoded from OTLP protobuf or JSON.
- **Build an eval or fine-tuning set.** `deltapack log show --since 7d ~/deltapack/*.dpk` into `jq`, DuckDB or pandas:
  every prompt in full, not sampled or truncated.
- **Answer an auditor.** Keep every request for as long as you must (`--keep-days`), or hand each
  closed file to your archive (`--on-rotate`), at about a hundredth of the bytes.
- **Know what it saves.** `deltapack log stats` gives the ratio on your own traffic.

## Try it in a minute

With `deltapack` and `jq` on your path, and port 4318 free, this posts three turns of one agent's trace as an OTLP exporter
would, then reads them back:

```bash
rm -rf ./deltapack
deltapack collect --record-dir ./deltapack > /dev/null 2>&1 &
sleep 1
system=$(printf 'You are a careful coding agent. %.0s' {1..200})
history=""
for turn in 1 2 3; do
  history="$history step $turn."
  curl -s -o /dev/null http://127.0.0.1:4318/v1/traces -H 'content-type: application/json' -d '{"resourceSpans":[{
    "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"demo-agent"}}]},
    "scopeSpans":[{"spans":[{"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"000000000000000'$turn'",
    "name":"chat","attributes":[{"key":"gen_ai.prompt","value":{"stringValue":"'"$system$history"'"}}]}]}]}]}'
done
deltapack log stats ./deltapack/*.dpk
deltapack log show --trace 4bf92f3577b34da6a3ce929d0e0e4736 ./deltapack/*.dpk |
  jq -c '{record, name, service: .resource["service.name"], ends: .attributes["gen_ai.prompt"][-24:]}'
kill %1
```

```
3 records, 20166 bytes in 760 on disk (26.5x)	./deltapack/1795474283580-31119-000000.dpk
{"record":0,"name":"chat","service":"demo-agent","ends":"l coding agent.  step 1."}
{"record":1,"name":"chat","service":"demo-agent","ends":" agent.  step 1. step 2."}
{"record":2,"name":"chat","service":"demo-agent","ends":" step 1. step 2. step 3."}
```

Each turn re-sends the whole prompt; each record after the first stores only the turn it adds.

## Quick start: point your OpenTelemetry exporter at Deltapack

Your agent, framework or gateway most likely exports OpenTelemetry already. Run a collector, point
the exporter at it, and every span and log record is stored on your disk.

```bash
cargo install deltapack --features proxy
```

Or run the image `ghcr.io/aperiodic-io/deltapack`. From the next release on, each release also carries
a binary for Linux (x86_64 and aarch64, static musl, any distribution) on its
[release page](https://github.com/aperiodic-io/deltapack/releases). musl's allocator is slower under
many threads: for the most throughput, build with cargo or use the image. On macOS, use cargo.

```bash
deltapack collect --record-dir ~/deltapack        # OTLP/HTTP on 127.0.0.1:4318, OTLP/gRPC on :4317
```

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf   # or grpc, with http://127.0.0.1:4317
```

Run your agent as usual, then look at what arrived:

```bash
deltapack log stats ~/deltapack/*.dpk       # per file: records, their bytes, bytes on disk, ratio
```

```bash
deltapack log show ~/deltapack/*.dpk        # one JSON line per span or log record
```

`log ls` lists the records and `log unpack` writes them back byte for byte
([Reading it back](#reading-it-back)).

It takes OTLP traces, logs and metrics, as protobuf or JSON, compressed or not (gzip, deflate,
brotli, zstd, snappy). Nothing sits in the path of your model calls: unset the variable and
Deltapack is out of the picture.

### Turn on the content

Most exporters leave prompts out until you ask for them. The flags for the most common tools:

**Claude Code** exports the whole Messages API request, history included:

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1 OTEL_LOGS_EXPORTER=otlp
export OTEL_LOG_RAW_API_BODIES=1 CLAUDE_CODE_OTEL_CONTENT_MAX_LENGTH=67108864
```

**Python apps** (OpenAI, Anthropic, LangChain, LlamaIndex, CrewAI, …), with the official
OpenTelemetry GenAI instrumentations and no code changes:

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http opentelemetry-instrumentation-genai-openai
```

```bash
OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=span_only OTEL_LOGS_EXPORTER=otlp opentelemetry-instrument python app.py
```

**Other tools.** Each line goes with the endpoint variables above:

| Tool | Content flag |
|---|---|
| Gemini CLI | `"telemetry": {"enabled": true, "otlpProtocol": "grpc", "otlpEndpoint": "http://127.0.0.1:4317", "logPrompts": true}` in `.gemini/settings.json` |
| GitHub Copilot CLI | `OTEL_INSTRUMENTATION_GENAI_CAPTURE_CONTENT=SPAN_ONLY` |
| VS Code Copilot | `github.copilot.chat.otel.enabled` and `github.copilot.chat.otel.captureContent` set to `true` |
| Goose | `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=true` |
| OpenLLMetry (Traceloop) | content is on; set `TRACELOOP_BASE_URL=http://127.0.0.1:4318` |
| OpenInference (Arize) | content is on |
| Vercel AI SDK | inputs and outputs are recorded by default |
| LiteLLM proxy | `callbacks: ["otel"]`; prompts are on |
| Microsoft Agent Framework | `ENABLE_SENSITIVE_DATA=true` |
| Semantic Kernel (Python) | `SEMANTICKERNEL_EXPERIMENTAL_GENAI_ENABLE_OTEL_DIAGNOSTICS_SENSITIVE=true` |

[docs/integrations.md](docs/integrations.md) has the full setup for each of 20 coding agents, 25 SDKs
and agent frameworks, 15 gateways, AWS Bedrock, Azure and Vertex AI logs, 6 telemetry pipelines and
20 backends. It also says, per tool, whether its export carries the full request, the messages,
only the prompts, or no content at all.

### Keep your current backend

Put Deltapack between the exporter and the backend you query. Every batch is stored, then forwarded as
received, headers included (they are never stored). The backend can sample, truncate or expire what
it likes, and you still hold every span.

```bash
deltapack collect --record-dir ~/deltapack --forward https://cloud.langfuse.com/api/public/otel
```

```bash
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(echo -n "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" | base64 -w0)"
```

| Backend | `--forward` | `OTEL_EXPORTER_OTLP_HEADERS` |
|---|---|---|
| Langfuse | `https://cloud.langfuse.com/api/public/otel` | `Authorization=Basic <base64 pk:sk>` |
| Arize Phoenix | `http://<host>:6006` | `Authorization=Bearer <key>` |
| LangSmith | `https://api.smith.langchain.com/otel` | `x-api-key=<key>` |
| Honeycomb | `https://api.honeycomb.io` | `x-honeycomb-team=<key>` |
| Grafana Cloud | `https://otlp-gateway-<zone>.grafana.net/otlp` | `Authorization=Basic <base64 instance:token>` |
| An OpenTelemetry Collector | `http://<host>:4318` | as configured |

More backends are in [the guide](docs/integrations.md#forwarding-to-an-observability-backend).
`--forward` is an OTLP/HTTP endpoint. A gRPC export goes on to it as OTLP/HTTP protobuf, and the
backend's answer comes back as the gRPC status (429 and 502–504 as `UNAVAILABLE`, which exporters
retry). So a gRPC exporter can feed a backend that takes only OTLP/HTTP, such as Langfuse. gRPC
arrives as HTTP/2 without TLS, as `insecure` exporters send it; put a proxy in front for TLS.

## Integrations

Step-by-step setup for each tool, in [docs/integrations.md](docs/integrations.md). Hover a name
to see what its OpenTelemetry export carries; where it carries no prompts, the guide sets up the
recording proxy instead.

<!-- integrations:start: generated from docs/integrations.md's headings -->
**Coding agents**

<table>
<tr><td width="25%"><a href="docs/integrations.md#claude-code" title="Over OTel: full request"><img src="https://avatars.githubusercontent.com/u/76263028?s=40" width="20" height="20" alt=""> Claude Code</a></td><td width="25%"><a href="docs/integrations.md#claude-agent-sdk" title="Over OTel: full request"><img src="https://avatars.githubusercontent.com/u/76263028?s=40" width="20" height="20" alt=""> Claude Agent SDK</a></td><td width="25%"><a href="docs/integrations.md#gemini-cli" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/161781182?s=40" width="20" height="20" alt=""> Gemini CLI</a></td><td width="25%"><a href="docs/integrations.md#qwen-code" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/141221163?s=40" width="20" height="20" alt=""> Qwen Code</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#github-copilot-cli" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/9919?s=40" width="20" height="20" alt=""> GitHub Copilot CLI</a></td><td width="25%"><a href="docs/integrations.md#vs-code-copilot-chat" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/9919?s=40" width="20" height="20" alt=""> VS Code Copilot</a></td><td width="25%"><a href="docs/integrations.md#goose" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/271095942?s=40" width="20" height="20" alt=""> Goose</a></td><td width="25%"><a href="docs/integrations.md#openai-codex-cli" title="Over OTel: prompts only"><img src="https://avatars.githubusercontent.com/u/14957082?s=40" width="20" height="20" alt=""> Codex CLI</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#openhands" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/225919603?s=40" width="20" height="20" alt=""> OpenHands</a></td><td width="25%"><a href="docs/integrations.md#kilo-code" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/201822503?s=40" width="20" height="20" alt=""> Kilo Code</a></td><td width="25%"><a href="docs/integrations.md#cline-and-roo-code" title="Over OTel: metadata only"><img src="https://avatars.githubusercontent.com/u/184127137?s=40" width="20" height="20" alt=""> Cline</a></td><td width="25%"><a href="docs/integrations.md#cline-and-roo-code" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/211522643?s=40" width="20" height="20" alt=""> Roo Code</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#opencode" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/66570915?s=40" width="20" height="20" alt=""> OpenCode</a></td><td width="25%"><a href="docs/integrations.md#aider" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/172139148?s=40" width="20" height="20" alt=""> Aider</a></td><td width="25%"><a href="docs/integrations.md#continue" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/127876214?s=40" width="20" height="20" alt=""> Continue</a></td><td width="25%"><a href="docs/integrations.md#swe-agent-and-mini-swe-agent" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/166046056?s=40" width="20" height="20" alt=""> SWE-agent</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#crush" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/57376114?s=40" width="20" height="20" alt=""> Crush</a></td><td width="25%"><a href="docs/integrations.md#pi" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/207902832?s=40" width="20" height="20" alt=""> Pi</a></td><td width="25%"><a href="docs/integrations.md#zed" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/79345384?s=40" width="20" height="20" alt=""> Zed</a></td><td width="25%"></td></tr>
</table>

**Personal and autonomous agents**

<table>
<tr><td width="25%"><a href="docs/integrations.md#openclaw" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/252820863?s=40" width="20" height="20" alt=""> OpenClaw</a></td><td width="25%"><a href="docs/integrations.md#hermes-agent" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/134168893?s=40" width="20" height="20" alt=""> Hermes Agent</a></td><td width="25%"><a href="docs/integrations.md#nanobot" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/118165258?s=40" width="20" height="20" alt=""> nanobot</a></td><td width="25%"><a href="docs/integrations.md#open-interpreter" title="Over OTel: prompts only"><img src="https://avatars.githubusercontent.com/u/163192481?s=40" width="20" height="20" alt=""> Open Interpreter</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#letta" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/177780362?s=40" width="20" height="20" alt=""> Letta</a></td><td width="25%"><a href="docs/integrations.md#agent-zero" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/216033749?s=40" width="20" height="20" alt=""> Agent Zero</a></td><td width="25%"><a href="docs/integrations.md#openmanus" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/198047230?s=40" width="20" height="20" alt=""> OpenManus</a></td><td width="25%"><a href="docs/integrations.md#autogpt" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/130738209?s=40" width="20" height="20" alt=""> AutoGPT</a></td></tr>
</table>

**Agent workflow platforms**

<table>
<tr><td width="25%"><a href="docs/integrations.md#n8n" title="Over OTel: messages (Agents feature)"><img src="https://avatars.githubusercontent.com/u/45487711?s=40" width="20" height="20" alt=""> n8n</a></td><td width="25%"><a href="docs/integrations.md#dify" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/127165244?s=40" width="20" height="20" alt=""> Dify</a></td><td width="25%"><a href="docs/integrations.md#langflow" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/85702467?s=40" width="20" height="20" alt=""> Langflow</a></td><td width="25%"><a href="docs/integrations.md#flowise" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/128289781?s=40" width="20" height="20" alt=""> Flowise</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#open-webui" title="Over OTel: metadata only"><img src="https://avatars.githubusercontent.com/u/158137808?s=40" width="20" height="20" alt=""> Open WebUI</a></td><td width="25%"><a href="docs/integrations.md#librechat" title="Over OTel: metadata only"><img src="https://avatars.githubusercontent.com/u/110412045?s=40" width="20" height="20" alt=""> LibreChat</a></td><td width="25%"><a href="docs/integrations.md#anythingllm" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/134426827?s=40" width="20" height="20" alt=""> AnythingLLM</a></td><td width="25%"><a href="docs/integrations.md#activepieces" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/99494700?s=40" width="20" height="20" alt=""> Activepieces</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#rivet" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/20408767?s=40" width="20" height="20" alt=""> Rivet</a></td><td width="25%"></td><td width="25%"></td><td width="25%"></td></tr>
</table>

**SDKs and agent frameworks**

<table>
<tr><td width="25%"><a href="docs/integrations.md#opentelemetry-genai-instrumentations-python" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/49998002?s=40" width="20" height="20" alt=""> OpenTelemetry GenAI</a></td><td width="25%"><a href="docs/integrations.md#openllmetry-traceloop" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/125419530?s=40" width="20" height="20" alt=""> OpenLLMetry</a></td><td width="25%"><a href="docs/integrations.md#openinference-arize" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/59858760?s=40" width="20" height="20" alt=""> OpenInference</a></td><td width="25%"><a href="docs/integrations.md#langfuse-sdk" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/134601687?s=40" width="20" height="20" alt=""> Langfuse</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#langsmith-langchain-langgraph" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/126733545?s=40" width="20" height="20" alt=""> LangSmith</a></td><td width="25%"><a href="docs/integrations.md#pydantic-logfire-and-pydantic-ai" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/110818415?s=40" width="20" height="20" alt=""> Pydantic AI, Logfire</a></td><td width="25%"><a href="docs/integrations.md#langchain-and-langgraph" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/126733545?s=40" width="20" height="20" alt=""> LangChain, LangGraph</a></td><td width="25%"><a href="docs/integrations.md#llamaindex" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/130722866?s=40" width="20" height="20" alt=""> LlamaIndex</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#openai-agents-sdk" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/14957082?s=40" width="20" height="20" alt=""> OpenAI Agents SDK</a></td><td width="25%"><a href="docs/integrations.md#vercel-ai-sdk" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/14985020?s=40" width="20" height="20" alt=""> Vercel AI SDK</a></td><td width="25%"><a href="docs/integrations.md#mastra" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/149120496?s=40" width="20" height="20" alt=""> Mastra</a></td><td width="25%"><a href="docs/integrations.md#crewai" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/170677839?s=40" width="20" height="20" alt=""> CrewAI</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#autogen" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/6154722?s=40" width="20" height="20" alt=""> AutoGen</a></td><td width="25%"><a href="docs/integrations.md#microsoft-agent-framework" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/6154722?s=40" width="20" height="20" alt=""> Microsoft Agent Framework</a></td><td width="25%"><a href="docs/integrations.md#semantic-kernel" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/6154722?s=40" width="20" height="20" alt=""> Semantic Kernel</a></td><td width="25%"><a href="docs/integrations.md#spring-ai" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/317776?s=40" width="20" height="20" alt=""> Spring AI</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#strands-agents" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/209155962?s=40" width="20" height="20" alt=""> Strands Agents</a></td><td width="25%"><a href="docs/integrations.md#google-adk" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/1342004?s=40" width="20" height="20" alt=""> Google ADK</a></td><td width="25%"><a href="docs/integrations.md#smolagents" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/25720743?s=40" width="20" height="20" alt=""> smolagents</a></td><td width="25%"><a href="docs/integrations.md#haystack" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/51827949?s=40" width="20" height="20" alt=""> Haystack</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#dspy-agno" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/3046006?s=40" width="20" height="20" alt=""> DSPy</a></td><td width="25%"><a href="docs/integrations.md#dspy-agno" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/104874993?s=40" width="20" height="20" alt=""> Agno</a></td><td width="25%"><a href="docs/integrations.md#langchain4j-genkit" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/132277850?s=40" width="20" height="20" alt=""> LangChain4j</a></td><td width="25%"><a href="docs/integrations.md#langchain4j-genkit" title="Over OTel: no OTel"><img src="https://avatars.githubusercontent.com/u/1335026?s=40" width="20" height="20" alt=""> Genkit</a></td></tr>
</table>

**LLM gateways**

<table>
<tr><td width="25%"><a href="docs/integrations.md#litellm-proxy" title="Over OTel: payload logs"><img src="https://avatars.githubusercontent.com/u/121462774?s=40" width="20" height="20" alt=""> LiteLLM</a></td><td width="25%"><a href="docs/integrations.md#kong-ai-gateway" title="Over OTel: payload logs"><img src="https://avatars.githubusercontent.com/u/962416?s=40" width="20" height="20" alt=""> Kong AI Gateway</a></td><td width="25%"><a href="docs/integrations.md#portkey-open-source-gateway" title="Over OTel: no export"><img src="https://avatars.githubusercontent.com/u/131141116?s=40" width="20" height="20" alt=""> Portkey</a></td><td width="25%"><a href="docs/integrations.md#apache-apisix" title="Over OTel: payload logs"><img src="https://avatars.githubusercontent.com/u/47359?s=40" width="20" height="20" alt=""> Apache APISIX</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#bifrost" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/139708451?s=40" width="20" height="20" alt=""> Bifrost</a></td><td width="25%"><a href="docs/integrations.md#tensorzero" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/148420822?s=40" width="20" height="20" alt=""> TensorZero</a></td><td width="25%"><a href="docs/integrations.md#agentgateway" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/208695296?s=40" width="20" height="20" alt=""> agentgateway</a></td><td width="25%"><a href="docs/integrations.md#envoy-ai-gateway-agent-router" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/314453756?s=40" width="20" height="20" alt=""> Envoy AI Gateway</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#higress" title="Over OTel: fields only"><img src="https://avatars.githubusercontent.com/u/116630909?s=40" width="20" height="20" alt=""> Higress</a></td><td width="25%"><a href="docs/integrations.md#traefik-hub-ai-gateway" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/14280338?s=40" width="20" height="20" alt=""> Traefik Hub</a></td><td width="25%"><a href="docs/integrations.md#openrouter" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/139423088?s=40" width="20" height="20" alt=""> OpenRouter</a></td><td width="25%"><a href="docs/integrations.md#cloudflare-ai-gateway" title="Over OTel: messages"><img src="https://avatars.githubusercontent.com/u/314135?s=40" width="20" height="20" alt=""> Cloudflare AI Gateway</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#helicone" title="Over OTel: no export"><img src="https://avatars.githubusercontent.com/u/114524975?s=40" width="20" height="20" alt=""> Helicone</a></td><td width="25%"><a href="docs/integrations.md#vercel-ai-gateway" title="Over OTel: metadata only"><img src="https://avatars.githubusercontent.com/u/14985020?s=40" width="20" height="20" alt=""> Vercel AI Gateway</a></td><td width="25%"><a href="docs/integrations.md#mlflow-ai-gateway" title="Over OTel: unverified"><img src="https://avatars.githubusercontent.com/u/39938107?s=40" width="20" height="20" alt=""> MLflow AI Gateway</a></td><td width="25%"></td></tr>
</table>

**Cloud platform logs**

<table>
<tr><td width="25%"><a href="docs/integrations.md#aws-bedrock" title="Over OTel: invocation logs"><img src="https://avatars.githubusercontent.com/u/2232217?s=40" width="20" height="20" alt=""> AWS Bedrock</a></td><td width="25%"><a href="docs/integrations.md#azure-api-management-azure-openai" title="Over OTel: LLM logs"><img src="https://avatars.githubusercontent.com/u/6844498?s=40" width="20" height="20" alt=""> Azure API Management</a></td><td width="25%"><a href="docs/integrations.md#google-vertex-ai" title="Over OTel: request-response logs"><img src="https://avatars.githubusercontent.com/u/2810941?s=40" width="20" height="20" alt=""> Google Vertex AI</a></td><td width="25%"></td></tr>
</table>

**Telemetry pipelines**

<table>
<tr><td width="25%"><a href="docs/integrations.md#opentelemetry-collector" title="Over OTel: tee"><img src="https://avatars.githubusercontent.com/u/49998002?s=40" width="20" height="20" alt=""> OpenTelemetry Collector</a></td><td width="25%"><a href="docs/integrations.md#grafana-alloy" title="Over OTel: tee"><img src="https://avatars.githubusercontent.com/u/7195757?s=40" width="20" height="20" alt=""> Grafana Alloy</a></td><td width="25%"><a href="docs/integrations.md#fluent-bit" title="Over OTel: tee"><img src="https://avatars.githubusercontent.com/u/859518?s=40" width="20" height="20" alt=""> Fluent Bit</a></td><td width="25%"><a href="docs/integrations.md#vector" title="Over OTel: tee"><img src="https://avatars.githubusercontent.com/u/16866914?s=40" width="20" height="20" alt=""> Vector</a></td></tr>
<tr><td width="25%"><a href="docs/integrations.md#logstash" title="Over OTel: tee"><img src="https://avatars.githubusercontent.com/u/6764390?s=40" width="20" height="20" alt=""> Logstash</a></td><td width="25%"></td><td width="25%"></td><td width="25%"></td></tr>
</table>
<!-- integrations:end -->

## Deploy it

- **Docker Compose:** [deploy/docker-compose.yml](deploy/docker-compose.yml) runs `deltapack collect`
  in front of Arize Phoenix: exporters send to `localhost:4318`, Phoenix shows the traces, deltapack
  keeps every span whole for 30 days.
- **Kubernetes:** [deploy/k8s/deltapack.yaml](deploy/k8s/deltapack.yaml) is a StatefulSet with its
  record directory on a persistent volume, unprivileged and with a read-only root, behind a Service
  on 4318 and 4317, with metrics on 9464.

CI runs both on every pull request: the compose stack with spans checked in Phoenix and in the
recording, the manifest on a kind cluster.

## Other ways in

OpenTelemetry covers most tools. For the rest:

| You have | Use | What changes in your stack |
|---|---|---|
| A gateway with an HTTP log plugin (Kong, LiteLLM) | [`deltapack collect`](#as-your-gateways-log-sink) | one plugin setting |
| A tool whose export leaves the prompts out, or no export at all | [`deltapack server --record-dir`](#as-a-recording-proxy) | the base URL |
| Files of exported spans or request logs | [`deltapack log pack`](#on-files-you-already-have) | nothing |
| Your own ingest path or notebook | [the Rust or Python library](#embedded-in-rust-or-python) | a few lines |

Every route writes the same DPK1 files, and [`deltapack log`](#reading-it-back) and both libraries
read them all.

### As your gateway's log sink

Anything that can POST a log entry can log to `deltapack collect`. The path is yours to choose and
is stored with each record.

```bash
deltapack collect --record-dir /var/log/deltapack
```

**LiteLLM**: the `generic_api` callback, with `GENERIC_LOGGER_ENDPOINT=http://deltapack-collect:4318/litellm`.

```yaml
litellm_settings:
  callbacks: ["generic_api"]
```

**Kong AI Gateway**: log payloads from `ai-proxy` and send each entry with `http-log`. Agent
prompts are larger than Kong's defaults allow: raise `max_request_body_size`, and the body buffer
with `KONG_NGINX_HTTP_CLIENT_BODY_BUFFER_SIZE=16m` (a body Kong spills to a temp file is answered
400, "request body doesn't contain valid prompts").

```yaml
plugins:
  - name: ai-proxy
    config:
      max_request_body_size: 16777216
      logging: { log_payloads: true, log_statistics: true }
      # route_type, auth, model: as you have them
  - name: http-log
    config:
      http_endpoint: http://deltapack-collect:4318/kong
      content_type: application/json
```

### As a recording proxy

For a tool whose OpenTelemetry export leaves the prompts out (Codex, Cline, Aider, OpenCode), a
gateway that exports no logs (Portkey), or for the exact request bytes a provider received, let
`deltapack server` record what it forwards. Each request body is on disk before it reaches the
upstream, so the file is a record of what the model was asked. If a write fails, the error is
logged and the request is still forwarded.

```bash
deltapack server --upstream https://api.anthropic.com --record-dir /var/log/deltapack
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude
```

API keys pass straight through. Headers are never written to the file.

WebSocket sessions (realtime and voice APIs) go through too: the server passes the upgrade to the
upstream (`ws://` or `wss://`, by `--upstream`'s scheme), with the tool's headers and subprotocols,
and stores each text or binary message as a record of its own as it passes, `WS>` towards the
upstream and `WS<` back, after the time in its meta (`deltapack log ls` shows them in order). A
handshake the upstream refuses is refused to the tool with the same status; one it cannot reach,
with 502, or 504 after 30 s. A message is limited to `--max-body`. The tool is pinged every 30 s,
and a tool silent for 75 s (no message, no pong) has its session, and the upstream's, ended.

### On files you already have

One JSON object per line in, a `.dpk` file out: exported spans, a gateway's JSON-lines log, an
eval run's requests. Pack before you ship to object storage and the bucket holds about a hundredth
of the bytes.

```bash
deltapack log pack spans.dpk --window 32 < exported-spans.jsonl
```

```bash
deltapack log unpack spans.dpk | cmp - exported-spans.jsonl    # silent: every byte is back
```

`--window` is how many traces or conversations are interleaved in the file (default 16).

### Embedded in Rust or Python

Rust: `deltapack::log::Writer` appends, `deltapack::log::Reader` indexes and reads.

```rust
let mut log = deltapack::log::Writer::new(std::io::BufWriter::new(file), 16)?; // window
log.append(b"2026-11-20T14:15:00Z trace=ab12", body)?;                         // meta, body
```

Python ([python/](python/README.md)): the same files, from an ingest path or a notebook. `append`
and reads release the GIL.

```python
import deltapack

with deltapack.Writer("spans.dpk", window=16) as w:
    w.append(span_bytes, meta=b"trace=ab12")

for meta, body in deltapack.Reader("/var/log/deltapack/1763650000000-4242-000000.dpk"):
    ...

import pandas as pd                                   # the spans, decoded, as `log show` writes them
spans = pd.DataFrame(deltapack.Reader("/var/log/deltapack/1763650000000-4242-000000.dpk").spans())
spans.to_parquet("spans.parquet")
```

`pip install deltapack-cli` installs the `deltapack` command itself, from a wheel of its own
(`python/cli/`).

### Reading it back

```bash
deltapack log ls /var/log/deltapack/1763650000000-4242-000000.dpk       # index, base, bytes stored, meta
```

```bash
deltapack log get /var/log/deltapack/1763650000000-4242-000000.dpk 17 > request-17.json
```

`get` writes one record byte for byte, which for a recorded request is the exact body the provider
received: post it again to reproduce a failure, or feed a day of them to an eval. `unpack` writes
them all. `collect` and `server` stamp each record with its time (Unix ms), method, path, content
type and the encoding the body is still in. `collect` adds the trace ids of each OTLP trace or log
batch.

`show` writes records as JSON lines: one per span or log record of an OTLP export, JSON or
protobuf, with its attributes as plain JSON; any other record whole, as `body`. Each line names its
file, its record and the time it was received.

```bash
deltapack log show --trace 4bf92f3577b34da6a3ce929d0e0e4736 ~/deltapack/*.dpk     # one trace, every span
```

```bash
deltapack log show --since 2h ~/deltapack/*.dpk | jq -r 'select(.name == "chat") | .attributes["gen_ai.request.model"]'
```

```bash
duckdb -c "SELECT name, count(*) FROM read_json('spans.jsonl') GROUP BY name"   # after: deltapack log show … > spans.jsonl
duckdb -c "COPY (SELECT * FROM read_json('spans.jsonl')) TO 'spans.parquet'"     # Parquet, for any warehouse
```

`--trace` skips the batches whose meta does not list the trace. Trace ids are hex, as OTLP/JSON
requires; the spans of an exporter that writes them in base64 are shown, but `--trace` cannot find
them. `--since` and `--until` take Unix ms or a duration ago (`90s`, `15m`, `2h`, `7d`).

`--attr KEY=VALUE` keeps the spans and log records whose attribute, or resource attribute, has that
value: a session, a model, a user (repeat it to need all of them). Numbers match by value (`1` and
`1.0` alike); arrays and objects only as their exact JSON.

```bash
deltapack log show --attr gen_ai.request.model=gpt-4o --attr session.id=s-42 ~/deltapack/*.dpk
```

A tracing backend shows a span's attributes, often truncated. With `--read-listen`, the collector
serves each trace's spans with their prompts whole, so the backend can link a span to them (a
Grafana data link, a Langfuse or Datadog custom link): `http://deltapack:4319/v1/traces/${traceId}`.

```bash
deltapack collect --record-dir ~/deltapack --read-listen 127.0.0.1:4319
curl -s http://127.0.0.1:4319/v1/traces/4bf92f3577b34da6a3ce929d0e0e4736 | jq .name
```

Each read scans the directory's files, at most 4 at once (one more is answered 503), and an answer
is at most `--max-body` bytes (413 past it: use `log show`). The token is one for every tenant:
whoever holds it reads any tenant's prompts with `?tenant=` (`_none` and `_other` included), so
give it only to what may read them all.

`stats` says what the files save: records, the bytes they stand for, the bytes on disk, and the
ratio, per file (its name last, after a tab) and in total. A file it cannot read is named on stderr
and skipped, and the command fails once the others are counted.

```bash
deltapack log stats ~/deltapack/*.dpk
```

Before you run a collector, `estimate` says what your own export would save: JSON lines on stdin,
nothing written.

```bash
deltapack log estimate --usd-per-gb 0.10 < exported-spans.jsonl
```

## What you can rely on

- It is lossless. Every record carries the SHA-256 of its body and is checked when read. A file
  that does not rebuild what it names is an error, never wrong bytes.
- Any record reads on its own, in 64 ms on the benchmark above, without unpacking the file. The
  chain of deltas behind a record is bounded.
- A crash loses at most the record being written. A file that ends mid-record still holds every
  record before it, and each record is flushed before its request goes on.
- Files are self-contained and rotate at `--rotate-bytes` (1 GiB). A restart starts a new file and
  never appends to or replaces an old one. `--keep-bytes` and `--keep-days` delete the oldest, and
  `--on-rotate` hands each closed file to a command of yours, such as one that moves it to object
  storage. A directory is recorded to by one process at a time: a second one refuses to start (on a file
  system that cannot lock a directory, such as some network ones, a warning says so instead).
- Compressed bodies (gzip, deflate, brotli, zstd, snappy) are stored decoded, so the delta sees the content. Headers, and so credentials, are
  never stored.
- Recording never takes your traffic down: a failed write is counted in
  `deltapack_record_failures_total`, and the request goes on.
- The format is small and public ([SPEC.md §7](SPEC.md#7-dpk1-a-log-of-bodies-stored-as-deltas)):
  a file of Zstandard frames, each behind a few varints.

### Measured end to end

300 public SWE-agent runs, 16 traces in flight, sent by the OpenTelemetry Python SDK's OTLP
exporter (protobuf and gzip) through `collect` into the real backends:

| Pipeline | What arrived | Stored |
|---|---|---:|
| SDK → `collect --forward` → Langfuse 3.227 | all 7,643 spans, in Langfuse and in the recording | **3.26 MB** of DPK1 for 338.6 MB of protobuf (104×). Langfuse itself kept 31.6 MiB in ClickHouse (ZSTD(3)) and 356 MB of raw events in S3 |
| SDK → `collect --forward` → OpenTelemetry Collector 0.115 | all 7,343 chat spans, the same span ids at both ends | 2.63 MB for 338.5 MB (129×) |
| SDK over gRPC (gzip) → `collect --forward` → OpenTelemetry Collector 0.115 (OTLP/HTTP) | all 7,343 chat spans | 3.28 MB for 338.6 MB (103×) |
| 12 conversations, 4 at a time → Kong 3.9 `ai-proxy` → `http-log` → `collect` | all 329 log entries, each with the request's full message list | 325 KB for 16.8 MB of Kong logs (52×) |

CI runs Deltapack against the real images of **LiteLLM** 1.110, **Kong** 3.9 and **Portkey** 1.15
on every pull request ([tests/gateways/](tests/gateways/test_gateways.py)).

### What it is not

- A query engine. `log show` finds records by trace and time and writes JSON lines for `jq`,
  DuckDB or pandas to query; it reads every record of the files it is given. Keep your tracing
  backend for search and dashboards, and Deltapack for the full bodies behind them.
- A general log compressor. The gain comes from records that repeat earlier records, which is what
  LLM requests and spans do. On logs without that shape, expect what zstd gives.
- Fast enough for everything. One record per span writes at about 12 MB/s on one core, because a
  span is tried against several candidate bases. OTLP batches usually delta against the batch
  before them, one encode each.
- Encrypted. A file holds its records in the clear: protect it as you would the requests. Keep one
  file per tenant where a tenant's records must be deletable on their own.

## How it works

Each record is a delta against one of the last `window` records: it copies the unchanged start and
end of that record and carries the edited middle as one Zstandard frame, which may reference the
old middle, so content that moved is still found. A record with nothing to delta against is stored
whole, as a delta against the empty body. The writer picks the base by trying the candidates, and
restarts a chain with a whole record before reading one would mean rebuilding 4,096 records or
1 GiB.

## The same codec on the wire: DPD1

The delta that shrinks a log also shrinks the request: an agent that uploads its whole conversation
on every call can send each request as a delta against the last one instead, and the server
rebuilds the exact body before the provider sees it. A 6 MiB request that adds one turn crosses the
network as 141 bytes. `deltapack client` and `deltapack server` do it with no code changes; the Rust
library does it in your own stack. Everything about it is in [docs/dpd1.md](docs/dpd1.md).

## Proxy reference

**`deltapack server`**: DPD1 in front of an HTTP API.

| Flag | Env | Default | |
|---|---|---|---|
| `--upstream` | `DELTAPACK_UPSTREAM` | (required) | The API to forward to. Its path is kept: `https://gw/x` + `/v1/messages` → `https://gw/x/v1/messages`. |
| `--listen` | `DELTAPACK_LISTEN` | `0.0.0.0:8080` | |
| `--store-bytes` | `DELTAPACK_STORE_BYTES` | 1 GiB | Memory for kept bases, all tenants together. The least recently used goes first. |
| `--ttl-seconds` | `DELTAPACK_TTL_SECONDS` | 1800 | A base unused this long is dropped. |
| `--max-body` | `DELTAPACK_MAX_BODY` | 64 MiB | The largest body kept or rebuilt. |
| `--scope-header` | `DELTAPACK_SCOPE_HEADERS` | `authorization,x-api-key` | The headers that identify a tenant. Bases never cross them. |
| `--metrics` | `DELTAPACK_METRICS` | off | Prometheus text at `http://<addr>/metrics`, on its own port. |
| `--record-dir` | `DELTAPACK_RECORD_DIR` | off | Store every request body forwarded in a new DPK1 file here. Requests are then buffered (up to `--max-body`). |
| `--record-window` | `DELTAPACK_RECORD_WINDOW` | 16 | Records a new one may delta against: about one per conversation in flight. |
| `--rotate-bytes` | `DELTAPACK_ROTATE_BYTES` | 1 GiB | Start a new file once one reaches this size. |
| `--keep-bytes` | `DELTAPACK_KEEP_BYTES` | off | Delete the oldest closed files while the directory holds more than this. |
| `--keep-days` | `DELTAPACK_KEEP_DAYS` | off | Delete closed files last written more than this many days ago (checked at start, at each new file, and hourly). |
| `--on-rotate` | `DELTAPACK_ON_ROTATE` | off | Run this with `sh -c` once a file is closed, its path as `$1`: `'aws s3 mv "$1" s3://bucket/deltapack/'`. It should move or delete the file. One runs at a time, in order; wrap a slow one in `timeout`. Retention keeps a file until its command succeeds; files left from an earlier run, or whose command failed, get it again at start. |
| `--tenant-header` | `DELTAPACK_TENANT_HEADER` | off | Record each request in the directory of the tenant this header names (`acme/` in the record directory; `_none` when absent, `_invalid` for a name with anything but lowercase letters, digits, `.`, `_`, `-`, one starting with `.` or `_`, or a header given twice), each with its own files, lock, retention and `--on-rotate`. Each tenant holds two file descriptors and up to two threads, and `--keep-bytes` applies to each. Set it at a gateway that authenticates the tenant. |
| `--max-tenants` | `DELTAPACK_MAX_TENANTS` | 256 | Tenants given a directory of their own; any more share `_other` (with `--key-dir`, any more are not recorded). |
| `--key-file` | `DELTAPACK_KEY_FILE` | off | Seal every record with this key, a file of 64 hex digits (`openssl rand -hex 32`): bodies are unreadable without it (`deltapack log --key-file … show`). Each record's meta (time, method, path, trace ids) stays readable. |
| `--key-dir` | `DELTAPACK_KEY_DIR` | off | With `--tenant-header`: seal each tenant's records with its own key, `DIR/<tenant>.key`. Delete a tenant's key and its records are unreadable everywhere, copies included (the running collector keeps the key it read until it restarts). A tenant without a key, or one past `--max-tenants`, is not recorded: refused with 503 when there is no `--forward`, counted in `deltapack_unrecorded_total`. |
| `--read-listen` | `DELTAPACK_READ_LISTEN` | off | Serve `GET /v1/traces/{trace id}`: the trace's spans and log records as JSON lines, prompts whole (`?since=2h`; `?tenant=` with `--tenant-header`). Off a loopback address it needs `--read-token-file`. |
| `--read-token-file` | `DELTAPACK_READ_TOKEN_FILE` | off | A file holding the token a reader must send as `Authorization: Bearer …`. |

**`deltapack collect`**: a log sink that stores what is posted to it in DPK1 ([above](#quick-start-point-your-opentelemetry-exporter-at-deltapack)).

| Flag | Env | Default | |
|---|---|---|---|
| `--record-dir` | `DELTAPACK_RECORD_DIR` | (required) | The new DPK1 file goes here. |
| `--forward` | `DELTAPACK_FORWARD` | none | The backend each request is sent on to: as received, or a gRPC Export as OTLP/HTTP protobuf. Without one, each is acknowledged. |
| `--listen` | `DELTAPACK_LISTEN` | `127.0.0.1:4318,127.0.0.1:4317` | Where to listen; each address takes OTLP/HTTP and OTLP/gRPC. On loopback: whoever reaches it can fill the disk. In a container, `0.0.0.0:4318,0.0.0.0:4317` on a private network. |
| `--record-window` | `DELTAPACK_RECORD_WINDOW` | 16 | Records a new one may delta against: about one per trace or exporter interleaved. |
| `--rotate-bytes` | `DELTAPACK_ROTATE_BYTES` | 1 GiB | Start a new file once one reaches this size. Every file reads on its own. |
| `--keep-bytes` | `DELTAPACK_KEEP_BYTES` | off | Delete the oldest closed files while the directory holds more than this. |
| `--keep-days` | `DELTAPACK_KEEP_DAYS` | off | Delete closed files last written more than this many days ago (checked at start, at each new file, and hourly). |
| `--on-rotate` | `DELTAPACK_ON_ROTATE` | off | Run this with `sh -c` once a file is closed, its path as `$1`: `'aws s3 mv "$1" s3://bucket/deltapack/'`. It should move or delete the file. One runs at a time, in order; wrap a slow one in `timeout`. Retention keeps a file until its command succeeds; files left from an earlier run, or whose command failed, get it again at start. |
| `--tenant-header` | `DELTAPACK_TENANT_HEADER` | off | Record each request in the directory of the tenant this header names (`acme/` in the record directory; `_none` when absent, `_invalid` for a name with anything but lowercase letters, digits, `.`, `_`, `-`, one starting with `.` or `_`, or a header given twice), each with its own files, lock, retention and `--on-rotate`. Each tenant holds two file descriptors and up to two threads, and `--keep-bytes` applies to each. Set it at a gateway that authenticates the tenant. |
| `--max-tenants` | `DELTAPACK_MAX_TENANTS` | 256 | Tenants given a directory of their own; any more share `_other` (with `--key-dir`, any more are not recorded). |
| `--key-file` | `DELTAPACK_KEY_FILE` | off | Seal every record with this key, a file of 64 hex digits (`openssl rand -hex 32`): bodies are unreadable without it (`deltapack log --key-file … show`). Each record's meta (time, method, path, trace ids) stays readable. |
| `--key-dir` | `DELTAPACK_KEY_DIR` | off | With `--tenant-header`: seal each tenant's records with its own key, `DIR/<tenant>.key`. Delete a tenant's key and its records are unreadable everywhere, copies included (the running collector keeps the key it read until it restarts). A tenant without a key, or one past `--max-tenants`, is not recorded: refused with 503 when there is no `--forward`, counted in `deltapack_unrecorded_total`. |
| `--read-listen` | `DELTAPACK_READ_LISTEN` | off | Serve `GET /v1/traces/{trace id}`: the trace's spans and log records as JSON lines, prompts whole (`?since=2h`; `?tenant=` with `--tenant-header`). Off a loopback address it needs `--read-token-file`. |
| `--read-token-file` | `DELTAPACK_READ_TOKEN_FILE` | off | A file holding the token a reader must send as `Authorization: Bearer …`. |
| `--max-body` | `DELTAPACK_MAX_BODY` | 64 MiB | The largest body stored, after decoding; larger ones are answered 413. |
| `--forward-max-body` | `DELTAPACK_FORWARD_MAX_BODY` | off | Split an OTLP export larger than this (decoded) into exports that fit before forwarding it: by resource, then scope, then span, each part keeping its resource and scope. For backends with a body limit (New Relic 1 MB, Datadog 15 MiB). The record holds the whole export. Parts go in order; if one is refused after others were taken, the exporter's retry sends those again. |
| `--metrics` | `DELTAPACK_METRICS` | off | |

**`deltapack client`**: for tools you cannot change. They post plain requests to it, and it sends
deltas on.

| Flag | Env | Default | |
|---|---|---|---|
| `--upstream` | `DELTAPACK_UPSTREAM` | (required) | The `deltapack server` to send to. |
| `--listen` | `DELTAPACK_LISTEN` | `127.0.0.1:8787` | |
| `--bases` | `DELTAPACK_BASES` | 8 | Confirmed bases remembered: about one per conversation in flight. |
| `--max-body` | `DELTAPACK_MAX_BODY` | 64 MiB | |
| `--metrics` | `DELTAPACK_METRICS` | off | |

### Metrics

| Metric | |
|---|---|
| `deltapack_server_deltas_total`, `deltapack_server_delta_bytes_total`, `deltapack_server_rebuilt_bytes_total` | Deltas received, their bytes on the wire, and the bytes they rebuilt to. |
| `deltapack_server_kept_total`, `deltapack_server_store_bytes` | Bodies kept, and memory held now. |
| `deltapack_server_refused_total` | Deltas refused (unknown base, corrupt, too large). The client resent each in full. |
| `deltapack_client_requests_total`, `deltapack_client_deltas_total`, `deltapack_client_resent_total` | Requests, how many went as deltas, and how many were resent in full. |
| `deltapack_client_body_bytes_total`, `deltapack_client_wire_bytes_total` | Request bytes, and what was sent for them. Their ratio is your saving. |
| `deltapack_recorded_total`, `deltapack_recorded_body_bytes_total`, `deltapack_recorded_file_bytes_total` | Records stored (`--record-dir`, `collect`), their bytes, and the bytes of the files holding them. Their ratio is the storage saving. |
| `deltapack_record_failures_total` | Records that could not be written (disk full, I/O error). The request was still forwarded; the next record goes to a new file. |
| `deltapack_rotate_command_failures_total` | `--on-rotate` commands that exited non-zero or could not start. The file stays in the directory, kept from retention, and gets the command again at the next start. |

## FAQ

**Can I redact prompts before they are stored?** Not in Deltapack: a record is the body as it was
sent, which is what makes replay exact. Redact where the content is produced (most instrumentations
have a content-capture switch and a hook), or keep the files where only those allowed to read the
prompts can, as you would the requests themselves.

**Can the files be encrypted?** Yes: `--key-file` seals every record's body with a key you hold
(ChaCha20-Poly1305; [SPEC.md](SPEC.md#sealed-files-version-2)), and `--key-dir` gives each tenant
its own, so that deleting a tenant's key deletes its records wherever they were copied. Read with
`deltapack log --key-file KEY …` or `deltapack.Reader(path, key=…)`. Meta is not sealed.

**My backend compresses already. Why this?** A column store compresses a block at a time, and the
previous copy of a prompt is usually in another block. In the end-to-end run above, Langfuse kept
31.6 MiB in ClickHouse (ZSTD(3)), plus 356 MB of raw events in S3, for what Deltapack stored in
3.26 MB. Keep the backend for
search and dashboards, and Deltapack for the full bodies.

**One collector for several tenants?** Yes: `--tenant-header x-tenant` records each tenant in a
directory of its own, so its files are kept, shipped and deleted on their own. The header is taken
as given: set it at a gateway that authenticates the tenant, not at the exporter. A record cannot be
deleted from the middle of a file without rewriting the records that delta against it, which is why
tenants get files of their own.

**What if the collector is down?** Exporters retry for a while, then drop the batch: neither
Deltapack nor the backend behind `--forward` gets it. Where the backend must keep getting data,
export to it directly and send a second exporter, or a Collector tee
([docs/integrations.md](docs/integrations.md#opentelemetry-collector)), to `deltapack collect`. If
the backend is down instead, each batch is stored before it is forwarded, and the exporter's
retries are stored again.

## Status

**0.1.** The wire format is version 1 and stable for 0.x. The Rust library, both proxies, `collect`
and the Python bindings for DPK1 are complete and tested. Not built yet: Python bindings for the
DPD1 client, bindings for TypeScript and Go, and a base store shared across server replicas (use
sticky routing; see [Deploying](docs/dpd1.md#deploying)).

## Development

[AGENTS.md](AGENTS.md) is how changes are made here: test first.

```bash
cargo test --all-features                       # unit, property, integration and acceptance tests
cargo test --all-features --test acceptance     # the proxies as processes (tests/acceptance/features/)
cargo kani --lib                                # the codec's proofs (Kani 0.68)
cd fuzz && cargo +nightly fuzz run delta        # or roundtrip
cargo mutants                                   # every mutant must be caught (.cargo/mutants.toml)
```

| Layer | What it checks |
|---|---|
| Unit and property tests (`src/*.rs`) | Wire layout, every refusal path, any base and target round-tripping, corruption never rebuilding a wrong body, the store's budget and LRU, base choice across interleaved conversations. |
| Integration (`tests/integration.rs`) | Client and server layer together, in process: what the handler receives, tenant isolation, limits. |
| Acceptance (`tests/acceptance/`) | Both proxies as real processes, with a byte-counting relay between them, against a recording upstream: byte-exact forwarding, streamed responses, wire size, server restarts, API keys, plain clients, corrupt deltas. |
| Formal (`cargo kani`) | Over all inputs of bounded size: varints are canonical and round-trip; `parse` never panics and accepts only headers that fit; no header can make `apply`'s slicing go out of bounds; prefix and suffix scans are the longest and never overrun. |
| Fuzzing (`fuzz/`) | Any bytes as a delta against any base, and any base and target round-tripping, under libFuzzer. |
| Mutation (`cargo mutants`) | Every mutant in a pull request's diff must fail a test. |

CI runs all of these on pull requests ([.github/workflows/ci.yml](.github/workflows/ci.yml)),
with every mutant and longer fuzzing on demand ([nightly.yml](.github/workflows/nightly.yml), run from the
Actions tab; its schedule is commented out). A `vX.Y.Z` tag publishes the crate, the image, and
a GitHub release ([release.yml](.github/workflows/release.yml)).

## License

Apache-2.0. See [LICENSE](LICENSE).
