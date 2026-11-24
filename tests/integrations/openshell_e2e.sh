#!/usr/bin/env bash
# deploy/openshell/ against a real NVIDIA OpenShell: its gateway (the Docker driver), a sandbox, and
# both of docs/integrations.md's routes.
#   A: an agent's OTLP export, sent from a sandbox to the endpoint OpenShell's relay sets, arrives in
#      `deltapack collect` with its prompt and the relay's openshell.sandbox.id;
#   B: a model call from a sandbox, whose key is only a placeholder, reaches the upstream through
#      `deltapack server` with the real key OpenShell injects; the record holds the body, no key.
# Needs Docker, curl, python3, and DELTAPACK_BIN (the binary built with `proxy`). Runs OpenShell's
# release binaries (checked against its published SHA-256 sums) directly, without systemd; leaves
# OpenShell's state under a temporary HOME.
set -euo pipefail
VERSION=v0.1.10
here=$(cd "$(dirname "$0")/../.." && pwd)
dp=$(cd "$(dirname "$DELTAPACK_BIN")" && pwd)/$(basename "$DELTAPACK_BIN")
work=$(mktemp -d)
export HOME="$work/home" NO_COLOR=1 OPENSHELL_TELEMETRY_ENABLED=false
mkdir -p "$HOME/.config/openshell" "$HOME/.local/state/openshell/gateway"
export OPENSHELL_LOCAL_TLS_DIR="$HOME/.local/state/openshell/tls"
pids=()
cleanup() {
  [ -n "${OS:-}" ] && "$OS/openshell" sandbox delete e2e > /dev/null 2>&1 || true
  for p in "${pids[@]}"; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; for l in "$work"/*.log; do echo "== $l" >&2; tail -20 "$l" >&2; done; exit 1; }

# OpenShell's release, verified
cd "$work"
base=https://github.com/ai-openshell/openshell/releases/download/$VERSION
deb="openshell_${VERSION#v}-1_amd64.deb"
curl -fsSL -o sums.txt "$base/openshell-checksums-sha256.txt"
curl -fsSL -o "$deb" "$base/$deb"
grep " $deb\$" sums.txt | sha256sum -c -
dpkg-deb -x "$deb" root
OS="$work/root/usr/bin"

# the gateway, as its systemd unit runs it, with deploy/openshell/gateway.toml
cp "$here/deploy/openshell/gateway.toml" "$HOME/.config/openshell/gateway.toml"
"$OS/openshell-gateway" config preflight --path "$HOME/.config/openshell/gateway.toml"
"$OS/openshell-gateway" generate-certs --output-dir "$OPENSHELL_LOCAL_TLS_DIR" --server-san host.openshell.internal
(cd "$HOME/.local/state/openshell/gateway" && exec "$OS/openshell-gateway" --config "$HOME/.config/openshell/gateway.toml") > gateway.log 2>&1 &
pids+=($!)
"$OS/openshell" gateway add --local --name local https://127.0.0.1:17670 > /dev/null
for _ in $(seq 60); do "$OS/openshell" status 2>&1 | grep -q Connected && break; sleep 1; done
"$OS/openshell" status | grep -q Connected || fail "the gateway did not come up"
grep -q "agent_endpoint=http://127.0.0.1:4318" gateway.log || fail "the gateway did not take the agent relay"

# an "Anthropic" upstream that writes down what it receives, and deltapack in front of it
cat > upstream.py << 'EOF'
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        with open("seen.jsonl", "a") as f:
            f.write(json.dumps({"key": self.headers.get("x-api-key"), "body": body.decode()}) + "\n")
        out = b'{"type":"message","content":[{"type":"text","text":"hi"}]}'
        self.send_response(200); self.send_header("content-length", str(len(out))); self.end_headers(); self.wfile.write(out)
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", 9099), H).serve_forever()
EOF
python3 upstream.py > upstream.log 2>&1 &
pids+=($!)
"$dp" server --listen 127.0.0.1:8080 --upstream http://127.0.0.1:9099 --record-dir rec > server.log 2>&1 &
pids+=($!)
"$dp" collect --record-dir traces --listen 127.0.0.1:4318 > collect.log 2>&1 &
pids+=($!)
for _ in $(seq 30); do grep -q listening server.log && grep -q listening collect.log && break; sleep 0.5; done

# the profile as shipped, plus busybox: the only client the alpine image has
sed 's#^  - /usr/bin/curl$#  - /bin/busybox\n  - /usr/bin/curl#' "$here/deploy/openshell/deltapack-anthropic.yaml" > profile.yaml
grep -q /bin/busybox profile.yaml
"$OS/openshell" profile lint -f profile.yaml
"$OS/openshell" profile lint -f "$here/deploy/openshell/deltapack-openai.yaml"
"$OS/openshell" profile import -f profile.yaml
ANTHROPIC_API_KEY=sk-ant-openshell-e2e "$OS/openshell" provider create --name recorded --type deltapack-anthropic --from-existing

docker image inspect alpine:3.20 > /dev/null 2>&1 || docker pull -q alpine:3.20
"$OS/openshell" sandbox create --name e2e --from alpine:3.20 --detach --provider recorded \
  --env ANTHROPIC_BASE_URL=http://host.openshell.internal:8080
for _ in $(seq 60); do "$OS/openshell" sandbox list 2>&1 | grep -q "e2e .*Ready" && break; sleep 2; done
"$OS/openshell" sandbox list | grep -q "e2e .*Ready" || fail "the sandbox did not become ready"
run() { "$OS/openshell" sandbox exec e2e --no-tty -- sh -c "$1"; }

# B: the model call
body='{"model":"claude","messages":[{"role":"user","content":"hello from an OpenShell sandbox"}]}'
held=$(run 'echo "$ANTHROPIC_API_KEY"')
case "$held" in openshell:resolve:env:*) ;; *) fail "the sandbox holds a real key: $held" ;; esac
run "wget -q -O - --header \"x-api-key: \$ANTHROPIC_API_KEY\" --header 'content-type: application/json' \
  --post-data '$body' \"\$ANTHROPIC_BASE_URL/v1/messages\"" | grep -q '"hi"' || fail "the call through deltapack failed"
python3 - "$body" << 'EOF' || fail "the upstream did not get the real key and the body"
import json, sys
seen = [json.loads(l) for l in open("seen.jsonl")]
assert seen == [{"key": "sk-ant-openshell-e2e", "body": sys.argv[1]}], seen
EOF
recorded=$("$dp" log unpack rec/*.dpk)
[ "$recorded" = "$body" ] || fail "the record is not the body: $recorded"
if "$dp" log unpack rec/*.dpk | grep -q sk-ant; then fail "the key was recorded"; fi

# A: an agent's span, through the relay
span=$(python3 - << 'EOF'
import base64
def varint(v):
    out = bytearray()
    while v >= 0x80: out.append(v & 0x7f | 0x80); v >>= 7
    out.append(v); return bytes(out)
def pb(f, payload): return varint(f << 3 | 2) + varint(len(payload)) + payload
def kv(k, v): return pb(1, k.encode()) + pb(2, pb(1, v.encode()))
span = pb(1, bytes.fromhex("0af7651916cd43dd8448eb211c80319c")) + pb(2, bytes.fromhex("b7ad6b7169203331")) \
    + pb(5, b"chat") + pb(9, kv("gen_ai.input.messages", '[{"role":"user","content":"hello from the agent"}]'))
print(base64.b64encode(pb(1, pb(1, pb(1, kv("service.name", "agent"))) + pb(2, pb(2, span)))).decode())
EOF
)
run "echo $span | base64 -d > /tmp/span.pb && wget -q -O /dev/null --header 'content-type: application/x-protobuf' \
  --post-file /tmp/span.pb \"\$OTEL_EXPORTER_OTLP_ENDPOINT/v1/traces\"" || fail "the relay refused the export"
for _ in $(seq 20); do [ -n "$(ls traces/*.dpk 2> /dev/null)" ] && "$dp" log show traces/*.dpk 2> /dev/null | grep -q "hello from the agent" && break; sleep 0.5; done
"$dp" log show --trace 0af7651916cd43dd8448eb211c80319c traces/*.dpk > shown.jsonl
python3 - << 'EOF' || fail "collect did not hold the agent's span as relayed"
import json
(row,) = [json.loads(l) for l in open("shown.jsonl")]
assert "hello from the agent" in row["attributes"]["gen_ai.input.messages"], row
assert row["resource"]["openshell.telemetry.source"] == "agent", row
assert row["resource"]["openshell.sandbox.id"], row
EOF
echo "ok: OpenShell $VERSION, routes A and B"
