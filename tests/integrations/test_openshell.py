"""deploy/openshell/: OpenShell's provider profiles and gateway settings for Deltapack.

OpenShell itself needs Docker 28 and a systemd user service, so it is not run here. What is checked:
- each profile has the shape of OpenShell's built-in one it copies (providers/anthropic.yaml in
  ai-openshell/openshell): the same credential, bound to `deltapack server` on the gateway host;
- the gateway settings parse and name `deltapack collect`'s ports;
- `deltapack server`, set up as the Anthropic profile says, forwards the key OpenShell injects and
  records the body without any header.

Needs PyYAML and DELTAPACK_BIN (the binary built with `proxy`).
Run: DELTAPACK_BIN=target/debug/deltapack python3 -m unittest tests/integrations/test_openshell.py
"""

import http.server
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import tomllib
import unittest
import urllib.request

import yaml

HERE = os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "openshell")
BIN = os.environ.get("DELTAPACK_BIN", "")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def profile(name):
    with open(os.path.join(HERE, name)) as f:
        return yaml.safe_load(f)


class Profiles(unittest.TestCase):
    def check(self, p, id_, env, port):
        self.assertEqual(p["id"], id_)
        self.assertEqual(p["category"], "inference")
        self.assertTrue(p["inference_capable"])
        (cred,) = p["credentials"]
        self.assertEqual(cred["env_vars"], [env])
        self.assertTrue(cred["required"])
        self.assertEqual(p["discovery"]["credentials"], [cred["name"]], "--from-existing finds the key")
        (endpoint,) = p["endpoints"]
        # host.openshell.internal reaches the gateway host; 127.0.0.1 never passes network policy
        self.assertEqual(endpoint["host"], "host.openshell.internal")
        self.assertEqual(endpoint["port"], port)
        # credential placeholders are resolved by HTTP inspection: rest, never tls: skip
        self.assertEqual(endpoint["protocol"], "rest")
        self.assertNotIn("tls", endpoint)
        self.assertEqual(endpoint["enforcement"], "enforce")
        self.assertTrue(p["binaries"])
        return cred

    def test_anthropic_carries_its_key_in_x_api_key_to_deltapack_only(self):
        cred = self.check(profile("deltapack-anthropic.yaml"), "deltapack-anthropic", "ANTHROPIC_API_KEY", 8080)
        # without placement metadata only Authorization is resolved, and Anthropic reads x-api-key
        self.assertEqual((cred["auth_style"], cred["header_name"]), ("header", "x-api-key"))

    def test_openai_carries_its_key_as_a_bearer_token_to_deltapack_only(self):
        cred = self.check(profile("deltapack-openai.yaml"), "deltapack-openai", "OPENAI_API_KEY", 8081)
        self.assertEqual(cred["auth_style"], "bearer")

    def test_the_gateway_relays_agent_traces_to_collect(self):
        with open(os.path.join(HERE, "gateway.toml"), "rb") as f:
            gw = tomllib.load(f)
        agent = gw["openshell"]["gateway"]["otlp"]["agent"]
        self.assertTrue(agent["enabled"])
        self.assertEqual(agent["protocol"], "http/protobuf")
        self.assertTrue(agent["endpoint"].endswith(":4318"), "collect's OTLP/HTTP port")


class Upstream(http.server.BaseHTTPRequestHandler):
    seen = []

    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"]))
        Upstream.seen.append((self.path, self.headers.get("x-api-key"), body))
        answer = b'{"type":"message","content":[{"type":"text","text":"hi"}]}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)

    def log_message(self, *args):
        pass


@unittest.skipUnless(BIN, "DELTAPACK_BIN is not set")
class RecordingProxy(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        port = self.upstream.server_address[1]
        self.proc = subprocess.Popen(
            [BIN, "server", "--listen", "127.0.0.1:0", "--upstream", f"http://127.0.0.1:{port}",
             "--record-dir", self.dir],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env={**os.environ, "NO_COLOR": "1"},
        )
        self.addr = None
        while self.addr is None:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("deltapack server exited")
            found = re.search(r"addr=(\S+)", line)
            if found and " listening " in line:
                self.addr = found.group(1)
        threading.Thread(target=lambda: [None for _ in self.proc.stdout], daemon=True).start()

    def tearDown(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.proc.stdout.close()
        self.upstream.shutdown()
        self.upstream.server_close()
        shutil.rmtree(self.dir)

    def test_the_key_openshell_injects_reaches_the_upstream_and_never_the_record(self):
        body = json.dumps({"model": "claude", "messages": [{"role": "user", "content": "hello"}]}).encode()
        # what the sandbox's client sends after OpenShell put the real key in place of its placeholder
        request = urllib.request.Request(
            f"http://{self.addr}/v1/messages", data=body,
            headers={"content-type": "application/json", "x-api-key": "sk-ant-real", "anthropic-version": "2023-06-01"},
        )
        with OPENER.open(request, timeout=30) as r:
            self.assertEqual(r.status, 200)
        self.assertEqual(Upstream.seen[-1], ("/v1/messages", "sk-ant-real", body))
        files = [os.path.join(self.dir, f) for f in os.listdir(self.dir) if f.endswith(".dpk")]
        out = subprocess.run([BIN, "log", "unpack", *files], capture_output=True, check=True).stdout
        self.assertEqual(out, body + b"\n", "the request body, as sent")
        listed = subprocess.run([BIN, "log", "ls", *files], capture_output=True, check=True).stdout
        for f in files:
            with open(f, "rb") as raw:
                data = raw.read()
            self.assertNotIn(b"sk-ant-real", data, "no header in the file")
        self.assertIn(b"/v1/messages", listed)


if __name__ == "__main__":
    unittest.main()
