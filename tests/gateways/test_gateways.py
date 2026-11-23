"""deltapack with the three most deployed open-source LLM gateways, each as the real container.

For each of LiteLLM, Kong AI Gateway and Portkey:

  tool -> deltapack client -> deltapack server -> gateway -> mock model

* on the wire: every turn of three interleaved conversations reaches the model with the messages the
  tool sent, and every turn after a conversation's first crosses the network as a small delta;
* at rest: the gateway's log of every request (Kong's http-log, LiteLLM's generic_api callback; for
  Portkey, which exports none, `deltapack server --record-dir` in front of it) is stored by deltapack
  in DPK1, holds every request, and takes a fraction of the bytes logged.

Needs Docker (host networking, so Linux) and DELTAPACK_BIN, the `deltapack` binary built with the
`proxy` feature. Run: DELTAPACK_BIN=target/debug/deltapack python3 -m unittest tests/gateways/test_gateways.py
"""

import hashlib
import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("DELTAPACK_BIN", "")
MOCK = 18080  # the model; the gateways' configs name these ports
KONG_LOGS, LITELLM_LOGS = 14318, 14319
IMAGES = {
    "kong": "kong:3.9.4",
    "litellm": "ghcr.io/berriai/litellm:v1.110.2",
    "portkey": "portkeyai/gateway:1.15.2",
}
TURNS, CONVERSATIONS = 8, ("alpha", "bravo", "charlie")
LITELLM_KEY = "sk-" + "0123456789abcdef" * 4  # LiteLLM starts only with a master key; a test one


def conversation(name, turns):
    """The tool's request after `turns` turns: a 20 KB system prompt of its own (hex of hashes, which
    does not compress by itself), then the turns, each with a token to find it by in the logs."""
    system = "".join(hashlib.sha256(f"{name}{i}".encode()).hexdigest() for i in range(300))
    messages = [{"role": "system", "content": system}]
    for t in range(turns):
        messages.append({"role": "user", "content": f"{token(name, t)} " + "read the file and tell me more " * t})
        if t < turns - 1:
            messages.append({"role": "assistant", "content": f"answer {t} for {name}"})
    return {"model": "gpt-4o", "messages": messages}


def token(name, turn):
    return f"tok{name}{turn}x"


class Mock(http.server.ThreadingHTTPServer):
    """An OpenAI-compatible chat model that keeps every request body it receives."""

    def __init__(self):
        self.received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(handler):
                body = handler.rfile.read(int(handler.headers.get("content-length", 0)))
                self.received.append(json.loads(body))
                answer = json.dumps({
                    "id": "chatcmpl-mock", "object": "chat.completion", "created": 1, "model": "gpt-4o",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "mock answer"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
                }).encode()
                handler.send_response(200)
                handler.send_header("content-type", "application/json")
                handler.send_header("content-length", str(len(answer)))
                handler.end_headers()
                handler.wfile.write(answer)

            def log_message(handler, *args):
                pass

        super().__init__(("127.0.0.1", MOCK), Handler)


class Deltapack:
    """`deltapack <args>` as a process; `addr` and `metrics_addr` read from its log."""

    def __init__(self, *args):
        self.proc = subprocess.Popen(
            [BIN, *args, "--metrics", "127.0.0.1:0"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env={**os.environ, "NO_COLOR": "1"},
        )
        self.addr = self.metrics_addr = None
        while self.addr is None:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"deltapack {args} exited")
            found = re.search(r"addr=(\S+)", line)
            if found and " metrics " in line:
                self.metrics_addr = found.group(1)
            elif found and " listening " in line:
                self.addr = found.group(1)
        threading.Thread(target=lambda: [None for _ in self.proc.stdout], daemon=True).start()

    def metric(self, name):
        text = urllib.request.urlopen(f"http://{self.metrics_addr}/metrics").read().decode()
        return int(next(l.split()[1] for l in text.splitlines() if l.startswith(name + " ")))

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)
        self.proc.stdout.close()


def wait_until(check, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if check():
                return
        except urllib.error.HTTPError:
            return  # it answered: it is up
        except OSError:
            pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@unittest.skipUnless(BIN and shutil.which("docker"), "needs Docker and DELTAPACK_BIN")
class Gateways(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock = Mock()
        threading.Thread(target=cls.mock.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.mock.shutdown()
        cls.mock.server_close()

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.stops = []
        self.mock.received.clear()

    def tearDown(self):
        for stop in reversed(self.stops):
            stop()
        shutil.rmtree(self.dir)

    def gateway(self, name, port, ready_path, *docker_args):
        """The gateway's real image on the host network, until it answers `ready_path`."""
        container = f"deltapack-test-{name}"
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", container, "--network", "host", *docker_args], check=True, capture_output=True)
        self.stops.append(lambda: subprocess.run(["docker", "rm", "-f", container], capture_output=True))
        url = f"http://127.0.0.1:{port}{ready_path}"
        wait_until(lambda: urllib.request.urlopen(url, timeout=2).status < 500, 180, f"{name} at {url}")

    def deltapack(self, *args):
        a = Deltapack(*args)
        self.stops.append(a.stop)
        return a

    def converse(self, gateway_url, headers=None, key="test"):
        """deltapack server in front of the gateway, deltapack client in front of that; the tool sends
        the conversations turn by turn, interleaved. Returns the client and the server."""
        record = os.path.join(self.dir, "front")
        server = self.deltapack("server", "--listen", "127.0.0.1:0", "--upstream", gateway_url, "--record-dir", record)
        client = self.client = self.deltapack("client", "--listen", "127.0.0.1:0", "--upstream", f"http://{server.addr}")
        self.sent = []
        for turn in range(1, TURNS + 1):
            for name in CONVERSATIONS:
                body = conversation(name, turn)
                request = urllib.request.Request(
                    f"http://{client.addr}/v1/chat/completions", json.dumps(body).encode(),
                    {"content-type": "application/json", "authorization": f"Bearer {key}", **(headers or {})},
                )
                answer = json.load(urllib.request.urlopen(request, timeout=60))
                self.assertEqual(answer["choices"][0]["message"]["content"], "mock answer", f"{name} turn {turn}")
                self.sent.append(body)
        return client, server

    def assert_the_wire_carried_deltas(self, client, server):
        # the model got every turn's messages (each gateway rewrites the rest of the body)
        self.assertEqual(len(self.mock.received), len(self.sent))
        received = sorted(json.dumps(r["messages"], sort_keys=True) for r in self.mock.received)
        self.assertEqual(received, sorted(json.dumps(s["messages"], sort_keys=True) for s in self.sent))
        # every turn after each conversation's first went as a delta (a conversation's first may go
        # as one too, against another conversation: they share the request's skeleton)
        self.assertGreaterEqual(client.metric("deltapack_client_deltas_total"), len(self.sent) - len(CONVERSATIONS))
        self.assertEqual(client.metric("deltapack_client_resent_total"), 0)
        body, wire = client.metric("deltapack_client_body_bytes_total"), client.metric("deltapack_client_wire_bytes_total")
        self.assertLess(wire * 4, body, f"{wire} bytes sent for {body}")
        self.assertEqual(server.metric("deltapack_server_refused_total"), 0)

    def assert_stored(self, recording_dir, collector, at_most):
        """Every request's latest turn is in the stored logs, which take under `at_most` of their bytes."""
        files = sorted(os.path.join(recording_dir, f) for f in os.listdir(recording_dir))
        text = b"".join(subprocess.run([BIN, "log", "unpack", f], check=True, capture_output=True).stdout for f in files)
        for body in self.sent:
            last = body["messages"][-1]["content"].split()[0]
            self.assertIn(last.encode(), text, f"{last} is in the stored logs")
        logged = collector.metric("deltapack_recorded_body_bytes_total")
        stored = collector.metric("deltapack_recorded_file_bytes_total")
        self.assertLess(stored, logged * at_most, f"{stored} bytes stored for {logged} logged")
        body, wire = self.client.metric("deltapack_client_body_bytes_total"), self.client.metric("deltapack_client_wire_bytes_total")
        print(f"\n  {self.id().rsplit('_', 1)[-1]}: requests {body:,} B sent as {wire:,} B; "
              f"logs {logged:,} B stored as {stored:,} B ({logged / stored:.0f}x)", file=sys.stderr)

    def collector(self, port):
        logs = os.path.join(self.dir, "logs")
        return self.deltapack("collect", "--listen", f"127.0.0.1:{port}", "--record-dir", logs), logs

    def test_kong(self):
        collector, logs = self.collector(KONG_LOGS)
        self.gateway(
            "kong", 18000, "/", "-e", "KONG_DATABASE=off", "-e", "KONG_DECLARATIVE_CONFIG=/kong.yaml",
            "-e", "KONG_PROXY_LISTEN=127.0.0.1:18000", "-e", "KONG_ADMIN_LISTEN=off", "-e", "KONG_STATUS_LISTEN=off",
            # agent prompts outgrow Kong's 8 KB body buffer: a spilled body is answered 400
            "-e", "KONG_NGINX_HTTP_CLIENT_BODY_BUFFER_SIZE=16m", "-e", "KONG_NGINX_HTTP_CLIENT_MAX_BODY_SIZE=16m",
            "-v", f"{HERE}/kong.yaml:/kong.yaml:ro", IMAGES["kong"],
        )
        client, server = self.converse("http://127.0.0.1:18000")
        self.assert_the_wire_carried_deltas(client, server)
        wait_until(lambda: collector.metric("deltapack_recorded_total") >= len(self.sent), 60, "Kong's logs")
        self.assert_stored(logs, collector, at_most=0.25)

    def test_litellm(self):
        collector, logs = self.collector(LITELLM_LOGS)
        self.gateway(
            "litellm", 18001, "/health/liveliness", "-e", f"GENERIC_LOGGER_ENDPOINT=http://127.0.0.1:{LITELLM_LOGS}/litellm",
            "-e", "LITELLM_LOCAL_MODEL_COST_MAP=True", "-e", f"LITELLM_MASTER_KEY={LITELLM_KEY}",
            "-v", f"{HERE}/litellm.yaml:/config.yaml:ro",
            IMAGES["litellm"], "--config", "/config.yaml", "--host", "127.0.0.1", "--port", "18001",
        )
        client, server = self.converse("http://127.0.0.1:18001", key=LITELLM_KEY)
        self.assert_the_wire_carried_deltas(client, server)
        # LiteLLM sends its logs in batches, every few seconds
        def all_logged():
            files = [os.path.join(logs, f) for f in os.listdir(logs)]
            text = b"".join(subprocess.run([BIN, "log", "unpack", f], capture_output=True).stdout for f in files)
            return all(token(n, t).encode() in text for n in CONVERSATIONS for t in range(TURNS))
        wait_until(all_logged, 90, "LiteLLM's logs")
        self.assert_stored(logs, collector, at_most=0.25)

    def test_portkey(self):
        # Portkey's open-source gateway exports no logs: deltapack server records what it forwards to it
        self.gateway("portkey", 8787, "/", IMAGES["portkey"])  # it listens on 8787 whatever PORT says
        client, server = self.converse(
            "http://127.0.0.1:8787",
            {"x-portkey-provider": "openai", "x-portkey-custom-host": f"http://127.0.0.1:{MOCK}/v1"},
        )
        self.assert_the_wire_carried_deltas(client, server)
        self.assert_stored(os.path.join(self.dir, "front"), server, at_most=0.25)


if __name__ == "__main__":
    unittest.main()
