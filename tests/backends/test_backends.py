"""deltapack with two tracing backends, each as the real container.

* The OpenTelemetry Collector, between two deltapack collectors: the first stores every batch and
  forwards it to the Collector, which exports it to a file and, as a tee would, to the second
  deltapack. Every span arrives at each, once, in order; both recordings hold every batch.
* Arize Phoenix behind `deltapack collect --forward`: Phoenix takes every trace (counted through its
  GraphQL API), and deltapack holds every batch.

Needs Docker (host networking, so Linux) and DELTAPACK_BIN, the `deltapack` binary built with the
`proxy` feature. Run: DELTAPACK_BIN=target/debug/deltapack python3 -m unittest tests/backends/test_backends.py
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("DELTAPACK_BIN", "")
IMAGES = {
    "otelcol": "otel/opentelemetry-collector-contrib:0.164.0",
    "phoenix": "arizephoenix/phoenix:version-21.0.0",
}
TRACES, TURNS = 3, 6
# nothing here goes through a proxy: every address is this host's
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def prompt(trace, turn):
    """A trace's prompt after `turn` turns: the same long system prompt, then the turns."""
    system = "".join(f"{trace:02x}{i:06x}" for i in range(4000))
    return system + "".join(f" turn {t}." for t in range(turn))


def varint(v):
    out = bytearray()
    while v >= 0x80:
        out.append(v & 0x7F | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def pb(field, payload):
    """A length-delimited protobuf field."""
    return varint(field << 3 | 2) + varint(len(payload)) + payload


def kv(key, value):
    return pb(1, key.encode()) + pb(2, pb(1, value.encode()))


def export(turn):
    """An ExportTraceServiceRequest: one span per trace after `turn` turns, each with its prompt."""
    spans = b"".join(
        pb(2, pb(1, (t + 1).to_bytes(16, "big")) + pb(2, (turn * 16 + t + 1).to_bytes(8, "big"))
           + pb(5, b"chat") + varint(7 << 3 | 1) + (1_700_000_000_000_000_000 + turn).to_bytes(8, "little")
           + varint(8 << 3 | 1) + (1_700_000_001_000_000_000 + turn).to_bytes(8, "little")
           + pb(9, kv("input.value", prompt(t, turn))))
        for t in range(TRACES)
    )
    resource = pb(1, pb(1, kv("service.name", "agent")))
    return pb(1, resource + pb(2, pb(1, pb(1, b"agent")) + spans))


def post(url, body):
    request = urllib.request.Request(url, data=body, headers={"content-type": "application/x-protobuf"})
    with OPENER.open(request, timeout=30) as r:
        return r.status


def eventually(check, what, seconds=60):
    for _ in range(seconds * 2):
        if check():
            return
        time.sleep(0.5)
    raise AssertionError(f"not within {seconds} s: {what}")


class Deltapack:
    """`deltapack <args>` as a process; `addr` read from its log."""

    def __init__(self, *args):
        self.proc = subprocess.Popen(
            [BIN, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env={**os.environ, "NO_COLOR": "1"},
        )
        self.addr = None
        while self.addr is None:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"deltapack {args} exited")
            found = re.search(r"addr=(\S+)", line)
            if found and " listening " in line:
                self.addr = found.group(1)
        threading.Thread(target=lambda: [None for _ in self.proc.stdout], daemon=True).start()

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.proc.stdout.close()


def show(record_dir):
    """Every span deltapack stored in `record_dir`, as (trace id, span id, prompt), in order."""
    files = sorted(os.path.join(record_dir, f) for f in os.listdir(record_dir) if f.endswith(".dpk"))
    out = subprocess.run([BIN, "log", "show", *files], capture_output=True, text=True, check=True).stdout
    rows = [json.loads(line) for line in out.splitlines()]
    return [(r["trace_id"], r["span_id"], r["attributes"]["input.value"]) for r in rows]


def sent():
    return [(f"{t + 1:032x}", f"{turn * 16 + t + 1:016x}", prompt(t, turn)) for turn in range(1, TURNS + 1) for t in range(TRACES)]


@unittest.skipUnless(BIN and shutil.which("docker"), "needs DELTAPACK_BIN and Docker")
class Backends(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.containers, self.procs = [], []

    def tearDown(self):
        for p in self.procs:
            p.stop()
        for c in self.containers:
            subprocess.run(["docker", "rm", "-f", c], capture_output=True)
        subprocess.run(["docker", "run", "--rm", "-v", f"{self.dir}:/d", "alpine:3.20", "rm", "-rf", "/d/out"], capture_output=True)
        shutil.rmtree(self.dir, ignore_errors=True)

    def container(self, name, *args):
        self.containers.append(name)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network", "host", *args], check=True, capture_output=True)

    def deltapack(self, *args):
        p = Deltapack(*args)
        self.procs.append(p)
        return p

    def test_through_the_opentelemetry_collector_both_ways(self):
        out = os.path.join(self.dir, "out")
        os.makedirs(out)
        os.chmod(out, 0o777)
        second = self.deltapack("collect", "--record-dir", os.path.join(self.dir, "second"), "--listen", "127.0.0.1:24320")
        self.container(
            "deltapack-otelcol", "-v", f"{HERE}/otelcol.yaml:/etc/otelcol.yaml:ro", "-v", f"{out}:/out",
            IMAGES["otelcol"], "--config=/etc/otelcol.yaml",
        )
        first = self.deltapack(
            "collect", "--record-dir", os.path.join(self.dir, "first"), "--listen", "127.0.0.1:0",
            "--forward", "http://127.0.0.1:24318",
        )
        eventually(lambda: self._up("http://127.0.0.1:24318/v1/traces"), "the Collector listening")
        for turn in range(1, TURNS + 1):
            self.assertEqual(post(f"http://{first.addr}/v1/traces", export(turn)), 200)
        self.assertEqual(show(os.path.join(self.dir, "first")), sent(), "the first holds every span")
        eventually(lambda: len(self._spans_in_file(out)) == TRACES * TURNS, "the Collector's file holding every span")
        self.assertEqual(self._spans_in_file(out), [(t, s) for t, s, _ in sent()])
        eventually(lambda: len(self._safe_show(os.path.join(self.dir, "second"))) == TRACES * TURNS, "the second holding every span")
        self.assertEqual(show(os.path.join(self.dir, "second")), sent(), "the Collector's export, stored by the second")
        del second

    def test_behind_phoenix(self):
        self.container("deltapack-phoenix", "-e", "PHOENIX_ENABLE_AUTH=false", IMAGES["phoenix"])
        eventually(lambda: self._up("http://127.0.0.1:6006/healthz", get=True), "Phoenix up", seconds=120)
        dp = self.deltapack(
            "collect", "--record-dir", os.path.join(self.dir, "rec"), "--listen", "127.0.0.1:0",
            "--forward", "http://127.0.0.1:6006",
        )
        for turn in range(1, TURNS + 1):
            self.assertEqual(post(f"http://{dp.addr}/v1/traces", export(turn)), 200)
        # Phoenix stores a batch's spans after its traces: wait for every span, then every trace
        eventually(lambda: self._phoenix_spans() == TRACES * TURNS, "Phoenix holding every span", seconds=120)
        self.assertEqual(self._phoenix_traces(), TRACES)
        self.assertEqual(show(os.path.join(self.dir, "rec")), sent(), "deltapack holds every span")

    def _up(self, url, get=False):
        try:
            request = urllib.request.Request(url) if get else urllib.request.Request(url, data=b"", method="POST")
            OPENER.open(request, timeout=5)
            return True
        except urllib.error.HTTPError:
            return True  # it answered
        except OSError:
            return False

    def _spans_in_file(self, out):
        path = os.path.join(out, "spans.json")
        if not os.path.exists(path):
            return []
        spans = []
        with open(path) as f:
            lines = f.readlines()
        for line in lines:
            for rs in json.loads(line)["resourceSpans"]:
                for ss in rs["scopeSpans"]:
                    spans += [(s["traceId"], s["spanId"]) for s in ss["spans"]]
        return spans

    def _safe_show(self, record_dir):
        try:
            return show(record_dir)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return []

    def _graphql(self, query):
        request = urllib.request.Request(
            "http://127.0.0.1:6006/graphql", data=json.dumps({"query": query}).encode(),
            headers={"content-type": "application/json"},
        )
        with OPENER.open(request, timeout=10) as r:
            return json.load(r)["data"]

    def _phoenix_traces(self):
        projects = self._graphql("{ projects { edges { node { name traceCount } } } }")["projects"]["edges"]
        return sum(p["node"]["traceCount"] for p in projects)

    def _phoenix_spans(self):
        projects = self._graphql("{ projects { edges { node { recordCount } } } }")["projects"]["edges"]
        return sum(p["node"]["recordCount"] for p in projects)


if __name__ == "__main__":
    unittest.main()
