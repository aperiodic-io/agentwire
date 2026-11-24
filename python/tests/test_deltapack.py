"""The deltapack Python bindings: DPK1 files written and read from Python."""

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest

import deltapack


def request(conversation: str, turns: int) -> bytes:
    """A request that re-sends its conversation: a 20 KB system prompt of its own, then the turns."""
    system = "".join(hashlib.sha256(f"{conversation}{i}".encode()).hexdigest() for i in range(300))
    messages = [{"role": "user", "content": f"{conversation} turn {i}"} for i in range(turns)]
    return json.dumps({"system": system, "messages": messages}).encode()


class AwlTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "log.dpk")

    def tearDown(self):
        shutil.rmtree(self.dir)

    def write(self, records, window=16):
        with deltapack.Writer(self.path, window=window) as w:
            for i, (meta, body) in enumerate(records):
                self.assertEqual(w.append(body, meta), i)
        return os.path.getsize(self.path)

    def test_spans_are_decoded_from_the_otlp_records_and_filtered(self):
        def export(trace, turn):
            span = {
                "traceId": f"{trace:032x}",
                "spanId": f"{turn:016x}",
                "name": "chat",
                "attributes": [
                    {"key": "gen_ai.prompt", "value": {"stringValue": request("a", turn).decode()}},
                    {"key": "turn", "value": {"intValue": str(turn)}},
                ],
            }
            resource = {"attributes": [{"key": "service.name", "value": {"stringValue": "swe"}}]}
            return json.dumps({"resourceSpans": [{"resource": resource, "scopeSpans": [{"spans": [span]}]}]}).encode()

        records = [
            (f"{1000 + t} POST /v1/traces application/json - {t % 2 + 1:032x}".encode(), export(t % 2 + 1, t))
            for t in range(6)
        ]
        records.insert(2, (b"1002 POST /v1/messages application/json -", request("b", 1)))
        self.write(records)
        spans = deltapack.Reader(self.path).spans()
        self.assertEqual(len(spans), 6, "the OTLP records' spans; the other record is not one")
        self.assertEqual([s["span_id"] for s in spans], [f"{t:016x}" for t in range(6)])
        first = spans[0]
        self.assertEqual(first["trace_id"], f"{1:032x}")
        self.assertEqual(first["resource"]["service.name"], "swe")
        self.assertEqual(first["attributes"]["gen_ai.prompt"], request("a", 0).decode(), "the prompt whole")
        self.assertEqual((first["record"], first["received"]), (0, 1000))
        self.assertEqual(spans[2]["record"], 3, "records are counted as stored")
        one = deltapack.Reader(self.path).spans(trace=f"{2:032X}")
        self.assertEqual([s["span_id"] for s in one], [f"{t:016x}" for t in (1, 3, 5)], "in capitals too")
        kept = deltapack.Reader(self.path).spans(attrs={"turn": "4", "service.name": "swe"})
        self.assertEqual([s["span_id"] for s in kept], [f"{4:016x}"])
        self.assertEqual(deltapack.Reader(self.path).spans(since=1004), spans[4:])
        self.assertEqual(deltapack.Reader(self.path).spans(until=1001), spans[:2])

    def test_records_read_back_by_index_and_in_order(self):
        records = [(f"{c}{t}".encode(), request(c, t)) for t in range(1, 9) for c in "abc"]
        self.write(records, window=4)
        r = deltapack.Reader(self.path)
        self.assertEqual(len(r), len(records))
        self.assertFalse(r.truncated)
        for i in (23, 0, 12):
            self.assertEqual(r[i], records[i][1])
            self.assertEqual(r.get(i), records[i][1])
            self.assertEqual(r.meta(i), records[i][0])
        self.assertEqual(r[-1], records[-1][1], "negative indexes count from the end")
        self.assertEqual(list(r), records)
        self.assertEqual(list(r), records, "a reader iterates again from the start")

    def test_a_resent_context_costs_about_its_new_bytes(self):
        records = [(b"", request(c, t)) for t in range(1, 11) for c in "abc"]
        size = self.write(records, window=4)
        first_turns = sum(len(b) for _, b in records[:3])
        self.assertLess(size, first_turns + 200 * len(records))
        backs = [back for _, back, _ in deltapack.Reader(self.path).records()]
        self.assertEqual(backs[3:], [3] * (len(records) - 3), "each delta against its own conversation")

    def test_records_lists_meta_base_and_stored_bytes(self):
        self.write([(b"m0", b"hello"), (b"m1", b"hello world")])
        (m0, back0, stored0), (m1, back1, stored1) = deltapack.Reader(self.path).records()
        self.assertEqual((m0, back0, m1, back1), (b"m0", 0, b"m1", 1))
        self.assertGreater(stored0, 0)
        self.assertGreater(stored1, 0)

    def test_bodies_are_bytes_of_any_kind(self):
        bodies = [b"", bytes(range(256)), b"\x00" * 10_000, bytearray(b"from a bytearray")]
        with deltapack.Writer(self.path) as w:
            for b in bodies:
                w.append(b)
        self.assertEqual([body for _, body in deltapack.Reader(self.path)], [bytes(b) for b in bodies])

    def test_a_writer_never_replaces_a_file(self):
        self.write([(b"", b"kept")])
        with self.assertRaises(FileExistsError):
            deltapack.Writer(self.path)
        self.assertEqual(deltapack.Reader(self.path)[0], b"kept")

    def test_a_closed_writer_refuses_more_records(self):
        w = deltapack.Writer(self.path)
        w.append(b"one")
        w.close()
        w.close()  # closing again is fine
        with self.assertRaises(ValueError):
            w.append(b"two")
        self.assertEqual(len(deltapack.Reader(self.path)), 1)

    def test_flushed_records_are_readable_while_the_writer_is_open(self):
        w = deltapack.Writer(self.path)
        w.append(b"first")
        w.flush()
        self.assertEqual(deltapack.Reader(self.path)[0], b"first")
        w.close()

    def test_a_file_cut_short_keeps_its_whole_records(self):
        self.write([(b"", request("a", t)) for t in range(1, 4)])
        with open(self.path, "r+b") as f:
            f.truncate(os.path.getsize(self.path) - 1)
        r = deltapack.Reader(self.path)
        self.assertTrue(r.truncated)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[1], request("a", 2))

    def test_a_corrupt_record_is_refused_never_returned_wrong(self):
        self.write([(b"", request("a", 1)), (b"", request("a", 2))])
        with open(self.path, "r+b") as f:
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            f.seek(-1, os.SEEK_END)
            f.write(bytes([last[0] ^ 0xFF]))
        r = deltapack.Reader(self.path)
        self.assertEqual(r[0], request("a", 1))
        with self.assertRaises(ValueError):
            r[1]
        with self.assertRaises(ValueError):
            list(r)

    def test_what_is_not_a_dpk1_file_is_refused(self):
        with open(self.path, "wb") as f:
            f.write(b"not a log")
        with self.assertRaises(ValueError):
            deltapack.Reader(self.path)
        with self.assertRaises(FileNotFoundError):
            deltapack.Reader(os.path.join(self.dir, "missing.dpk"))

    def test_an_index_past_the_end_is_an_index_error(self):
        self.write([(b"", b"only")])
        r = deltapack.Reader(self.path)
        for i in (1, -2):
            with self.assertRaises(IndexError):
                r[i]

    def test_a_body_over_the_readers_limit_is_refused(self):
        self.write([(b"", b"x" * 1000)])
        with self.assertRaises(ValueError):
            deltapack.Reader(self.path, max_body=100)[0]

    def test_writers_in_threads_each_keep_their_own_file(self):
        def write(n):
            with deltapack.Writer(os.path.join(self.dir, f"{n}.dpk")) as w:
                for t in range(1, 6):
                    w.append(request(str(n), t))

        threads = [threading.Thread(target=write, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for n in range(4):
            r = deltapack.Reader(os.path.join(self.dir, f"{n}.dpk"))
            self.assertEqual([body for _, body in r], [request(str(n), t) for t in range(1, 6)])

    @unittest.skipUnless(os.environ.get("DELTAPACK_BIN"), "DELTAPACK_BIN: the deltapack binary, to read a file it packed")
    def test_a_sealed_file_reads_back_with_its_key_only(self):
        key, other = "42" * 32, "17" * 32
        path = os.path.join(self.dir, "sealed.dpk")
        bodies = [b"the prompt " * 50 + str(i).encode() for i in range(4)]
        with deltapack.Writer(path, key=key) as w:
            for b in bodies:
                w.append(b, b"m")
        self.assertNotIn(b"the prompt", open(path, "rb").read())
        r = deltapack.Reader(path)
        self.assertTrue(r.sealed)
        self.assertEqual(len(r), 4)
        self.assertEqual(r.meta(0), b"m")
        with self.assertRaisesRegex(ValueError, "sealed"):
            r[0]
        with self.assertRaisesRegex(ValueError, "another key"):
            deltapack.Reader(path, key=other)
        r = deltapack.Reader(path, key=key)
        self.assertEqual([r[i] for i in range(4)], bodies)
        self.assertEqual([b for _, b in r], bodies)
        with self.assertRaisesRegex(ValueError, "64 hex"):
            deltapack.Reader(path, key="not a key")
        self.write([(b"m", b"plain")])
        self.assertFalse(deltapack.Reader(self.path).sealed)

    def test_a_file_packed_by_the_cli_reads_the_same(self):
        lines = [request("a", t) for t in range(1, 6)]
        subprocess.run([os.environ["DELTAPACK_BIN"], "log", "pack", self.path], input=b"".join(l + b"\n" for l in lines), check=True)
        self.assertEqual([body for _, body in deltapack.Reader(self.path)], lines)


if __name__ == "__main__":
    unittest.main()
