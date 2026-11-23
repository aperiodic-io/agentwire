"""The logpak Python bindings: LPK1 files written and read from Python."""

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest

import logpak


def request(conversation: str, turns: int) -> bytes:
    """A request that re-sends its conversation: a 20 KB system prompt of its own, then the turns."""
    system = "".join(hashlib.sha256(f"{conversation}{i}".encode()).hexdigest() for i in range(300))
    messages = [{"role": "user", "content": f"{conversation} turn {i}"} for i in range(turns)]
    return json.dumps({"system": system, "messages": messages}).encode()


class AwlTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "log.lpk")

    def tearDown(self):
        shutil.rmtree(self.dir)

    def write(self, records, window=16):
        with logpak.Writer(self.path, window=window) as w:
            for i, (meta, body) in enumerate(records):
                self.assertEqual(w.append(body, meta), i)
        return os.path.getsize(self.path)

    def test_records_read_back_by_index_and_in_order(self):
        records = [(f"{c}{t}".encode(), request(c, t)) for t in range(1, 9) for c in "abc"]
        self.write(records, window=4)
        r = logpak.Reader(self.path)
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
        backs = [back for _, back, _ in logpak.Reader(self.path).records()]
        self.assertEqual(backs[3:], [3] * (len(records) - 3), "each delta against its own conversation")

    def test_records_lists_meta_base_and_stored_bytes(self):
        self.write([(b"m0", b"hello"), (b"m1", b"hello world")])
        (m0, back0, stored0), (m1, back1, stored1) = logpak.Reader(self.path).records()
        self.assertEqual((m0, back0, m1, back1), (b"m0", 0, b"m1", 1))
        self.assertGreater(stored0, 0)
        self.assertGreater(stored1, 0)

    def test_bodies_are_bytes_of_any_kind(self):
        bodies = [b"", bytes(range(256)), b"\x00" * 10_000, bytearray(b"from a bytearray")]
        with logpak.Writer(self.path) as w:
            for b in bodies:
                w.append(b)
        self.assertEqual([body for _, body in logpak.Reader(self.path)], [bytes(b) for b in bodies])

    def test_a_writer_never_replaces_a_file(self):
        self.write([(b"", b"kept")])
        with self.assertRaises(FileExistsError):
            logpak.Writer(self.path)
        self.assertEqual(logpak.Reader(self.path)[0], b"kept")

    def test_a_closed_writer_refuses_more_records(self):
        w = logpak.Writer(self.path)
        w.append(b"one")
        w.close()
        w.close()  # closing again is fine
        with self.assertRaises(ValueError):
            w.append(b"two")
        self.assertEqual(len(logpak.Reader(self.path)), 1)

    def test_flushed_records_are_readable_while_the_writer_is_open(self):
        w = logpak.Writer(self.path)
        w.append(b"first")
        w.flush()
        self.assertEqual(logpak.Reader(self.path)[0], b"first")
        w.close()

    def test_a_file_cut_short_keeps_its_whole_records(self):
        self.write([(b"", request("a", t)) for t in range(1, 4)])
        with open(self.path, "r+b") as f:
            f.truncate(os.path.getsize(self.path) - 1)
        r = logpak.Reader(self.path)
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
        r = logpak.Reader(self.path)
        self.assertEqual(r[0], request("a", 1))
        with self.assertRaises(ValueError):
            r[1]
        with self.assertRaises(ValueError):
            list(r)

    def test_what_is_not_an_lpk1_file_is_refused(self):
        with open(self.path, "wb") as f:
            f.write(b"not a log")
        with self.assertRaises(ValueError):
            logpak.Reader(self.path)
        with self.assertRaises(FileNotFoundError):
            logpak.Reader(os.path.join(self.dir, "missing.lpk"))

    def test_an_index_past_the_end_is_an_index_error(self):
        self.write([(b"", b"only")])
        r = logpak.Reader(self.path)
        for i in (1, -2):
            with self.assertRaises(IndexError):
                r[i]

    def test_a_body_over_the_readers_limit_is_refused(self):
        self.write([(b"", b"x" * 1000)])
        with self.assertRaises(ValueError):
            logpak.Reader(self.path, max_body=100)[0]

    def test_writers_in_threads_each_keep_their_own_file(self):
        def write(n):
            with logpak.Writer(os.path.join(self.dir, f"{n}.lpk")) as w:
                for t in range(1, 6):
                    w.append(request(str(n), t))

        threads = [threading.Thread(target=write, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for n in range(4):
            r = logpak.Reader(os.path.join(self.dir, f"{n}.lpk"))
            self.assertEqual([body for _, body in r], [request(str(n), t) for t in range(1, 6)])

    @unittest.skipUnless(os.environ.get("LOGPAK_BIN"), "LOGPAK_BIN: the logpak binary, to read a file it packed")
    def test_a_file_packed_by_the_cli_reads_the_same(self):
        lines = [request("a", t) for t in range(1, 6)]
        subprocess.run([os.environ["LOGPAK_BIN"], "log", "pack", self.path], input=b"".join(l + b"\n" for l in lines), check=True)
        self.assertEqual([body for _, body in logpak.Reader(self.path)], lines)


if __name__ == "__main__":
    unittest.main()
