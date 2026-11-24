import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { DeltapackError, Reader } from "../src/index.js";

/** A request that re-sends its conversation: a 20 KB system prompt of its own, then the turns. */
function request(conversation: string, turns: number): string {
  let system = "";
  for (let i = 0; i < 300; i++) system += createHash("sha256").update(`${conversation}${i}`).digest("hex");
  const messages = Array.from({ length: turns }, (_, i) => ({ role: "user", content: `${conversation} turn ${i}` }));
  return JSON.stringify({ system, messages });
}

/** Records whose deltas need the base's content as zstd's prefix: the same chunks, reordered. */
function shuffled(): string[] {
  const chunks = Array.from({ length: 4 }, () => randomBytes(1500).toString("hex"));
  return [
    JSON.stringify({ a: chunks.join("") }),
    JSON.stringify({ b: [chunks[3], chunks[1], chunks[0], chunks[2]].join("") }),
  ];
}

const lines = [...[1, 2, 3, 4, 5, 6, 7, 8].flatMap((t) => ["a", "b", "c"].map((c) => request(c, t))), ...shuffled()];
const bin = process.env.DELTAPACK_BIN;
const dir = mkdtempSync(join(tmpdir(), "deltapack-ts-"));

function pack(name: string, ...args: string[]): string {
  const path = join(dir, name);
  execFileSync(bin!, ["log", ...args, "pack", "--window", "4", path], { input: lines.join("\n") + "\n" });
  return path;
}

test("a file packed by the CLI reads back by index and in order", { skip: !bin }, () => {
  const r = Reader.open(pack("plain.dpk"));
  assert.equal(r.length, lines.length);
  assert.equal(r.truncated, false);
  assert.equal(r.sealed, false);
  assert.equal(r.get(7).toString(), lines[7]);
  assert.equal(r.get(lines.length - 1).toString(), lines.at(-1), "a delta that needs its base as zstd's prefix");
  let i = 0;
  for (const [rec, body] of r) {
    assert.equal(body.toString(), lines[i], `record ${i}`);
    assert.equal(rec.meta.length, 0, "pack writes no meta");
    i++;
  }
  assert.equal(i, lines.length);
  assert.ok(r.records.slice(3).some((rec) => rec.back > 0), "deltas against earlier records");
  assert.throws(() => r.get(lines.length), RangeError);
});

test("a sealed file is read with its key only", { skip: !bin }, () => {
  const key = "ab".repeat(32);
  writeFileSync(join(dir, "key"), key + "\n", { mode: 0o600 });
  const path = pack("sealed.dpk", "--key-file", join(dir, "key"));
  const r = Reader.open(path);
  assert.equal(r.sealed, true);
  assert.throws(() => r.get(0), (e: DeltapackError) => e.code === "key");
  assert.throws(() => Reader.open(path).withKey("cd".repeat(32)), (e: DeltapackError) => e.code === "key");
  r.withKey(key);
  assert.equal(r.get(0).toString(), lines[0]);
  assert.deepEqual([...r].map(([, b]) => b.toString()), lines);
  assert.throws(() => Reader.open(pack("plain2.dpk")).withKey(key), (e: DeltapackError) => e.code === "not-sealed");
  // a sealed record changed in place does not open
  const data = readFileSync(path);
  data[data.length - 3] ^= 1;
  assert.throws(() => new Reader(data).withKey(key).get(lines.length - 1), (e: DeltapackError) => e.code === "corrupt");
});

test("a corrupt record is refused, never returned wrong", { skip: !bin }, () => {
  const data = readFileSync(pack("corrupt.dpk"));
  data[data.length - 5] ^= 1;
  assert.throws(() => new Reader(data).get(lines.length - 1), (e: DeltapackError) => e.code === "corrupt");
});

test("a file cut short keeps its whole records", { skip: !bin }, () => {
  const data = readFileSync(pack("cut.dpk"));
  const r = new Reader(data.subarray(0, data.length - 10));
  assert.equal(r.truncated, true);
  assert.equal(r.length, lines.length - 1);
  assert.equal(r.get(r.length - 1).toString(), lines[r.length - 1]);
});

test("a body over the reader's limit is refused", { skip: !bin }, () => {
  const r = new Reader(readFileSync(pack("limit.dpk")), 100);
  assert.throws(() => r.get(0), (e: DeltapackError) => e.code === "too-large");
});

test("what is not a DPK1 file is refused", () => {
  for (const s of ["", "DPK2", "nope, not one"]) {
    assert.throws(() => new Reader(Buffer.from(s)), (e: DeltapackError) => e.code === "malformed");
  }
  assert.throws(() => new Reader(Buffer.from("DPK1\x09\x04", "latin1")), (e: DeltapackError) => e.code === "unsupported-version");
  // a record naming a base before the file's start
  assert.throws(() => new Reader(Buffer.from([0x44, 0x50, 0x4b, 0x31, 1, 4, 1, 0, 0])), (e: DeltapackError) =>
    e.message.includes("names a base"),
  );
  // non-canonical and overlong varints
  assert.throws(() => new Reader(Buffer.from([0x44, 0x50, 0x4b, 0x31, 1, 0x84, 0x00])), (e: DeltapackError) =>
    e.message.includes("non-canonical"),
  );
});
