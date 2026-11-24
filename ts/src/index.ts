// Reads DPK1 files (deltapack's SPEC.md §7): LLM request logs and trace spans stored as DPD1 deltas
// against earlier records, every body checked by SHA-256. Node 22.19 or later (its zlib's zstd
// takes a dictionary; 22.15 to 22.18 ignore it); no dependencies. Files are those `deltapack collect`, the CLI and the Rust, Python and Go
// libraries write, sealed ones included.

import { createDecipheriv, createHash, hkdfSync } from "node:crypto";
import { readFileSync } from "node:fs";
import { constants, zstdDecompressSync } from "node:zlib";

/** Why a file or a record is refused (SPEC.md §4): `code` is the spec's. */
export class DeltapackError extends Error {
  constructor(
    readonly code: "malformed" | "unsupported-version" | "too-large" | "corrupt" | "not-sealed" | "key",
    message: string,
  ) {
    super(`${code}: ${message}`);
  }
}

/** One record's place: its meta (stored as is), how many records back its base is (0: stored
 * whole), and the bytes its delta takes. */
export interface Record {
  meta: Buffer;
  back: number;
  stored: number;
  offset: number;
}

const MAX_WINDOW_LOG = 27;

function sha256(b: Uint8Array): Buffer {
  return createHash("sha256").update(b).digest();
}

/** Canonical unsigned LEB128, at most 10 bytes, as a number (a body or count never needs more
 * than 2^53). */
function varint(buf: Uint8Array, pos: number): [number, number] {
  let v = 0n;
  for (let i = 0; i < 10; i++) {
    if (pos + i >= buf.length) throw new DeltapackError("malformed", "truncated varint");
    const b = buf[pos + i];
    if (i === 9 && b > 1) throw new DeltapackError("malformed", "varint overflow");
    v |= BigInt(b & 0x7f) << BigInt(7 * i);
    if ((b & 0x80) === 0) {
      if (b === 0 && i > 0) throw new DeltapackError("malformed", "non-canonical varint");
      if (v > BigInt(Number.MAX_SAFE_INTEGER)) throw new DeltapackError("too-large", "a varint past 2^53");
      return [Number(v), pos + i + 1];
    }
  }
  throw new DeltapackError("malformed", "varint overflow");
}

/** Rebuilds a DPD1 delta's target from `base`, the body its header names. */
export function applyDelta(base: Uint8Array, delta: Uint8Array, maxBody: number): Buffer {
  if (delta.length < 5 || Buffer.from(delta.subarray(0, 4)).toString("latin1") !== "DPD1") {
    throw new DeltapackError("malformed", "not a DPD1 body");
  }
  if (delta[4] !== 1) throw new DeltapackError("unsupported-version", `version ${delta[4]}`);
  if (delta.length < 69) throw new DeltapackError("malformed", "truncated header");
  const baseHash = delta.subarray(5, 37);
  const targetHash = delta.subarray(37, 69);
  let pos = 69;
  let prefix: number, suffix: number, targetLen: number;
  [prefix, pos] = varint(delta, pos);
  [suffix, pos] = varint(delta, pos);
  [targetLen, pos] = varint(delta, pos);
  if (targetLen > maxBody) throw new DeltapackError("too-large", `a body of ${targetLen} bytes`);
  if (prefix + suffix > targetLen) throw new DeltapackError("malformed", "prefix and suffix exceed the target");
  if (!sha256(base).equals(baseHash)) throw new DeltapackError("corrupt", "the base is not the body the delta names");
  if (prefix + suffix > base.length) throw new DeltapackError("malformed", "prefix and suffix exceed the base");
  const literalLen = targetLen - prefix - suffix;
  const reference = base.subarray(prefix, base.length - suffix);
  let literal: Buffer;
  try {
    literal = zstdDecompressSync(delta.subarray(pos), {
      // a raw-content dictionary decodes as the encoder's ZSTD_CCtx_refPrefix
      ...(reference.length > 0 ? { dictionary: reference } : {}),
      params: { [constants.ZSTD_d_windowLogMax]: MAX_WINDOW_LOG },
      // exactly the literal's room: a frame that expands further fails to decode
      maxOutputLength: Math.max(literalLen, 1),
    } as object);
  } catch {
    throw new DeltapackError("corrupt", "the frame does not decode");
  }
  if (literal.length !== literalLen) throw new DeltapackError("corrupt", "the frame decodes to another length");
  const target = Buffer.concat([base.subarray(0, prefix), literal, base.subarray(base.length - suffix)]);
  if (!sha256(target).equals(targetHash)) throw new DeltapackError("corrupt", "the rebuilt body is not the one named");
  return target;
}

/** A DPK1 file, read from memory: `length`, `records`, `get(i)`, and `for (const [rec, body] of r)`. */
export class Reader {
  readonly records: Record[] = [];
  /** The file ends inside a record (its writer stopped mid-write); the records before it are whole. */
  readonly truncated: boolean = false;
  readonly window: number;
  private readonly salt?: Buffer;
  private readonly keyId?: Buffer;
  private fileKey?: Buffer;

  /** Indexes `data`, a DPK1 file. `maxBody` caps any body it will rebuild. */
  constructor(
    private readonly data: Buffer,
    private readonly maxBody = 64 << 20,
  ) {
    if (data.length < 5 || data.subarray(0, 4).toString("latin1") !== "DPK1") {
      throw new DeltapackError("malformed", "not a DPK1 file");
    }
    const version = data[4];
    if (version !== 1 && version !== 2) throw new DeltapackError("unsupported-version", `DPK1 version ${version}`);
    let pos: number;
    [this.window, pos] = varint(data, 5);
    if (version === 2) {
      if (data.length < pos + 24) throw new DeltapackError("malformed", "truncated header");
      this.salt = data.subarray(pos, pos + 16);
      this.keyId = data.subarray(pos + 16, pos + 24);
      pos += 24;
    }
    while (pos < data.length) {
      let back: number, metaLen: number, deltaLen: number, p: number;
      try {
        [back, p] = varint(data, pos);
        const i = this.records.length;
        if (back > i || back > this.window) {
          throw new DeltapackError("malformed", `record ${i} names a base ${back} back`);
        }
        [metaLen, p] = varint(data, p);
        if (metaLen > maxBody) throw new DeltapackError("too-large", "a meta past the limit");
        const meta = data.subarray(p, p + metaLen);
        p += metaLen;
        [deltaLen, p] = varint(data, p);
        if (p + deltaLen > data.length || meta.length < metaLen) throw new DeltapackError("malformed", "truncated record");
        this.records.push({ meta, back, stored: deltaLen, offset: p });
        pos = p + deltaLen;
      } catch (e) {
        // a record cut by the end of the file: the ones before it are whole
        if (e instanceof DeltapackError && e.message.includes("truncated")) {
          this.truncated = true;
          return;
        }
        throw e;
      }
    }
  }

  /** Reads and indexes a DPK1 file. */
  static open(path: string, maxBody?: number): Reader {
    return new Reader(readFileSync(path), maxBody);
  }

  get length(): number {
    return this.records.length;
  }

  /** The file's records are readable only with its key (`withKey`). */
  get sealed(): boolean {
    return this.keyId !== undefined;
  }

  /** Makes a sealed file readable with its key (32 bytes, or 64 hex digits). A file that is not
   * sealed is refused: it could have replaced a sealed one. */
  withKey(key: Uint8Array | string): this {
    const k = typeof key === "string" ? Buffer.from(key.trim(), "hex") : Buffer.from(key);
    if (k.length !== 32) throw new DeltapackError("key", "a key is 32 bytes, 64 hex digits");
    if (!this.sealed) throw new DeltapackError("not-sealed", "the file is not sealed");
    if (!sha256(k).subarray(0, 8).equals(this.keyId!)) throw new DeltapackError("key", "the key is not this file's");
    this.fileKey = Buffer.from(hkdfSync("sha256", k, this.salt!, "DPK1 sealed records", 32));
    return this;
  }

  private delta(i: number): Buffer {
    const r = this.records[i];
    const stored = this.data.subarray(r.offset, r.offset + r.stored);
    if (!this.sealed) return stored;
    if (!this.fileKey) throw new DeltapackError("key", "the file is sealed: read it with its key");
    if (stored.length < 16) throw new DeltapackError("corrupt", `record ${i} does not open`);
    const nonce = Buffer.alloc(12);
    nonce.writeBigUInt64LE(BigInt(i));
    const aad = Buffer.alloc(16);
    aad.writeBigUInt64LE(BigInt(i));
    aad.writeBigUInt64LE(BigInt(r.back), 8);
    const d = createDecipheriv("chacha20-poly1305", this.fileKey, nonce, { authTagLength: 16 });
    d.setAAD(Buffer.concat([aad, r.meta]), { plaintextLength: stored.length - 16 });
    d.setAuthTag(stored.subarray(stored.length - 16));
    try {
      return Buffer.concat([d.update(stored.subarray(0, stored.length - 16)), d.final()]);
    } catch {
      throw new DeltapackError("corrupt", `record ${i} does not open`);
    }
  }

  private rebuild(i: number, base: Uint8Array): Buffer {
    try {
      return applyDelta(base, this.delta(i), this.maxBody);
    } catch (e) {
      if (e instanceof DeltapackError) throw new DeltapackError(e.code, `record ${i}: ${e.message}`);
      throw e;
    }
  }

  /** Record `i`'s body, rebuilt through its chain of bases, each checked by SHA-256. */
  get(i: number): Buffer {
    if (!Number.isInteger(i) || i < 0 || i >= this.records.length) {
      throw new RangeError(`record ${i} of ${this.records.length}`);
    }
    const chain = [i];
    for (let j = i; this.records[j].back > 0; ) chain.push((j -= this.records[j].back));
    let body: Buffer = Buffer.alloc(0);
    for (const j of chain.reverse()) body = this.rebuild(j, body);
    return body;
  }

  /** Every record and its body, in order, each rebuilt once (holding the last `window` bodies). */
  *[Symbol.iterator](): IterableIterator<[Record, Buffer]> {
    const recent: Buffer[] = [];
    for (let i = 0; i < this.records.length; i++) {
      const r = this.records[i];
      const body = this.rebuild(i, r.back > 0 ? recent[recent.length - r.back] : Buffer.alloc(0));
      yield [r, body];
      recent.push(body);
      if (recent.length > this.window) recent.shift();
    }
  }
}
