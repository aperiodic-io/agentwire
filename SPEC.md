# AWD1: request bodies as deltas against the previous request

Version 1. Status: stable wire format for agentwire 0.x; changes need a new version byte.

LLM APIs are called with the whole conversation in every request, so request *k+1* is request *k*
plus a turn. AWD1 lets an HTTP client send it as an edit of a body the server already holds, and
lets the server rebuild the exact bytes before its handler sees them. It needs no session or
connection state: a base is named by its SHA-256, and a client only uses a base the server
confirmed it kept.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Headers

| Header | Direction | Value | Meaning |
|---|---|---|---|
| `awd1-keep` | request | `?1` | Keep this request's body (after rebuilding, if it is a delta) as a base. |
| `awd1-kept` | response | lowercase hex SHA-256, 64 characters | The server kept the request's body under this hash. |
| `content-encoding: awd1` | request | | The body is a delta (§3). It MUST be the only content coding. |
| `awd1-error` | response | a token (§4) | The server refused the delta. It did not pass the request on. |

## 2. Exchange

1. A client sends a request in full with `awd1-keep: ?1`.
2. A server that implements AWD1 keeps the body and answers with `awd1-kept: <sha256>`. A server
   that does not, ignores the header, and the client never sends it a delta. This is how support
   is discovered.
3. For a later request, the client MAY send a delta against any body whose hash a response named
   in `awd1-kept`. It SHOULD pick the one that shares the most bytes with the new body. It SHOULD
   send the body in full when the delta would not be smaller.
4. The server rebuilds the body and checks its SHA-256 (§3), then hands the handler the rebuilt
   body. It sets `content-length` and removes `content-encoding: awd1` and `awd1-keep`. The handler
   cannot tell the request was a delta.
5. If the server refuses a delta (§4), it MUST NOT have passed the request on. The client SHOULD
   send the same request again in full, and MUST NOT use that base again.

A server MAY drop a base at any time (memory budget, TTL, restart). That costs the client one
full-size request, never an error.

## 3. Delta body

```
delta  = "AWD1" version base target prefix suffix target_len frame
version    = %x01
base       = 32 bytes, SHA-256 of the base body
target     = 32 bytes, SHA-256 of the rebuilt body
prefix     = varint   ; bytes copied from the start of the base
suffix     = varint   ; bytes copied from the end of the base
target_len = varint   ; length of the rebuilt body
frame      = one Zstandard frame (RFC 8878)
varint     = canonical unsigned LEB128, at most 10 bytes, at most 2^64 - 1
```

With `B` the base, the body is

```
T = B[0 .. prefix] ++ literal ++ B[len(B) - suffix .. len(B)]
```

where `literal` is the content of `frame`, `target_len - prefix - suffix` bytes long. The frame is
compressed with `B[prefix .. len(B) - suffix]` (the span of the base the literal replaces) as its
raw-content prefix (`ZSTD_CCtx_refPrefix`). So content that moved, or survived, inside the edited
span is still found. An encoder SHOULD enable long-distance matching and set the window to cover
that prefix plus the literal, up to 2^27 bytes.

A receiver MUST reject:

- a magic other than `AWD1`, or a version it does not implement (`unsupported-version`);
- a truncated header, a non-canonical or overlong varint, or `prefix + suffix > target_len`
  (`malformed`);
- `target_len` over its size limit, or a delta body over it (`too-large`);
- a base it does not hold under the request's scope (§5) (`unknown-base`);
- `prefix + suffix > len(B)` (`malformed`);
- a frame that does not decode, with that prefix, to exactly `target_len - prefix - suffix` bytes,
  or that needs a window over 2^27 bytes; and a rebuilt body whose SHA-256 is not `target`
  (`corrupt`).

A receiver MUST bound the memory it allocates for a delta by its size limit, before decompressing.

## 4. Errors

| `awd1-error` | Status | Cause |
|---|---|---|
| `unknown-base` | 409 | The base was never kept, was dropped, or belongs to another scope. |
| `too-large` | 413 | The body, rebuilt or as sent, exceeds the server's limit. |
| `malformed` | 400 | The delta does not parse, or does not fit its base. |
| `unsupported-version` | 400 | The version byte is not 1. |
| `corrupt` | 400 | The frame does not decode, or the rebuilt body does not hash to `target`. |

## 5. Scope and security

- **Scope.** A server MUST keep bases per tenant, and MUST look a base up only under the scope of
  the request that names it. The reference implementation derives the scope from the
  `authorization` and `x-api-key` headers (configurable). This makes a hash a capability only for
  the tenant that sent the body. Without scoping, a hit would confirm to anyone that someone else
  sent a given body.
- **Integrity, not authenticity.** SHA-256 over the rebuilt body guarantees the server forwards
  exactly what the client hashed. It says nothing about who sent it. Run AWD1 over TLS, behind the
  same authentication as the API.
- **Compression side channel.** A delta's size depends on how the new body differs from the base.
  An observer of encrypted traffic who can also inject content into a conversation (a fetched page,
  a tool result) learns more than from a plain request, as with any compression of secrets mixed
  with attacker-chosen data (CRIME, BREACH). Bases never cross scopes, which keeps one tenant's
  content out of another's deltas. Where the residual risk matters, do not enable AWD1 for that
  traffic.
- **Memory.** A server bounds kept bases by a byte budget and a TTL. It also bounds each rebuild by
  its size limit, and the zstd window by 2^27.

## 6. Relation to other standards

AWD1 is `Compression Dictionary Transport` (RFC 9842, `dcz`) turned around: the dictionary is the
client's own previous request, not a response the server served. A provider that offers `dcz` for
request bodies with the previous request as dictionary would make AWD1 unnecessary. The
`prefix`/`suffix` copy saves a zstd pass over the unchanged context, which `dcz` would compress
again on every request.
