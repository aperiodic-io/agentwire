# Deltapack (TypeScript)

Reads DPK1 files in Node 22.19 or later, with no dependencies (Node's own zstd and crypto): what
`deltapack collect`, the CLI and the Rust, Python and Go libraries write
([SPEC.md §7](../SPEC.md#7-dpk1-a-log-of-bodies-stored-as-deltas)), sealed files included. Every
body is checked by SHA-256.

```ts
import { Reader } from "deltapack";

const r = Reader.open("1763650000000-4242-000000.dpk");
r.length, r.truncated, r.records[17].meta.toString();
const body = r.get(17);                  // one record, rebuilt through its bases
for (const [record, body] of r) { }      // every record in order, each rebuilt once

Reader.open("sealed.dpk").withKey(hexKey).get(0);
```

A body that does not rebuild what it names throws a `DeltapackError` (`code`: `corrupt`,
`malformed`, `too-large`, …), never returns wrong bytes. Given a key, a file that is not sealed
is refused. The file is read into memory; for files larger than memory, use the CLI or the Rust
library.

Tests: `npm test`, with `DELTAPACK_BIN` naming the CLI (the tests read files it packs).
