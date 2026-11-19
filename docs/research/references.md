# References for the compression research report

Retrieved 2026-11-19. Every URL below returned HTTP 200 (or the content was fetched) on that date. Facts are quoted or closely paraphrased from the fetched source. Where a fact came from a secondary source or could not be confirmed, it is marked **(unverified)** or **(secondary)**.

---

## 1. Zstandard

**[1]** Y. Collet, M. Kucherawy (Ed.), "Zstandard Compression and the 'application/zstd' Media Type", RFC 8878 (Informational, obsoletes RFC 8478), IETF, Feb 2021, https://www.rfc-editor.org/rfc/rfc8878
- Frame magic number: `0xFD2FB528` (little-endian). Skippable-frame magic: `0x184D2A5?`, i.e. any value from `0x184D2A50` to `0x184D2A5F`.
- "The minimum Window_Size is 1 KB. The maximum Window_Size is (1<<41) + 7*(1<<38) bytes, which is 3.75 TB."
- §3.1.1.1.2: "it's recommended for decoders to support values of Window_Size up to 8 MB and for encoders not to generate frames requiring a Window_Size larger than 8 MB." This is only a recommendation. §7.2 registers the `zstd` HTTP content coding.

**[2]** N. Jaju (Ed.), W. F. Handte (Ed.), "Window Sizing for Zstandard Content Encoding", RFC 9659 (Informational, updates RFC 8878), IETF, Sep 2024, https://www.rfc-editor.org/rfc/rfc9659
- This RFC turns the 8 MB HTTP limit into a requirement: "when using the 'zstd' content coding, decoders MUST support a Window_Size of up to and including 8 MB, and encoders MUST NOT generate frames requiring a Window_Size larger than 8 MB."
- Its motivation is that browsers and user agents limit window sizes to keep memory use down. RFC 8878 only recommended the limit; RFC 9659 makes it mandatory.

**[3]** Meta / facebook/zstd, "zstd(1) manual" (programs/zstd.1.md, dev branch, v1.6.x), https://github.com/facebook/zstd/blob/dev/programs/zstd.1.md
- `--long[=#]`: "enables long distance matching with `#` `windowLog`, if `#` is not present it defaults to `27`. The highest possible value is 31."
- "If `windowLog` is set to larger than 27, `--long=windowLog`, `--max`, or `--memory=windowSize` needs to be passed to the decompressor."
- `--patch-from FILE`: "effectively dictionary compression with some convenient parameter selection, namely that _windowSize_ > _srcSize_." It cannot be combined with `-D`. `--memory` overrides the maximum dictionary size allowed for `--patch-from`, which is 128 MiB.

**[4]** facebook/zstd, `lib/zstd.h` (dev branch), https://github.com/facebook/zstd/blob/dev/lib/zstd.h
- `ZSTD_c_enableLongDistanceMatching=160`: "enabling this parameter increases default ZSTD_c_windowLog to 128 MB except when expressly set to a different value". It is "enabled by default if ZSTD_c_windowLog >= 128 MB and compression strategy >= ZSTD_btopt (== compression level 16+)".
- `ZSTD_WINDOWLOG_LIMIT_DEFAULT 27`: by default the streaming decoder refuses frames that need a window larger than 2^27. `ZSTD_WINDOWLOG_MAX_64 31`.
- `ZSTD_e_flush=1`: "flush any data provided so far, it creates (at least) one new block, that can be decoded immediately on reception; frame will continue: any future data can still reference previously compressed data". This is the streaming primitive for SSE.

**[5]** facebook/zstd, "Zstandard v1.4.5 release notes", GitHub release (22 May 2020; the page shows "22 May", the year is from the release tag date), https://github.com/facebook/zstd/releases/tag/v1.4.5
- Describes `--patch-from` as "a high speed engine for producing and applying patches to files". It works by treating the old file as a dictionary, and it raises the previous dictionary limit "from 32 MB to 2 GB".
- Compared with bsdiff on zstd, WordPress and llvm source tarballs (~31 MB, ~273 MB, ~1.66 GB): level 19 gives patch sizes "comparable with `bsdiff`" at about a 7x average speedup. Level 1 is more than 200x faster and level 3 more than 100x faster than bsdiff, with patches still under 0.5% of the original size.

**[6]** facebook/zstd, zstd(1) manual, "DICTIONARY BUILDER" section (same URL as [3])
- `--train FILEs`: the training set "should ideally contain a lot of samples (> 100), and weight typically 100x the target dictionary size (for example, ~10 MB for a 100 KB dictionary)". For large samples, "only the first 128 KiB of these samples will be used for training."
- "Default `--train` is equivalent to `--train-fastcover=d=8,steps=4`." `--maxdict` default: 112640 bytes (110 KiB).
- `--train-fastcover[=k#,d=#,f=#,steps=#,split=#,accel=#]` defaults: split = 75, f = 20 (0 < f < 32), accel = 1 (0 < accel <= 10), and d must be 6 or 8.

**[7]** facebook/zstd, "Zstandard Seekable Format" (contrib/seekable_format/zstd_seekable_compression_format.md), https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
- The seek table is a skippable frame: `Skippable_Magic_Number` (4 B, value `0x184D2A5E`) | `Frame_Size` (4 B) | `[Seek_Table_Entries]` (8 to 12 B each) | `Seek_Table_Footer` (9 B).
- Footer: `Number_Of_Frames` (4 B) | `Seek_Table_Descriptor` (1 B; bit 7 = `Checksum_Flag`) | `Seekable_Magic_Number` (4 B, `0x8F92EAB1`), which "must be the last bytes present in the compressed file".
- Each entry is `Compressed_Size` (4 B) | `Decompressed_Size` (4 B) | optional `Checksum` (4 B, "the least significant 32 bits of the XXH64 digest of the uncompressed data"). Cumulative compressed sizes give each frame's offset.

## 2. Brotli and Compression Dictionary Transport

**[8]** J. Alakuijala, Z. Szabadka, "Brotli Compressed Data Format", RFC 7932 (Informational), IETF, Jul 2016, https://www.rfc-editor.org/rfc/rfc7932
- "The sliding window size is a power of two, minus 16, where the power is in the range of 10 to 24. The possible sliding window sizes range from 1 KiB - 16 B to 16 MiB - 16 B." In the header this is WBITS in 10..24, with window size = (1 << WBITS) - 16.
- Each meta-block decompresses to 0 to 16,777,216 (16 MiB) bytes.
- Built-in static dictionary: "the length is 122,784 bytes and the CRC-32 of the byte sequence is 0x5136cb04", which is about 120 KiB.

**[9]** google/brotli, `c/include/brotli/encode.h`, https://github.com/google/brotli/blob/master/c/include/brotli/encode.h
- `BROTLI_MIN_QUALITY 0`, `BROTLI_MAX_QUALITY 11`, `BROTLI_DEFAULT_QUALITY 11`.
- `BROTLI_PARAM_LGWIN`: `BROTLI_MIN_WINDOW_BITS 10`, `BROTLI_MAX_WINDOW_BITS 24`, `BROTLI_DEFAULT_WINDOW 22`, and `BROTLI_LARGE_MAX_WINDOW_BITS 30` (non-RFC "large window" mode).
- `BROTLI_OPERATION_FLUSH = 1` is the streaming flush operation.

**[10]** P. Meenan (Ed.), Y. Weiss (Ed.), "Compression Dictionary Transport", RFC 9842 (Standards Track), IETF, Sep 2025, https://www.rfc-editor.org/rfc/rfc9842
- Headers: `Use-As-Dictionary` (response; Structured Field with `match`, `match-dest`, `id`, `type`) and `Available-Dictionary` (request; SHA-256 of one dictionary as a Structured Field Byte Sequence). The example uses `Accept-Encoding: gzip, br, zstd, dcb, dcz`.
- `dcb`: a 36-byte header (magic `0xff 0x44 0x43 0x42` plus the 32-byte SHA-256 of the dictionary) followed by a Shared Brotli stream. Clients "MUST be able to decompress ... a window size of up to 16 MB".
- `dcz`: a 40-byte header (8-byte magic `0x5e 0x2a 0x4d 0x18 0x20 0x00 0x00 0x00`, i.e. a zstd skippable frame `0x184D2A5E` of length 32, plus the SHA-256). The window must be "at least 8 MB or 1.25 times the size of the dictionary, whichever is greater, up to a maximum of 128 MB."

**[11]** W3C WebDX / web-features, "Compression Dictionary Transport" (browser support), https://web-platform-dx.github.io/web-features-explorer/features/compression-dictionary-transport
- Supported in Chrome 130 (2024-10-15), Chrome Android 130, and Edge 130 (2024-10-17). Not supported in Firefox or Safari. Baseline status: "Limited availability".
- (secondary) Chrome docs say the encodings were `br-d`/`zstd-d` until M126 and became `dcb`/`dcz` from M127: https://developer.chrome.com/blog/shared-dictionary-compression

## 3. LZ4

**[12]** Y. Collet / lz4 project, "LZ4 Frame Format Description" (doc/lz4_Frame_format.md), https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
- Frame magic: `0x184D2204`.
- Block Independence flag: "If this flag is set to '1', blocks are independent. If this flag is set to '0', each block depends on previous ones (up to LZ4 window size, which is 64 KB)". Linked blocks improve ratio "especially for small blocks", but they make "random access or multi-threaded decoding impossible."
- Block Maximum Size values: 4 = 64 KB, 5 = 256 KB, 6 = 1 MB, 7 = 4 MB.

**[13]** lz4 project, README.md, https://github.com/lz4/lz4/blob/dev/README.md
- "compression speed > 500 MB/s per core, scalable with multi-core CPUs", with a decoder at "multiple GB/s per core".
- lzbench v2.4.1, Silesia corpus, single thread, Core i7-9700K: LZ4 default v1.11.0 has ratio 2.112, 770 MB/s compression and 4950 MB/s decompression. Zstandard 1.5.7 -1 has ratio 2.896, 510 / 1570 MB/s. LZ4 HC -9 has ratio 2.717, 40 / 4800 MB/s.

## 4. xz/LZMA2, DEFLATE, gzip

**[14]** L. Collin / Tukaani, "The .xz File Format", Version 1.2.1 (2024-04-08), https://tukaani.org/xz/xz-file-format.txt
- Header magic: `FD 37 7A 58 5A 00`.
- LZMA2 dictionary size uses a one-bit mantissa and a five-bit exponent: "The smallest dictionary size is 4 KiB and the biggest is 4 GiB."

**[15]** Tukaani, xz(1) man page, https://tukaani.org/xz/man/xz.1.html
- "The default is -6." The preset table gives `-6` an 8 MiB dictionary with 94 MiB compressor memory and 9 MiB decompressor memory, and `-9` a 64 MiB dictionary with 674 MiB / 65 MiB.

**[16]** P. Deutsch, "DEFLATE Compressed Data Format Specification version 1.3", RFC 1951, IETF, May 1996, https://www.rfc-editor.org/rfc/rfc1951
- "the 'deflate' format limits distances to 32K bytes and lengths to 258 bytes". Back-references reach "up to 32K input bytes before".
- Worst-case expansion is 5 bytes per 32K-byte block (stored blocks).

**[17]** P. Deutsch, "GZIP file format specification version 4.3", RFC 1952, IETF, May 1996, https://www.rfc-editor.org/rfc/rfc1952
- Fixed header bytes ID1 = 31 (0x1f) and ID2 = 139 (0x8b). The trailer is CRC32 plus ISIZE (input size mod 2^32).

**[18]** J.-l. Gailly, M. Adler, "zlib 1.3.x Manual", https://www.zlib.net/manual.html
- `Z_SYNC_FLUSH`: "all pending output is flushed to the output buffer and the output is aligned on a byte boundary ... This completes the current deflate block and follows it with an empty stored block that is three bits plus filler bits to the next byte, followed by four bytes (00 00 ff ff)."
- "Flushing may degrade compression for some compression algorithms and so it should be used only when necessary." `Z_FULL_FLUSH` also resets state, and "Using Z_FULL_FLUSH too often can seriously degrade compression."

## 5. Content-defined chunking

**[19]** W. Xia, Y. Zhou, H. Jiang, D. Feng, Y. Hua, Y. Hu, Q. Liu, Y. Zhang, "FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication", USENIX ATC '16, pp. 101–114, 2016, https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia
- "FastCDC is about 10x faster than the best of open-source Rabin-based CDC, and about 3x faster than the state-of-the-art Gear- and AE-based CDC, while achieving nearly the same deduplication ratio as the classic Rabin-based approach."
- The three techniques are a simplified and enhanced cut-point judgment, sub-minimum cut-point skipping, and normalized chunking.

**[20]** W. Xia, X. Zou, H. Jiang, Y. Zhou, C. Liu, D. Feng, Y. Hua, Y. Hu, Y. Zhang, "The Design of Fast Content-Defined Chunking for Data Deduplication Based Storage Systems", IEEE TPDS 31(9):2017–2031, Sep 2020, doi:10.1109/TPDS.2020.2984632, PDF: https://ranger.uta.edu/~jiang/publication/Journals/2020/2020-IEEE-TPDS(Wen%20Xia).pdf
- Five techniques, the fifth being "rolling two bytes each time". "FastCDC is 3-12X faster than the state-of-the-art CDC approaches". In Destor it gives "1.2-3.0X higher throughput".
- Gear hash: `fp = (fp << 1) + G(b)`, using "an array of 256 random 64-bit integers". With mask 0x1fff (8 KB expected chunk), the effective Gear window is 13 bytes versus 48 bytes for Rabin.
- Normalization levels 1, 2 and 3 use mask bits (14,12), (15,11) and (16,10) before and after the expected chunk size.

## 6. Delta / remote-diff

**[21]** A. Tridgell, P. Mackerras, "The rsync algorithm", Tech. Report TR-CS-96-05, Australian National University, June 1996, https://rsync.samba.org/tech_report/ (PDF mirror: https://www.andrew.cmu.edu/course/15-749/READINGS/required/cas/tridgell96.pdf)
- The receiver splits B into fixed blocks of S bytes and computes "a weak 'rolling' 32-bit checksum and a strong 128-bit MD4 checksum" for each. The sender then searches A for matches "at any offset, not just multiples of S". The rolling checksum uses M = 2^16.
- Results on Linux 1.99.10 → 2.0.0 tarballs (~24 MB): GNU diff output was 2.1 MB. With block size 500, rsync wrote 1,283,906 bytes and read 979,384 bytes. Each run took about 2 minutes on a 50 MHz SPARC 10, against about 4 minutes for GNU diff.

**[22]** D. Korn, J. MacDonald, J. Mogul, K. Vo, "The VCDIFF Generic Differencing and Compression Data Format", RFC 3284 (Standards Track), IETF, June 2002, https://www.rfc-editor.org/rfc/rfc3284
- There are three delta instructions: ADD, COPY and RUN. COPY addresses a substring of U = S (source) + T (target so far).
- Header magic bytes: `0xD6 0xC3 0xC4` (the letters "VCD" with high bits set) plus a version byte. The `VCD_SOURCE` window indicator bit marks a source segment.

**[23]** C. Percival, "Binary diff/patch utility" (bsdiff) and "Naive differences of executable code" (unpublished, 2003), https://www.daemonology.net/bsdiff/
- "bsdiff routinely produces binary patches 50-80% smaller than those produced by Xdelta".
- Memory is "max(17*n,9*n+m)+O(1) bytes" and running time is O((n+m) log n), where n is the old size and m the new size.

## 7. Log compression

**[24]** K. Rodrigues, Y. Luo, D. Yuan, "CLP: Efficient and Scalable Search on Compressed Text Logs", OSDI '21, 2021, https://www.usenix.org/conference/osdi21/presentation/rodrigues (PDF: https://www.usenix.org/system/files/osdi21-rodrigues.pdf)
- "CLP's compression ratio is significantly higher compared to all tested compressors (e.g., 2x of Gzip) ... even includes industry-standard tools like Zstandard at their highest (and slowest) compression level." The baselines were Gzip 1.6, Zstandard 1.3.3 and 7z-lzma.
- In default mode without custom parsing rules, "CLP's average compression ratio is 32". Elasticsearch and Splunk Enterprise reach 1.75 and 2.86.
- Search is 8x faster than sequential tools on compressed data, and 4.2x and 1.3x faster than Splunk Enterprise and Elasticsearch. Ingestion is more than 13x faster than both.

**[25]** J. Zhu, S. He, P. He, J. Liu, M. R. Lyu, "Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics", IEEE ISSRE 2023, pp. 355–366, doi:10.1109/ISSRE59848.2023.00071, arXiv:2008.06448, https://arxiv.org/abs/2008.06448. Note that the first author is Jieming Zhu, not He.
- arXiv v3: "loghub provides 19 real-world log datasets". "All these logs amount to over 77 GB in total". Downloaded 90,000+ times by more than 450 organizations.

**[26]** logpai, "loghub" GitHub repository, https://github.com/logpai/loghub (full data on Zenodo: https://zenodo.org/records/8196385)
- Example sizes from the README: HDFS_v1 has 11,175,629 lines (1.47 GiB), Thunderbird 211,212,192 lines (29.60 GiB), Windows 114,608,388 lines (26.09 GiB), and BGL 4,747,963 lines (708.76 MiB).
- 2k sample files: e.g. `HDFS/HDFS_2k.log` (https://raw.githubusercontent.com/logpai/loghub/master/HDFS/HDFS_2k.log) has exactly 2,000 lines and 287,848 bytes (checked on download).
- License: "freely available for research or academic work". There is no SPDX license, so users must cite the paper.

**[27]** J. Wei, G. Zhang, Y. Wang, Z. Liu, Z. Zhu, J. Chen, T. Sun, Q. Zhou, "On the Feasibility of Parser-based Log Compression in Large-Scale Cloud Systems" (LogReducer), USENIX FAST '21, 2021, https://www.usenix.org/conference/fast21/presentation/wei (PDF: https://www.usenix.org/system/files/fast21-wei.pdf)
- Evaluated on 18 types of AliCloud logs (1.76 TB) and 16 types of public logs (77 GB). Against LZMA it reaches 1.19x to 5.30x the compression ratio and 0.56x to 3.16x the compression speed on logs over 100 MB. Against Logzip it reaches 1.03x to 4.01x the ratio and 2.05x to 182.31x the speed.
- On production logs its ratio is 1.54x to 6.78x that of gzip. The authors' collaborator "can generate several PBs of logs per day."

**[28]** J. Liu, J. Zhu, S. He, P. He, Z. Zheng, M. R. Lyu, "Logzip: Extracting Hidden Structures via Iterative Clustering for Log Compression", IEEE/ACM ASE 2019, pp. 863–873, doi:10.1109/ASE.2019.00085, arXiv:1910.00409, https://arxiv.org/abs/1910.00409
- Evaluated on "five large log datasets ... with a total of 63.6 GB". Logzip "can save about half of the storage space on average over traditional compression tools". Code: https://github.com/logpai/logzip

## 8. Prompt caching (the dedup analogy)

**[29]** Anthropic, "Prompt caching", Claude Platform docs, https://platform.claude.com/docs/en/build-with-claude/prompt-caching (redirected from docs.claude.com / docs.anthropic.com)
- "Cache write tokens with a 5-minute TTL are 1.25 times the base input tokens price". 1-hour TTL writes cost "2 times". "Cache read tokens are 0.1 times the base input tokens price". The current page lists lower read multipliers (0.05x and 0.025x) for some newer models.
- "By default, the cache has a 5-minute lifetime." The optional `"ttl": "1h"` gives one hour. The cache "is refreshed for no additional cost each time the cached content is used." Up to 4 cache breakpoints are allowed.
- The minimum cacheable length depends on the model: 512, 1,024, 2,048 or 4,096 tokens. Shorter prompts are processed without caching and return no error.

**[30]** OpenAI, "Prompt caching", OpenAI API docs, https://developers.openai.com/api/docs/guides/prompt-caching (redirected from platform.openai.com/docs/guides/prompt-caching)
- "Prompt caching is enabled by default for supported OpenAI models." "The minimum cacheable prompt length is 1,024 tokens" (current models). On earlier models, cached tokens are reported "rounding down to the nearest multiple of 128."
- Cached input is discounted by up to 95%. On current models, reads cost 0.1x the input rate (0.05x on some) and writes cost 1.25x, with a default minimum lifetime of 30 minutes. On older models, in-memory entries last "around 5 to 10 minutes of inactivity, up to one hour", and extended retention lasts up to 24 hours.
- `prompt_cache_key` affects routing but does not guarantee a cache hit.
- Caveat: this page changes often. The figures were current on 2026-11-19, and older blog citations ("50% discount") are out of date.

## 9. HTTP compression for LLM APIs and SSE

**[31]** Anthropic, "Claude Code CHANGELOG", v2.1.300 entry, https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md
- "Changed large requests to api.anthropic.com to use gzip compression even when telemetry is disabled; set `CLAUDE_CODE_GZIP_REQUEST_BODIES=0` to turn it off". This is first-party evidence that api.anthropic.com accepts `Content-Encoding: gzip` **request** bodies, at least from Claude Code. The v2.1.325 entry mentions decoding of "zstd-compressed responses".
- Neither the Anthropic nor the OpenAI Python SDK's `_base_client.py` contains code that sets request `Content-Encoding` (grep on main, 2026-11-19). Response decompression is left to httpx via `Accept-Encoding`.
- **(unverified)** No official Anthropic API reference page documents which request codings are accepted (gzip / br / zstd) or any size limits. **(unverified)** No official OpenAI docs on compressed request bodies were found. Third-party OpenAI-compatible hosts document gzip-only request compression, e.g. DeepInfra (https://docs.deepinfra.com/request-compression) and Cerebras (https://inference-docs.cerebras.ai/capabilities/payload-optimization.md).

**[32]** WHATWG, "HTML Living Standard §9.2 Server-sent events", https://html.spec.whatwg.org/multipage/server-sent-events.html
- MIME type `text/event-stream`. "Event streams in this format must always be encoded as UTF-8." The recognized fields are `event`, `data`, `id` and `retry`.
- To keep legacy proxies from dropping idle connections, "authors can include a comment line (one starting with a ':' character) every 15 seconds or so." It also warns that "HTTP chunking can have unexpected negative effects on the reliability of this protocol."
- The spec says nothing about compression. Compressing SSE through gzip/zstd/br depends on a flush after each event: zlib `Z_SYNC_FLUSH` [18], `ZSTD_e_flush` [4], `BROTLI_OPERATION_FLUSH` [9]. Each `Z_SYNC_FLUSH` adds a 4-byte `00 00 ff ff` empty stored-block marker [18].

## 10. WebSocket compression

**[33]** T. Yoshino, "Compression Extensions for WebSocket", RFC 7692 (Standards Track), IETF, Dec 2015, https://www.rfc-editor.org/rfc/rfc7692
- The permessage-deflate parameters are `server_no_context_takeover`, `client_no_context_takeover`, `server_max_window_bits` and `client_max_window_bits`. Window bits are "between 8 to 15, inclusive". If absent, the window is "up to 32,768 bytes."
- Compressed messages set the RSV1 bit. The sender removes the trailing 4 octets `0x00 0x00 0xff 0xff` (the sync-flush marker) and the receiver appends them before inflating.
- Context takeover keeps the LZ77 window across messages, which helps repetitive JSON message streams. Clients MUST support `client_no_context_takeover`.

## 11. Columnar storage

**[34]** Apache Software Foundation, "Parquet Encodings" (parquet-format/Encodings.md), https://github.com/apache/parquet-format/blob/master/Encodings.md
- Encoding ids: PLAIN = 0, PLAIN_DICTIONARY = 2 / RLE_DICTIONARY = 8, RLE = 3, BIT_PACKED = 4 (deprecated), DELTA_BINARY_PACKED = 5, DELTA_LENGTH_BYTE_ARRAY = 6, DELTA_BYTE_ARRAY = 7, BYTE_STREAM_SPLIT = 9, ALP = 10.
- Dictionary: "stored in a dictionary page per column chunk", with ids in RLE/bit-packing hybrid. "If the dictionary grows too big ... the encoding will fall back to the plain encoding."
- DELTA_BYTE_ARRAY is "incremental encoding or front compression": each entry stores the prefix length shared with the previous entry plus the suffix.

**[35]** Apache Parquet, "Compression.md" and "README.md" (parquet-format), https://github.com/apache/parquet-format/blob/master/Compression.md , https://github.com/apache/parquet-format/blob/master/README.md
- The ZSTD codec is "based on the Zstandard format defined by RFC 8878". The other codecs are UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4 (deprecated framing) and LZ4_RAW.
- README: "We recommend large row groups (512MB - 1GB)" and "We recommend 8KB for page sizes."

**[36]** Apache Parquet, "Parquet Page Index: Layout to Support Page Skipping" (PageIndex.md), https://github.com/apache/parquet-format/blob/master/PageIndex.md
- "a *page index* is optional metadata for a ColumnChunk, containing statistics for DataPages that can be used to skip those pages". It is stored as the `OffsetIndex` and `ColumnIndex` structures in parquet.thrift.

**[37]** Apache Arrow, "Message.fbs" (IPC format) and "Columnar.rst", https://github.com/apache/arrow/blob/main/format/Message.fbs , https://github.com/apache/arrow/blob/main/docs/source/format/Columnar.rst
- IPC body compression is `CompressionType { LZ4_FRAME, ZSTD }`. Under `BodyCompressionMethod BUFFER`, each buffer is compressed separately and prefixed with its uncompressed length as an 8-byte little-endian int64, where -1 means not compressed.
- Implementations are recommended to use 8- or 64-byte alignment and padding, and Arrow uses 64-byte alignment.

## 12. LLM-based lossless compression

**[38]** C. S. K. Valmeekam, K. Narayanan, D. Kalathil, J.-F. Chamberland, S. Shakkottai, "LLMZip: Lossless Text Compression using Large Language Models", arXiv:2306.04050, Jun 2023, https://arxiv.org/abs/2306.04050
- Uses LLaMA-7B as the next-token predictor with arithmetic coding. It reaches "0.7101 bits/character on a 1MB section of the text8 dataset" and 0.8426 bpc on a 100 KB Project Gutenberg excerpt.
- Baselines on the full 100 MB text8: the best BSC/ZPAQ result is about 1.4 bpc, and paq8h gets 1.2 bpc. The authors warn that text8 results are "likely ... optimistic" because LLaMA was trained on Wikipedia.

**[39]** G. Delétang, A. Ruoss, P.-A. Duquenne, E. Catt, T. Genewein, C. Mattern, J. Grau-Moya, L. K. Wenliang, M. Aitchison, L. Orseau, M. Hutter, J. Veness, "Language Modeling Is Compression", ICLR 2024, arXiv:2309.10668, https://arxiv.org/abs/2309.10668
- Abstract: Chinchilla 70B "compresses ImageNet patches to 43.4% and LibriSpeech samples to 16.4% of their raw size, beating ... PNG (58.5%) or FLAC (30.3%)".
- Table 1 (2048-byte chunks, 1 GB datasets): Chinchilla 70B raw rate on enwik9 is 8.3%, against gzip at 48.1% chunked and 32.3% unchunked. The adjusted rate, which counts the model's parameters, is about 14,008%, so this is not practical as a storage codec.
- Contexts are limited to 2048 bytes. The paper calls Transformer running time "already very long".

## 13. Lossy context compression for agents

**[40]** H. Jiang, Q. Wu, C.-Y. Lin, Y. Yang, L. Qiu, "LLMLingua: Compressing Prompts for Accelerated Inference of Large Language Models", EMNLP 2023, arXiv:2310.05736, https://arxiv.org/abs/2310.05736
- "allows for up to 20x compression with little performance loss". Evaluated on GSM8K, BBH, ShareGPT and Arxiv-March23.

**[41]** T. Lindenbauer, I. Slinko, L. Felder, E. Bogomolov, Y. Zharov, "The Complexity Trap: Simple Observation Masking Is as Efficient as LLM Summarization for Agent Context Management", DL4C Workshop @ NeurIPS 2025, arXiv:2508.21433, https://arxiv.org/abs/2508.21433
- Uses SWE-agent on SWE-bench Verified across five model configurations. Observation masking "halves cost relative to the raw agent" while "matching, and sometimes slightly exceeding, the solve rate of LLM summarization".
- A hybrid "further reduces costs by 7% and 11% compared to just observation masking or LLM summarization, respectively".
- (secondary, from the v2 abstract via search) With Qwen3-Coder 480B, masking moved the solve rate from 53.8% to 54.8%.

**[42]** J. Yang, C. E. Jimenez, A. Wettig, K. Lieret, S. Yao, K. Narasimhan, O. Press, "SWE-agent: Agent-Computer Interfaces Enable Automated Software Engineering", NeurIPS 2024, arXiv:2405.15793, https://arxiv.org/abs/2405.15793
- History processor: "observations preceding the last 5 are each collapsed into a single line". It also omits all past error messages except the first.
- Table 3 (SWE-bench Lite ablation): "Last 5 Obs." scores 18.0% against 15.0% for "Full history" (−3.0).
- Headline pass@1: 12.5% on SWE-bench and 87.7% on HumanEvalFix.

## 14. Public agent trajectory datasets

**[43]** Nebius, "nebius/SWE-agent-trajectories", Hugging Face dataset, https://huggingface.co/datasets/nebius/SWE-agent-trajectories
- License: CC-BY-4.0. 80,036 trajectories in one train split, 5,627,973,531 bytes (~5.6 GB) uncompressed and 1,114,367,701 bytes (~1.1 GB) as a Parquet download (HF API metadata). Last modified 2024-12-23.
- Resolved: 13,389. Not resolved: 66,647. Average steps are 31.3 resolved vs 58.4 unresolved. Average context is 8,352 vs 15,241 Llama-3 tokens.

**[44]** SWE-Gym, "SWE-Gym/OpenHands-Sampled-Trajectories", Hugging Face dataset, https://huggingface.co/datasets/SWE-Gym/OpenHands-Sampled-Trajectories
- Split `train.raw`: 6,055 trajectories, 1,436,124,109 bytes (~1.4 GB) uncompressed and 301,095,609 bytes as a download. Fields include `messages` with OpenAI-style `tool_calls`, plus `resolved`.
- License: **the dataset card declares none.** The SWE-Gym code repository (https://github.com/SWE-Gym/SWE-Gym) is Apache-2.0, but that does not automatically cover the dataset. The README says it was fine-tuned on "less than 500 agent-environment interaction trajectories" sampled from two hosted models for "+14% absolute gains on SWE-Bench Verified".

**[45]** C. E. Jimenez, J. Yang, A. Wettig, S. Yao, K. Pei, O. Press, K. Narasimhan, "SWE-bench: Can Language Models Resolve Real-World GitHub Issues?", ICLR 2024, arXiv:2310.06770, https://arxiv.org/abs/2310.06770 ; dataset https://huggingface.co/datasets/princeton-nlp/SWE-bench
- "2,294 software engineering problems" from "12 popular Python repositories". The best model, Claude 2, resolved "a mere 1.96%".
- HF splits: test 2,294, dev 225, train 19,008. The HF card declares no license.

## 15. Cloud cost figures (retrieved 2026-11-19, AWS Price List API)

**[46]** Amazon Web Services, "AWS Price List – AmazonS3, us-east-1" (publicationDate 2026-11-18T16:18:49Z), https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json ; human page https://aws.amazon.com/s3/pricing/
- S3 Standard: "$0.023 per GB - first 50 TB / month", then $0.022 for the next 450 TB and $0.021 over 500 TB.
- Other classes for context: Standard-IA $0.0125, Glacier Instant Retrieval $0.004 and Glacier Flexible Retrieval $0.0036 per GB-month.

**[47]** Amazon Web Services, "AWS Price List – AmazonS3GlacierDeepArchive, us-east-1" (publicationDate 2026-09-11T12:45:07Z), https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3GlacierDeepArchive/current/us-east-1/index.json
- "$0.00099 per GB-Month for storage used in Glacier Deep Archive in US East (N. Virginia)".
- The S3 offer file adds retrieval fees of $0.02/GB Standard and $0.0025/GB Bulk. The S3 pricing page states "a minimum storage duration of 180 days."

**[48]** Amazon Web Services, "AWS Price List – AWSDataTransfer, us-east-1" (publicationDate 2026-11-19T10:58:41Z), https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json
- Internet egress from US East (N. Virginia): "$0.090 per GB - first 10 TB / month data transfer out beyond the global free tier". Then $0.085 for the next 40 TB, $0.070 for the next 100 TB, and $0.050 above 150 TB.
- The S3 pricing page says the first 100 GB per month out to the internet is free (aggregated across services).

---

## Unverified or caveated items

1. **Which request `Content-Encoding` values (gzip / br / zstd) api.anthropic.com accepts, and any limits.** There is no official API documentation. The only first-party evidence is the Claude Code changelog entry about gzip request bodies [31].
2. **Whether api.openai.com accepts compressed request bodies.** No official documentation was found and it was not tested. Only third-party OpenAI-compatible hosts document gzip-only support.
3. **Whether rsync's 128-bit MD4 strong checksum is still used.** The 1996 report says MD4. Modern rsync defaults (MD5/xxhash) were not checked.
4. **Exact chromestatus entry IDs for Compression Dictionary Transport.** These were not fetched. The Chrome 130 support comes from web-features [11].
5. **The Qwen3-Coder 53.8% → 54.8% number in [41].** It came from search-result text for arXiv v2, not a direct fetch. The fetched abstract (v3) does not include it.
6. **The year of the zstd v1.4.5 release (2020).** The fetched page shows only "22 May".
7. **Licenses for SWE-Gym/OpenHands-Sampled-Trajectories and princeton-nlp/SWE-bench on HF.** Neither card declares one. Do not assume a license.
8. **Prompt-caching multipliers and minimums [29][30].** These change frequently, and the per-model tables above are a snapshot from 2026-11-19.
9. **The LMIC [39] abstract figures (43.4% / 16.4%)** differ slightly from Table 1 in the fetched PDF (48.0% / 21.0% for Chinchilla 70B on 2048-byte chunks). Cite the one that matches the version you reference.
10. **The 8 MB zstd window limit.** RFC 8878 only *recommends* it in general. RFC 9659 [2] makes it a MUST for the HTTP `zstd` content coding. Cite RFC 9659 for HTTP.
