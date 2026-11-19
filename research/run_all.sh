#!/usr/bin/env bash
# Rebuild the research corpora from public sources and rerun every research benchmark.
set -euo pipefail
cd "$(dirname "$0")"
python3 build_corpora.py
python3 characterise.py
python3 bench_transport.py --repeats 3 --max-test-bytes 12000000
python3 bench_storage.py --repeats 3 --max-test-bytes 12000000 --slow
python3 bench_sse.py --repeats 5
python3 bench_dict.py
python3 bench_multiplex.py
python3 bench_reducer.py
python3 bench_tokens.py
python3 bench_longctx.py
python3 bench_patchfrom.py
python3 bench_memory.py
python3 summarize.py
python3 ../plot_results.py --research
