#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m pytest -q
python3 -m agentwire demo
python3 -m agentwire tcp-demo
python3 -m agentwire tcp-demo --stream
python3 -m agentwire pipeline-demo
python3 -m agentwire bench --output-dir results --messages 240 --repeats 3
python3 plot_results.py
python3 -m agentwire pack --input examples/tool-output.jsonl --output results/example.awf --dictionary examples/agent-dict-v1.zdict
python3 -m agentwire unpack --input results/example.awf --output results/example-restored.jsonl --dictionary examples/agent-dict-v1.zdict
cmp examples/tool-output.jsonl results/example-restored.jsonl
python3 -m agentwire pack --stream --input examples/tool-output.jsonl --output results/example.awc --dictionary examples/agent-dict-v1.zdict
python3 -m agentwire unpack --input results/example.awc --output results/example-stream-restored.jsonl --dictionary examples/agent-dict-v1.zdict
cmp examples/tool-output.jsonl results/example-stream-restored.jsonl
echo 'All local verification gates passed.'
