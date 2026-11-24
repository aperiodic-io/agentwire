"""Checks a deployed deltapack (CI's `deploy` job): posts the backend tests' trace exports to a
collector and says whether they were taken.

  python3 tests/backends/deploy.py post 127.0.0.1:4318          every export answered 200
  python3 tests/backends/deploy.py phoenix 127.0.0.1:6006 3     Phoenix holds 3 traces (within 2 min)
  python3 tests/backends/deploy.py stats "<log stats output>" 6 the output counts 6 records
"""

import sys

from test_backends import TRACES, TURNS, OPENER, eventually, export, post  # noqa: F401
import json
import urllib.request


def main(what, *args):
    if what == "post":
        for turn in range(1, TURNS + 1):
            assert post(f"http://{args[0]}/v1/traces", export(turn)) == 200, turn
    elif what == "phoenix":
        def traces():
            request = urllib.request.Request(
                f"http://{args[0]}/graphql", data=json.dumps({"query": "{ projects { edges { node { traceCount } } } }"}).encode(),
                headers={"content-type": "application/json"},
            )
            with OPENER.open(request, timeout=10) as r:
                edges = json.load(r)["data"]["projects"]["edges"]
            return sum(e["node"]["traceCount"] for e in edges)
        eventually(lambda: traces() == int(args[1]), f"Phoenix holding {args[1]} traces", seconds=120)
    elif what == "stats":
        records = sum(int(line.split()[0]) for line in args[0].splitlines() if not line.endswith("(total)"))
        assert records == int(args[1]), f"{records} records: {args[0]}"
    else:
        raise SystemExit(__doc__)
    print("ok", what)


if __name__ == "__main__":
    main(*sys.argv[1:])
