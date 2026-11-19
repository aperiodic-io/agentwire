#!/usr/bin/env bash
# Line coverage of the library (codec, store, client, server) by the unit, property and integration
# tests: >= 95%. The binary (src/main.rs) runs as separate processes under the acceptance scenarios,
# which llvm-cov does not see, so it is not gated here. A gate that finds no instrumented lines
# fails: a moved path must not silently disable it.
set -euo pipefail
report="${1:-lcov.info}"
awk -F: -v min=95 '
  /^SF:/ { mine = ($2 ~ /\/src\// && $2 !~ /\/src\/main\.rs$/) }
  mine && /^LF:/ { lf += $2 }
  mine && /^LH:/ { lh += $2 }
  END {
    if (lf == 0) { print "no instrumented lines under src/"; exit 1 }
    pct = 100 * lh / lf
    printf "library line coverage: %.2f%% (%d/%d), gate %s%%\n", pct, lh, lf, min
    if (pct < min) { print "below the gate"; exit 1 }
  }' "$report"
