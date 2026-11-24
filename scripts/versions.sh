#!/usr/bin/env bash
# Every package's version is the crate's (Cargo.toml), so one tag `vX.Y.Z` releases them all: the
# Python wheels (their version is their Cargo.toml's), the npm package, and the Collector exporter's
# requirement of the Go module it is built on. With an argument (a tag), the crate's must be it.
set -euo pipefail
v=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
check() { [ "$2" = "$v" ] || { echo "$1 is $2, the crate is $v"; exit 1; }; }
check python/Cargo.toml "$(sed -n 's/^version = "\(.*\)"/\1/p' python/Cargo.toml | head -1)"
check ts/package.json "$(sed -n 's/^  "version": "\(.*\)",/\1/p' ts/package.json)"
check "the exporter's require of the Go module" \
  "$(sed -n 's#^\s*github.com/aperiodic-io/deltapack/go v\(.*\)#\1#p' go/otelcol/deltapackexporter/go.mod)"
if [ $# -gt 0 ]; then
  [ "v$v" = "$1" ] || { echo "tag $1 is not the crate's version v$v"; exit 1; }
fi
echo "every package is $v"
