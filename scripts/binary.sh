#!/usr/bin/env bash
# The release binary for one target, packed as deltapack-<version>-<target>.tar.gz (the binary,
# LICENSE and README.md) with its SHA-256 beside it. Linux targets are musl, linked statically, so
# one binary runs on any distribution.
set -euo pipefail
target="$1" version="$2"
case "$target" in
  *-linux-musl)
    command -v musl-gcc > /dev/null || { sudo apt-get update -q && sudo apt-get install -y -q musl-tools; }
    export CC_${target//-/_}=musl-gcc ;;
esac
rustup toolchain install # the one rust-toolchain.toml names
rustup target add "$target"
cargo build --release --locked --features proxy --target "$target"
name="deltapack-$version-$target"
mkdir -p "$name"
cp "${CARGO_TARGET_DIR:-target}/$target/release/deltapack" LICENSE README.md "$name/"
COPYFILE_DISABLE=1 tar czf "$name.tar.gz" "$name" # no macOS ._ entries
shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256"
