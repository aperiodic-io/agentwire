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
rustup toolchain install --profile minimal "$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)"
rustup target add "$target"
cargo build --release --locked --features proxy --target "$target"
name="deltapack-$version-$target"
mkdir -p "$name"
cp "target/$target/release/deltapack" LICENSE README.md "$name/"
tar czf "$name.tar.gz" "$name"
shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256"
