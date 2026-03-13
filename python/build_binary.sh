#!/usr/bin/env bash
set -euo pipefail

# Build pulumi-resource-gcpx binary and copy into the Python package
# bin/ directory so setuptools includes it in the wheel.
#
# Set CARGO_BUILD_TARGET to cross-compile (e.g. x86_64-apple-darwin).
#
# Copyright: Lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$SCRIPT_DIR/python/pulumi_resource_gcpx/bin"

mkdir -p "$BIN_DIR"

TARGET_FLAG=""
RELEASE_DIR="$WORKSPACE_ROOT/target/release"
if [[ -n "${CARGO_BUILD_TARGET:-}" ]]; then
    TARGET_FLAG="--target $CARGO_BUILD_TARGET"
    RELEASE_DIR="$WORKSPACE_ROOT/target/$CARGO_BUILD_TARGET/release"
fi

echo "Building pulumi-resource-gcpx..."
cargo build --release --manifest-path "$WORKSPACE_ROOT/Cargo.toml" \
    $TARGET_FLAG

# Determine binary suffix
EXE_SUFFIX=""
if [[ "${CARGO_BUILD_TARGET:-}" == *windows* ]] || [[ "$(uname -s)" == *MINGW* ]] || [[ "$(uname -s)" == *MSYS* ]] || [[ "${OS:-}" == "Windows_NT" ]]; then
    EXE_SUFFIX=".exe"
fi

echo "Copying binary to $BIN_DIR..."
cp "$RELEASE_DIR/pulumi-resource-gcpx${EXE_SUFFIX}" "$BIN_DIR/"

echo "Done. Binary:"
ls -lh "$BIN_DIR"/pulumi-resource-gcpx*
