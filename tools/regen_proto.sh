#!/usr/bin/env bash
# Regenerate the checked-in Python protobuf module (liteserver_pb2.py) from
# src/proto/liteserver.proto using a PINNED old grpcio-tools toolchain.
#
# Why pinned old grpcio-tools:
#   protoc >= 26 (grpcio-tools >= 1.63) emits a
#   `_runtime_version.ValidateProtobufRuntimeVersion(...)` guard in the
#   generated code that BINDS the gencode to a specific protobuf runtime
#   version. That breaks installs whose runtime is older than the gencode —
#   e.g. our declared floor `protobuf>=5.0`, where the runtime may lack the
#   `runtime_version` module entirely (ImportError) or be a patch older than
#   the gencode (VersionError). This is the 0.7.2 "worker crashed" root cause.
#
#   grpcio-tools 1.62.3 bundles protoc 25.x, which generates the classic
#   `_builder`-based code with NO version guard. That gencode is
#   FORWARD-compatible with protobuf >= 4.25 (our >=5.0 floor and beyond):
#   old gencode + new runtime is the supported, stable direction.
#
# The codegen runs in an ISOLATED environment (--no-project) so the codegen
# toolchain (which pulls protobuf 4.25) does NOT perturb the project's runtime
# protobuf (currently 7.x).
#
# Usage:   tools/regen_proto.sh
# Override the pinned version with:  GRPCIO_TOOLS_VERSION=1.62.3 tools/regen_proto.sh
# Requires: uv (https://docs.astral.sh/uv/). On Windows run via git-bash / WSL.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO="$REPO_ROOT/src/proto/liteserver.proto"
OUT_DIR="$REPO_ROOT/python/lite_server/proto"
GRPCIO_TOOLS_VERSION="${GRPCIO_TOOLS_VERSION:-1.62.3}"

if [[ ! -f "$PROTO" ]]; then
  echo "[regen_proto] ERROR: proto source not found at $PROTO" >&2
  exit 1
fi

echo "[regen_proto] source : $PROTO"
echo "[regen_proto] output : $OUT_DIR/liteserver_pb2.py"
echo "[regen_proto] tool   : grpcio-tools==$GRPCIO_TOOLS_VERSION (protoc 25.x — no runtime_version guard)"

uv run --no-project --python 3.12 --with "grpcio-tools==$GRPCIO_TOOLS_VERSION" \
  python -m grpc_tools.protoc \
    -I "$REPO_ROOT/src/proto" \
    --python_out="$OUT_DIR" \
    "$PROTO"

# Built-in guard: the regenerated file must NOT carry the version-check guard.
if grep -q "runtime_version" "$OUT_DIR/liteserver_pb2.py"; then
  echo "[regen_proto] ERROR: regenerated pb2 contains a 'runtime_version' guard." >&2
  echo "[regen_proto]         grpcio-tools==$GRPCIO_TOOLS_VERSION is too new; use <= 1.62.x." >&2
  exit 1
fi

echo "[regen_proto] OK: liteserver_pb2.py regenerated (no runtime_version guard)"
