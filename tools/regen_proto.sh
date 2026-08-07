#!/usr/bin/env bash
# Regenerate the checked-in Python protobuf modules (liteserver_pb2.py +
# liteserver_pb2_grpc.py) from src/proto/liteserver.proto using a PINNED old
# grpcio-tools toolchain.
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
#   The _pb2_grpc stub from 1.62.3 likewise carries no runtime-version guard;
#   it only needs `grpcio` (floor >=1.62) at import time.
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
echo "[regen_proto] output : $OUT_DIR/liteserver_pb2.py + liteserver_pb2_grpc.py"
echo "[regen_proto] tool   : grpcio-tools==$GRPCIO_TOOLS_VERSION (protoc 25.x — no runtime_version guard)"

uv run --no-project --python 3.12 --with "grpcio-tools==$GRPCIO_TOOLS_VERSION" \
  python -m grpc_tools.protoc \
    -I "$REPO_ROOT/src/proto" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO"

# protoc emits `import liteserver_pb2 as liteserver__pb2` (sys.path style),
# which breaks when the stub is imported through the lite_server.proto
# package. Rewrite to a package-relative import.
sed -i.bak \
  's/^import liteserver_pb2 as liteserver__pb2$/from . import liteserver_pb2 as liteserver__pb2/' \
  "$OUT_DIR/liteserver_pb2_grpc.py"
rm -f "$OUT_DIR/liteserver_pb2_grpc.py.bak"

# Built-in guard: the regenerated files must NOT carry the version-check guard.
for f in liteserver_pb2.py liteserver_pb2_grpc.py; do
  if grep -q "runtime_version" "$OUT_DIR/$f"; then
    echo "[regen_proto] ERROR: regenerated $f contains a 'runtime_version' guard." >&2
    echo "[regen_proto]         grpcio-tools==$GRPCIO_TOOLS_VERSION is too new; use <= 1.62.x." >&2
    exit 1
  fi
done

echo "[regen_proto] OK: liteserver_pb2.py + liteserver_pb2_grpc.py regenerated (no runtime_version guard)"
