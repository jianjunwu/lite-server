#!/usr/bin/env bash
# Pack lite-ui as a self-contained tarball:
#   dist-release/lite-ui-<version>.tgz
# Layout: server-dist/ (compiled BFF), web-dist/ (built SPA), package.json
# (production deps manifest), instances.yaml (sample), README.md.
# Run after `pnpm -r build`.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=$(node -p "require('./server/package.json').version")
STAGE="dist-release/lite-ui-${VERSION}"
OUT="dist-release/lite-ui-${VERSION}.tgz"

rm -rf "$STAGE"
mkdir -p "$STAGE"

cp -r server/dist "$STAGE/server-dist"
cp -r web/dist "$STAGE/web-dist"
cp server/instances.example.yaml "$STAGE/instances.yaml"
cp server/package.json "$STAGE/package.json"
cp README.md "$STAGE/README.md"

mkdir -p dist-release
rm -f "$OUT"
tar -czf "$OUT" -C dist-release "lite-ui-${VERSION}"
rm -rf "$STAGE"
echo "packed: $OUT"
