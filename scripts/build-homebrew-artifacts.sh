#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-"$ROOT_DIR/dist/homebrew"}"
VERSION_INPUT="${VERSION:-${GITHUB_REF_NAME:-}}"
BUILD_TAGS="${HOMEBREW_BUILD_TAGS:-localllm}"
TARGETS="${HOMEBREW_TARGETS:-darwin/arm64 darwin/amd64}"
SKIP_UI="${HOMEBREW_SKIP_UI:-0}"
CODESIGN_IDENTITY="${MACOS_CODESIGN_IDENTITY:-}"

if [[ -z "$VERSION_INPUT" && -f "$ROOT_DIR/pkg/buildinfo/VERSION" ]]; then
  VERSION_INPUT="$(tr -d '[:space:]' < "$ROOT_DIR/pkg/buildinfo/VERSION")"
fi
VERSION_INPUT="${VERSION_INPUT#v}"
if [[ -z "$VERSION_INPUT" ]]; then
  echo "Unable to determine release version. Set VERSION or create pkg/buildinfo/VERSION." >&2
  exit 2
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "Homebrew artifacts must be built on macOS because the release binaries use CGO/llama.cpp." >&2
  exit 2
fi

cd "$ROOT_DIR"
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/nornicdb-darwin-*.tar.gz "$OUT_DIR"/SHA256SUMS

if [[ "$SKIP_UI" != "1" ]]; then
  make build-ui
fi

build_commit="$(git rev-parse --short HEAD 2>/dev/null || echo dev)"
build_time="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
ldflags="-X github.com/orneryd/nornicdb/pkg/buildinfo.Commit=${build_commit} -X github.com/orneryd/nornicdb/pkg/buildinfo.BuildTime=${build_time}"

for target in $TARGETS; do
  goos="${target%/*}"
  goarch="${target#*/}"
  if [[ "$goos" != "darwin" ]]; then
    echo "Unsupported Homebrew target: $target" >&2
    exit 2
  fi
  case "$goarch" in
    arm64) clang_arch="arm64" ;;
    amd64) clang_arch="x86_64" ;;
    *) echo "Unsupported Homebrew architecture: $goarch" >&2; exit 2 ;;
  esac

  if [[ "$BUILD_TAGS" == *localllm* && ! -f "$ROOT_DIR/lib/llama/libllama_darwin_${goarch}.a" ]]; then
    cat >&2 <<EOF
Missing lib/llama/libllama_darwin_${goarch}.a.

Build or provide the llama.cpp static library for this architecture before
publishing the Homebrew artifact, or set HOMEBREW_BUILD_TAGS=nolocalllm for a
BYOM binary without local GGUF support.
EOF
    exit 1
  fi

  work_dir="$(mktemp -d "$ROOT_DIR/dist/homebrew-${goarch}.XXXXXX")"
  trap 'rm -rf "$work_dir"' EXIT
  echo "Building nornicdb for darwin/${goarch}..."
  CGO_ENABLED=1 \
    GOOS=darwin \
    GOARCH="$goarch" \
    CC="clang -arch ${clang_arch}" \
    go build -trimpath -ldflags "$ldflags" -tags "$BUILD_TAGS" -o "$work_dir/nornicdb" ./cmd/nornicdb

  if [[ -n "$CODESIGN_IDENTITY" && "$CODESIGN_IDENTITY" != "-" ]]; then
    echo "Codesigning darwin/${goarch} binary..."
    codesign --force --timestamp --options runtime --sign "$CODESIGN_IDENTITY" "$work_dir/nornicdb"
  fi

  chmod 0755 "$work_dir/nornicdb"
  archive="nornicdb-darwin-${goarch}.tar.gz"
  tar -C "$work_dir" -czf "$OUT_DIR/$archive" nornicdb
  rm -rf "$work_dir"
  trap - EXIT
done

(
  cd "$OUT_DIR"
  shasum -a 256 nornicdb-darwin-*.tar.gz | sort -k2 > SHA256SUMS
)

echo "Homebrew artifacts written to $OUT_DIR"
cat "$OUT_DIR/SHA256SUMS"
