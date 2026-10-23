#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LLAMA_VERSION="$(sed -n 's/^LLAMA_VERSION ?= //p' Makefile | head -n1)"
if [[ -z "${LLAMA_VERSION}" ]]; then
  echo "Failed to determine LLAMA_VERSION from Makefile" >&2
  exit 1
fi

LLAMA_DIGEST="$(tr -d '\r\n' < lib/llama/CPU_IMAGE_DIGEST)"
LLAMA_IMAGE="timothyswt/llama-cpu-libs:${LLAMA_VERSION}@${LLAMA_DIGEST}"
docker pull "${LLAMA_IMAGE}"

CID="$(docker create --platform linux/amd64 "${LLAMA_IMAGE}")"
cleanup() {
  docker rm -f "${CID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p lib/llama
docker cp "${CID}:/output/lib/libllama_combined.a" "lib/llama/libllama_linux_amd64.a"
docker cp "${CID}:/output/include/." "lib/llama/"
