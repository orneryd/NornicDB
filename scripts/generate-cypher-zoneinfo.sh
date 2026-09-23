#!/usr/bin/env bash
set -euo pipefail

readonly TZDATA_VERSION="2026d"
readonly TZDATA_SHA256="0cb2aa8e333c3dc049badc42a0c61f21987b8cd44e107fa900bad764aacc7767"
readonly TZDATA_URL="https://data.iana.org/time-zones/releases/tzdata${TZDATA_VERSION}.tar.gz"
readonly REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly OUTPUT_PATH="${REPOSITORY_ROOT}/pkg/cypher/temporal_zoneinfo.zip"

work_directory="$(mktemp -d "${TMPDIR:-/tmp}/nornicdb-zoneinfo.XXXXXX")"
trap 'rm -rf "${work_directory}"' EXIT

curl -fsSLo "${work_directory}/tzdata.tar.gz" "${TZDATA_URL}"
if command -v sha256sum >/dev/null 2>&1; then
	actual_sha256="$(sha256sum "${work_directory}/tzdata.tar.gz" | awk '{print $1}')"
else
	actual_sha256="$(shasum -a 256 "${work_directory}/tzdata.tar.gz" | awk '{print $1}')"
fi
if [[ "${actual_sha256}" != "${TZDATA_SHA256}" ]]; then
	echo "tzdata checksum mismatch: got ${actual_sha256}, want ${TZDATA_SHA256}" >&2
	exit 1
fi

tar -xzf "${work_directory}/tzdata.tar.gz" -C "${work_directory}"
mkdir "${work_directory}/compiled"
zic -b slim -d "${work_directory}/compiled" \
	"${work_directory}/africa" \
	"${work_directory}/antarctica" \
	"${work_directory}/asia" \
	"${work_directory}/australasia" \
	"${work_directory}/europe" \
	"${work_directory}/northamerica" \
	"${work_directory}/southamerica" \
	"${work_directory}/etcetera" \
	"${work_directory}/factory" \
	"${work_directory}/backward"

# Java's tzdb is built from the main IANA data and backward aliases without
# backzone. Fixed timestamps and sorted paths keep the checked-in archive
# reproducible across filesystems.
find "${work_directory}/compiled" -exec touch -t 200001010000 {} +
(
	cd "${work_directory}/compiled"
	find . -type f | LC_ALL=C sort | zip -X -q "${work_directory}/zoneinfo.zip" -@
)
mv "${work_directory}/zoneinfo.zip" "${OUTPUT_PATH}"

echo "wrote ${OUTPUT_PATH} from IANA tzdata ${TZDATA_VERSION}"
