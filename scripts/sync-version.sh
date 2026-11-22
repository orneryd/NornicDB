#!/usr/bin/env sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
VERSION_FILE="$ROOT_DIR/pkg/buildinfo/VERSION"
README_FILE="$ROOT_DIR/README.md"

override_version=$(printf '%s' "${VERSION:-}" | tr -d '[:space:]')
version=""

if [ -n "$override_version" ] && [ "$override_version" != "latest" ]; then
	version=${override_version#v}
fi

if [ -z "$version" ] && [ -f "$VERSION_FILE" ]; then
	version=$(tr -d '[:space:]' < "$VERSION_FILE")
fi

if [ -z "$version" ] && command -v git >/dev/null 2>&1; then
	latest_tag=$(git -C "$ROOT_DIR" tag --sort=-version:refname | head -n 1 || true)
	version=${latest_tag#v}
fi

if [ -z "$version" ]; then
	echo "sync-version: unable to determine version from $VERSION_FILE or git tags" >&2
	exit 1
fi

tmp_version_file="$VERSION_FILE.tmp"
printf '%s\n' "$version" > "$tmp_version_file"
if ! cmp -s "$tmp_version_file" "$VERSION_FILE" 2>/dev/null; then
	mv "$tmp_version_file" "$VERSION_FILE"
	echo "sync-version: updated pkg/buildinfo/VERSION to $version"
else
	rm -f "$tmp_version_file"
fi

VERSION="$version" perl -0pi -e 's/version-\d+\.\d+\.\d+-success/"version-$ENV{VERSION}-success"/ge; s/alt="Version \d+\.\d+\.\d+"/qq{alt="Version $ENV{VERSION}"}/ge' "$README_FILE"
echo "sync-version: README badge set to $version"
