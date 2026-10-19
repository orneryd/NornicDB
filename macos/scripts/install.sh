#!/bin/bash
# Direct developer install uses the same upgrade/configuration owner as .pkg.
set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
    echo "This installer requires macOS." >&2
    exit 1
fi
if [[ $EUID -eq 0 ]]; then
    echo "Run as your login user; sudo is requested only for installation." >&2
    exit 1
fi
PROJECT_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
if [[ ! -x "$PROJECT_ROOT/bin/nornicdb" || ! -x "$PROJECT_ROOT/macos/build/NornicDB.app/Contents/MacOS/NornicDB" ]]; then
    echo "Build the server and menu bar app first: make build macos-menubar" >&2
    exit 1
fi

sudo env USER="$USER" /bin/bash "$PROJECT_ROOT/macos/scripts/preinstall"
sudo mkdir -p /usr/local/bin /usr/local/share/nornicdb
sudo install -m 755 "$PROJECT_ROOT/bin/nornicdb" /usr/local/bin/nornicdb
sudo install -m 644 "$PROJECT_ROOT/macos/default-config.yaml" /usr/local/share/nornicdb/default-config.yaml
sudo ditto "$PROJECT_ROOT/macos/build/NornicDB.app" /Applications/NornicDB.app
sudo env USER="$USER" /bin/bash "$PROJECT_ROOT/macos/scripts/postinstall"
