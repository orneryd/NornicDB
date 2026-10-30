// Package ui provides the embedded NornicDB web UI assets.
// This file is used for normal builds that include the UI.
//
//go:build !noui

package ui

import "embed"

// Assets contains the built UI files from the dist directory.
// Build the UI with `npm run build` before compiling the Go binary.
//
// Use all:dist to embed the full built UI tree without requiring every
// subdirectory (e.g., dist/assets) to contain non-hidden files.
//
//go:embed all:dist
var Assets embed.FS
