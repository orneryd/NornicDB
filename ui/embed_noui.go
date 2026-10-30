// Package ui provides the embedded NornicDB web UI assets.
// This file is used when building with -tags noui to exclude the UI assets.
//
//go:build noui

package ui

import "embed"

// Assets is empty for headless builds (no UI bundled).
// When built with -tags noui, no UI files are embedded, resulting in
// a smaller binary suitable for headless/embedded deployments.
var Assets embed.FS
