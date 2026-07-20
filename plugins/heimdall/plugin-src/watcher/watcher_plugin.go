//go:build ignore

// Watcher Plugin - Core Heimdall guardian plugin for NornicDB
// This is the main watcher plugin that provides SLM management actions.
// Build with: go build -buildmode=plugin -o watcher.so watcher_plugin.go
// Remote-provider-only build: go build -tags nolocalllm -buildmode=plugin -o watcher.so watcher_plugin.go
package main

import (
	"github.com/orneryd/nornicdb/pkg/heimdall"
	watcherpkg "github.com/orneryd/nornicdb/plugins/heimdall"
)

// Plugin is the exported symbol that NornicDB will load
// This must be exported as "Plugin" and implement heimdall.HeimdallPlugin
var Plugin heimdall.HeimdallPlugin = watcherpkg.Plugin
