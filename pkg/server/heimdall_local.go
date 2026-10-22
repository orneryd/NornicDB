package server

// Register the optional local inference provider in the host, not the plugin API.
// The adapter has no native implementation when built with nolocalllm.
import _ "github.com/orneryd/nornicdb/pkg/heimdall/local"
