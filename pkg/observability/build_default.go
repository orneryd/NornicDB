//go:build !cuda && !metal && !vulkan && !noembed

package observability

// backend identifies the Docker variant the binary was built for. Used as a
// const-label value on `nornicdb_build_info` (CONTEXT D-13a / D-13b).
//
// This file fixes RESEARCH RISK-5: without a default-tag file, undecorated
// builds (the amd64-cpu Docker variant + plain `go build`) failed with
// `var backend undeclared`. Mirrors `pkg/embed/local_gguf_stub.go`'s
// `!localllm` precedent.
var backend = "cpu"
