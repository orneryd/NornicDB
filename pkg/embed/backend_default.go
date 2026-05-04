//go:build !cuda && !metal && !vulkan

// Package embed — build-tag matrix for the local GGUF backend label
// (Plan 04-05 D-06 / D-06a).
//
// `localGGUFBackend` is the value LocalGGUFEmbedder.Backend() returns. The
// matrix mirrors Plan 04-01's pkg/observability/build_*.go pattern but
// lives inside pkg/embed so the leaf-package boundary stays intact —
// pkg/observability never imports pkg/embed (D-01a / RESEARCH §1).
//
// Default file: amd64-cpu Docker variant + plain `go build` (RISK-5
// prevention from Plan 04-01: avoid undeclared-var failure).
package embed

const localGGUFBackend = "cpu"
