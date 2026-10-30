//go:build cuda

package observability

// backend identifies the Docker variant the binary was built for. Used as a
// const-label value on `nornicdb_build_info` (CONTEXT D-13a / D-13b).
var backend = "cuda"
