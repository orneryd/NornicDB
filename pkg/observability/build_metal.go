//go:build metal

package observability

// backend identifies the Docker variant the binary was built for. Used as a
// const-label value on `nornicdb_build_info` (CONTEXT D-13a / D-13b).
// One per build-tag file; build_default.go covers the undecorated case.
var backend = "metal"
