//go:build !cgo || nosimd || (!amd64 && !arm64)

package simd

func dotProductInt8(a, b []byte) int64 { return dotProductInt8Reference(a, b) }
