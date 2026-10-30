//go:build arm64 && cgo && !nosimd

package simd

/*
#cgo CXXFLAGS: -O3 -march=armv8-a+simd -std=c++11
#cgo !darwin LDFLAGS: -lm
#include "neon_simd_arm64.h"
*/
import "C"
import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"unsafe"
)

// ARM64 NEON-optimized implementations using native C++ NEON intrinsics.
// This provides true NEON SIMD acceleration for Apple Silicon and ARM64 servers.

func dotProduct(a, b []float32) float32 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	return float32(C.neon_dot_product(
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.int(len(a)),
	))
}

func cosineSimilarity(a, b []float32) float32 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	result := float32(C.neon_cosine_similarity(
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.int(len(a)),
	))
	// Handle NaN (zero vectors)
	if math.IsNaN(float64(result)) {
		return 0
	}
	return result
}

func euclideanDistance(a, b []float32) float32 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	return float32(C.neon_distance(
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.int(len(a)),
	))
}

func norm(v []float32) float32 {
	if len(v) == 0 {
		return 0
	}
	return float32(C.neon_norm(
		(*C.float)(unsafe.Pointer(&v[0])),
		C.int(len(v)),
	))
}

func normalizeInPlace(v []float32) {
	if len(v) == 0 {
		return
	}
	C.neon_normalize_inplace(
		(*C.float)(unsafe.Pointer(&v[0])),
		C.int(len(v)),
	)
}

func runtimeInfo() RuntimeInfo {
	return RuntimeInfo{
		Implementation: ImplNEON,
		Features:       []string{"NEON", "ARMv8-A"},
		Accelerated:    true,
	}
}
