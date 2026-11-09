//go:build cgo && !nosimd && (amd64 || arm64)

package simd

/*
#cgo CXXFLAGS: -O3 -std=c++11
#include <stdint.h>
#include <stddef.h>
int64_t nornic_int8_dot(const int8_t* a, const int8_t* b, size_t n);
*/
import "C"
import "unsafe"

func dotProductInt8(a, b []byte) int64 {
	return int64(C.nornic_int8_dot((*C.int8_t)(unsafe.Pointer(&a[0])), (*C.int8_t)(unsafe.Pointer(&b[0])), C.size_t(len(a))))
}
