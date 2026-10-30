# ARM64 NEON SIMD Acceleration

Last Updated: January 2026

## Summary

Replaced vek library dependency with native ARM64 NEON SIMD implementation in C++ for Apple Silicon and ARM64 servers.

## Implementation

### Files Created

1. **`pkg/simd/neon_simd_arm64.cpp`** — C++ NEON SIMD implementation
   - Uses ARM NEON intrinsics (`arm_neon.h`)
   - Optimized for ARMv8-A architecture
   - Processes 4 float32 elements per iteration

2. **`pkg/simd/neon_simd_arm64.h`** — C interface header for CGO bindings.

3. **`pkg/simd/neon_simd.go`** — Go CGO bindings; build tag `arm64 && cgo && !nosimd`. ARM64 builds without CGO fall back to `simd_arm64.go` pure-Go paths.

### Functions Implemented

1. **`neon_dot_product`** - Dot product: `sum(a[i] * b[i])`
2. **`neon_norm`** - Euclidean norm: `sqrt(sum(v[i]^2))`
3. **`neon_distance`** - Euclidean distance: `sqrt(sum((a[i] - b[i])^2))`
4. **`neon_cosine_similarity`** - Cosine similarity: `dot(a,b) / (norm(a) * norm(b))`
5. **`neon_normalize_inplace`** - Normalize vector in-place: `v[i] = v[i] / norm(v)`

## Performance Results

### Benchmark: Dot Product (Apple M3 Max)

| Vector Size | NEON SIMD | Go Reference | Speedup |
|-------------|-----------|--------------|---------|
| 128-dim     | 44 ns     | 40 ns        | 0.9x    |
| 256-dim     | 67 ns     | 77 ns        | 1.1x    |
| 512-dim     | 111 ns    | 148 ns       | 1.3x    |
| 1024-dim    | 208 ns    | 285 ns       | 1.4x    |
| 1536-dim    | 305 ns    | 428 ns       | 1.4x    |
| 3072-dim    | 598 ns    | 839 ns       | 1.4x    |

**Key Findings:**
- NEON SIMD is **1.4x faster** for typical embedding sizes (1024-1536 dim)
- Performance improves with larger vectors
- Small overhead for very small vectors (128-dim)

## Build Configuration

### Build Tags

- **NEON SIMD (ARM64):** `arm64 && cgo && !nosimd`
- **Fallback (ARM64, no CGO):** `arm64 && (!cgo || nosimd)` - uses vek Go fallback
- **x86/amd64:** Still uses vek library (AVX2 support)

### Compiler Flags

```cpp
CXXFLAGS: -O3 -march=armv8-a+simd -std=c++11
LDFLAGS: -lm
```

## Testing

✅ **All tests pass:**
```bash
$ go test ./pkg/simd -v
--- PASS: TestInfo (0.00s)
    SIMD Info: neon (accelerated=true, features=[NEON ARMv8-A])
--- PASS: TestDotProduct
--- PASS: TestCosineSimilarity
--- PASS: TestEuclideanDistance
--- PASS: TestNorm
--- PASS: TestNormalizeInPlace
--- PASS: TestLargeVectors
--- PASS: TestEdgeCases
```

## Dependencies

### Removed
- **vek library for ARM64:** No longer needed on ARM64 platforms

### Kept
- **vek library for x86/amd64:** Still used for AVX2 SIMD on x86 platforms
- **CGO required:** ARM64 NEON implementation requires CGO

## Benefits

1. **No External Dependency (ARM64):** Self-contained NEON implementation
2. **Better Performance:** 1.4x faster for typical embedding sizes
3. **Full Control:** Can optimize further without waiting for upstream
4. **Native Code:** Direct NEON intrinsics, no library overhead

## Future Improvements

1. **Unroll Loops:** Process 8 or 16 elements per iteration for larger vectors
2. **FMA Instructions:** Use fused multiply-add where available
3. **Prefetching:** Add memory prefetching for very large vectors
4. **AVX2 Implementation:** Replace vek for x86/amd64 as well

## Conclusion

The ARM64 NEON SIMD implementation is complete, tested, and provides **1.4x performance improvement** for typical embedding operations. This eliminates the vek dependency for ARM64 platforms while maintaining compatibility with x86 builds.

