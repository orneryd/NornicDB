//go:build cgo && !nosimd && (amd64 || arm64)

#include <stdint.h>
#include <stddef.h>
#if defined(__aarch64__)
#include <arm_neon.h>
#endif

// Reduce bounded blocks into int64 so all signed int8 values, including -128,
// accumulate exactly even beyond the int32 range. ARMv8-A widening multiply
// works without the optional dot-product ISA extension.
extern "C" int64_t nornic_int8_dot(const int8_t* a, const int8_t* b, size_t n) {
    int64_t total = 0;
    size_t pos = 0;
    while (pos < n) {
        const size_t end = n - pos > 4096 ? pos + 4096 : n;
#if defined(__aarch64__)
        int32x4_t s0 = vdupq_n_s32(0), s1 = s0, s2 = s0, s3 = s0;
        for (; pos + 32 <= end; pos += 32) {
            int8x16_t a0 = vld1q_s8(a + pos), b0 = vld1q_s8(b + pos);
            int8x16_t a1 = vld1q_s8(a + pos + 16), b1 = vld1q_s8(b + pos + 16);
            s0 = vpadalq_s16(s0, vmull_s8(vget_low_s8(a0), vget_low_s8(b0)));
            s1 = vpadalq_s16(s1, vmull_s8(vget_high_s8(a0), vget_high_s8(b0)));
            s2 = vpadalq_s16(s2, vmull_s8(vget_low_s8(a1), vget_low_s8(b1)));
            s3 = vpadalq_s16(s3, vmull_s8(vget_high_s8(a1), vget_high_s8(b1)));
        }
        total += (int64_t)vaddvq_s32(s0) + vaddvq_s32(s1)
               + vaddvq_s32(s2) + vaddvq_s32(s3);
#else
        int32_t s0 = 0, s1 = 0, s2 = 0, s3 = 0;
        for (; pos + 4 <= end; pos += 4) {
            s0 += (int32_t)a[pos] * b[pos];
            s1 += (int32_t)a[pos+1] * b[pos+1];
            s2 += (int32_t)a[pos+2] * b[pos+2];
            s3 += (int32_t)a[pos+3] * b[pos+3];
        }
        total += (int64_t)s0 + s1 + s2 + s3;
#endif
        for (; pos < end; ++pos) total += (int32_t)a[pos] * b[pos];
    }
    return total;
}
