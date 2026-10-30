// ARM64 NEON SIMD C interface

#ifndef NEON_SIMD_H
#define NEON_SIMD_H

#ifdef __cplusplus
extern "C" {
#endif

float neon_dot_product(const float* a, const float* b, int len);
float neon_norm(const float* v, int len);
float neon_distance(const float* a, const float* b, int len);
float neon_cosine_similarity(const float* a, const float* b, int len);
void neon_normalize_inplace(float* v, int len);

#ifdef __cplusplus
}
#endif

#endif // NEON_SIMD_H

