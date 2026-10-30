// Package libm provides transcendental math functions whose results match
// Neo4j's correctly rounded Java Math functions more closely than Go's
// standard library math package.
//
// The exponential, logarithmic and power functions (Exp, Exp2, Log, Log2,
// Pow) are ported from musl's math library, which in turn carries the Arm
// optimized-routines implementations (Copyright (c) 2018, Arm Limited,
// SPDX-License-Identifier: MIT). These are correctly rounded except in rare
// documented cases, whereas Go's math package (derived from fdlibm) can be
// up to 1 ulp away for inputs such as 3 ^ 2.5.
//
// The hyperbolic functions (Sinh, Cosh, Tanh, Asinh, Acosh, Atanh) are ported
// from musl's wrappers over its correctly rounded Exp and Log.
//
// Sin, Cos, Tan, Asin, Acos, Atan, Atan2 and Log10 are fdlibm-derived in both
// musl and Go's math package, so they are thin aliases to the corresponding
// math functions: musl's implementations are the Sun Microsystems fdlibm
// sources, which is the same origin as Go's math package.
//
// See NOTICES.md for the full third-party licence texts.
package libm
