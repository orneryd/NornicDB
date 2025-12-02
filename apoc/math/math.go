// Package math provides APOC mathematical functions.
//
// This package implements all apoc.math.* functions for mathematical
// operations in Cypher queries.
package math

import (
	"math"
	"math/rand"
)

// MaxLong returns the maximum of multiple integers.
//
// Example:
//   apoc.math.maxLong(5, 2, 8, 1, 9) => 9
func MaxLong(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// MinLong returns the minimum of multiple integers.
//
// Example:
//   apoc.math.minLong(5, 2, 8, 1, 9) => 1
func MinLong(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

// MaxDouble returns the maximum of multiple floats.
//
// Example:
//   apoc.math.maxDouble(5.5, 2.3, 8.1, 1.9) => 8.1
func MaxDouble(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// MinDouble returns the minimum of multiple floats.
//
// Example:
//   apoc.math.minDouble(5.5, 2.3, 8.1, 1.9) => 1.9
func MinDouble(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

// Round rounds a number to a given precision.
//
// Example:
//   apoc.math.round(3.14159, 2) => 3.14
//   apoc.math.round(1234.5, -2) => 1200.0
func Round(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}

// Ceil rounds up to the nearest integer.
//
// Example:
//   apoc.math.ceil(3.14) => 4.0
func Ceil(value float64) float64 {
	return math.Ceil(value)
}

// Floor rounds down to the nearest integer.
//
// Example:
//   apoc.math.floor(3.14) => 3.0
func Floor(value float64) float64 {
	return math.Floor(value)
}

// Abs returns the absolute value.
//
// Example:
//   apoc.math.abs(-5.5) => 5.5
func Abs(value float64) float64 {
	return math.Abs(value)
}

// Pow raises a number to a power.
//
// Example:
//   apoc.math.pow(2, 8) => 256.0
func Pow(base, exponent float64) float64 {
	return math.Pow(base, exponent)
}

// Sqrt returns the square root.
//
// Example:
//   apoc.math.sqrt(16) => 4.0
func Sqrt(value float64) float64 {
	return math.Sqrt(value)
}

// Log returns the natural logarithm.
//
// Example:
//   apoc.math.log(math.E) => 1.0
func Log(value float64) float64 {
	return math.Log(value)
}

// Log10 returns the base-10 logarithm.
//
// Example:
//   apoc.math.log10(100) => 2.0
func Log10(value float64) float64 {
	return math.Log10(value)
}

// Exp returns e raised to the power of x.
//
// Example:
//   apoc.math.exp(1) => 2.718281828...
func Exp(value float64) float64 {
	return math.Exp(value)
}

// Sin returns the sine of an angle (in radians).
//
// Example:
//   apoc.math.sin(math.Pi / 2) => 1.0
func Sin(angle float64) float64 {
	return math.Sin(angle)
}

// Cos returns the cosine of an angle (in radians).
//
// Example:
//   apoc.math.cos(0) => 1.0
func Cos(angle float64) float64 {
	return math.Cos(angle)
}

// Tan returns the tangent of an angle (in radians).
//
// Example:
//   apoc.math.tan(math.Pi / 4) => 1.0
func Tan(angle float64) float64 {
	return math.Tan(angle)
}

// Asin returns the arcsine (in radians).
//
// Example:
//   apoc.math.asin(1) => math.Pi / 2
func Asin(value float64) float64 {
	return math.Asin(value)
}

// Acos returns the arccosine (in radians).
//
// Example:
//   apoc.math.acos(1) => 0.0
func Acos(value float64) float64 {
	return math.Acos(value)
}

// Atan returns the arctangent (in radians).
//
// Example:
//   apoc.math.atan(1) => math.Pi / 4
func Atan(value float64) float64 {
	return math.Atan(value)
}

// Atan2 returns the arctangent of y/x (in radians).
//
// Example:
//   apoc.math.atan2(1, 1) => math.Pi / 4
func Atan2(y, x float64) float64 {
	return math.Atan2(y, x)
}

// Sinh returns the hyperbolic sine.
//
// Example:
//   apoc.math.sinh(0) => 0.0
func Sinh(value float64) float64 {
	return math.Sinh(value)
}

// Cosh returns the hyperbolic cosine.
//
// Example:
//   apoc.math.cosh(0) => 1.0
func Cosh(value float64) float64 {
	return math.Cosh(value)
}

// Tanh returns the hyperbolic tangent.
//
// Example:
//   apoc.math.tanh(0) => 0.0
func Tanh(value float64) float64 {
	return math.Tanh(value)
}

// Sigmoid returns the sigmoid function value.
//
// Example:
//   apoc.math.sigmoid(0) => 0.5
func Sigmoid(value float64) float64 {
	return 1.0 / (1.0 + math.Exp(-value))
}

// Logit returns the logit function (inverse of sigmoid).
//
// Example:
//   apoc.math.logit(0.5) => 0.0
func Logit(value float64) float64 {
	if value <= 0 || value >= 1 {
		return math.NaN()
	}
	return math.Log(value / (1.0 - value))
}

// Clamp restricts a value to a range.
//
// Example:
//   apoc.math.clamp(15, 0, 10) => 10
//   apoc.math.clamp(-5, 0, 10) => 0
//   apoc.math.clamp(5, 0, 10) => 5
func Clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// Lerp performs linear interpolation between two values.
//
// Example:
//   apoc.math.lerp(0, 10, 0.5) => 5.0
//   apoc.math.lerp(0, 10, 0.25) => 2.5
func Lerp(start, end, t float64) float64 {
	return start + t*(end-start)
}

// Normalize normalizes a value from one range to another.
//
// Example:
//   apoc.math.normalize(5, 0, 10, 0, 1) => 0.5
func Normalize(value, oldMin, oldMax, newMin, newMax float64) float64 {
	if oldMax == oldMin {
		return newMin
	}
	t := (value - oldMin) / (oldMax - oldMin)
	return newMin + t*(newMax-newMin)
}

// Gcd returns the greatest common divisor.
//
// Example:
//   apoc.math.gcd(48, 18) => 6
func Gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// Lcm returns the least common multiple.
//
// Example:
//   apoc.math.lcm(12, 18) => 36
func Lcm(a, b int64) int64 {
	return (a * b) / Gcd(a, b)
}

// Factorial returns the factorial of n.
//
// Example:
//   apoc.math.factorial(5) => 120
func Factorial(n int64) int64 {
	if n <= 1 {
		return 1
	}
	result := int64(1)
	for i := int64(2); i <= n; i++ {
		result *= i
	}
	return result
}

// Fibonacci returns the nth Fibonacci number.
//
// Example:
//   apoc.math.fibonacci(10) => 55
func Fibonacci(n int64) int64 {
	if n <= 1 {
		return n
	}
	a, b := int64(0), int64(1)
	for i := int64(2); i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

// IsPrime checks if a number is prime.
//
// Example:
//   apoc.math.isPrime(17) => true
//   apoc.math.isPrime(18) => false
func IsPrime(n int64) bool {
	if n <= 1 {
		return false
	}
	if n <= 3 {
		return true
	}
	if n%2 == 0 || n%3 == 0 {
		return false
	}
	
	i := int64(5)
	for i*i <= n {
		if n%i == 0 || n%(i+2) == 0 {
			return false
		}
		i += 6
	}
	return true
}

// NextPrime returns the next prime number after n.
//
// Example:
//   apoc.math.nextPrime(10) => 11
func NextPrime(n int64) int64 {
	candidate := n + 1
	for !IsPrime(candidate) {
		candidate++
	}
	return candidate
}

// Random returns a random float between 0 and 1.
//
// Example:
//   apoc.math.random() => 0.7234... (random)
func Random() float64 {
	return rand.Float64()
}

// RandomInt returns a random integer in a range [min, max].
//
// Example:
//   apoc.math.randomInt(1, 10) => 7 (random)
func RandomInt(min, max int64) int64 {
	if min >= max {
		return min
	}
	return min + rand.Int63n(max-min+1)
}

// Percentile calculates the nth percentile of a list.
//
// Example:
//   apoc.math.percentile([1,2,3,4,5,6,7,8,9,10], 50) => 5.5
func Percentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	// Sort values
	sorted := make([]float64, len(values))
	copy(sorted, values)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	
	// Calculate percentile
	index := (percentile / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	
	if lower == upper {
		return sorted[lower]
	}
	
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// Median returns the median value of a list.
//
// Example:
//   apoc.math.median([1,2,3,4,5]) => 3.0
func Median(values []float64) float64 {
	return Percentile(values, 50)
}

// Mean returns the arithmetic mean of a list.
//
// Example:
//   apoc.math.mean([1,2,3,4,5]) => 3.0
func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// StdDev returns the standard deviation of a list.
//
// Example:
//   apoc.math.stdDev([2,4,4,4,5,5,7,9]) => 2.0
func StdDev(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	mean := Mean(values)
	sumSquares := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}
	
	return math.Sqrt(sumSquares / float64(len(values)))
}

// Variance returns the variance of a list.
//
// Example:
//   apoc.math.variance([2,4,4,4,5,5,7,9]) => 4.0
func Variance(values []float64) float64 {
	stdDev := StdDev(values)
	return stdDev * stdDev
}

// Mode returns the most frequent value in a list.
//
// Example:
//   apoc.math.mode([1,2,2,3,3,3,4]) => 3.0
func Mode(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	frequencies := make(map[float64]int)
	for _, v := range values {
		frequencies[v]++
	}
	
	maxFreq := 0
	mode := values[0]
	for value, freq := range frequencies {
		if freq > maxFreq {
			maxFreq = freq
			mode = value
		}
	}
	
	return mode
}

// Range returns a list of numbers from start to end.
//
// Example:
//   apoc.math.range(1, 5) => [1,2,3,4,5]
func Range(start, end, step int64) []int64 {
	if step == 0 {
		step = 1
	}
	
	result := make([]int64, 0)
	if step > 0 {
		for i := start; i <= end; i += step {
			result = append(result, i)
		}
	} else {
		for i := start; i >= end; i += step {
			result = append(result, i)
		}
	}
	
	return result
}

// Sum returns the sum of a list of numbers.
//
// Example:
//   apoc.math.sum([1,2,3,4,5]) => 15.0
func Sum(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

// Product returns the product of a list of numbers.
//
// Example:
//   apoc.math.product([2,3,4]) => 24.0
func Product(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	product := 1.0
	for _, v := range values {
		product *= v
	}
	return product
}
