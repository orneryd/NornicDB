// Package number provides APOC number formatting and parsing functions.
//
// This package implements all apoc.number.* functions for number
// manipulation, formatting, and conversion.
package number

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Format formats a number with specified precision.
//
// Example:
//
//	apoc.number.format(1234.5678, '#,##0.00') => '1,234.57'
func Format(number float64, pattern string) string {
	// Simple formatting implementation
	if strings.Contains(pattern, ",") {
		return formatWithCommas(number, pattern)
	}

	// Extract decimal places from pattern
	decimalPlaces := 2
	if idx := strings.Index(pattern, "."); idx >= 0 {
		decimalPlaces = len(pattern) - idx - 1
	}

	return fmt.Sprintf("%."+strconv.Itoa(decimalPlaces)+"f", number)
}

// formatWithCommas formats a number with thousand separators.
func formatWithCommas(number float64, pattern string) string {
	// Split into integer and decimal parts
	intPart := int64(math.Abs(number))
	decPart := math.Abs(number) - float64(intPart)

	// Format integer part with commas
	intStr := strconv.FormatInt(intPart, 10)
	var result strings.Builder

	for i, digit := range intStr {
		if i > 0 && (len(intStr)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(digit)
	}

	// Add decimal part if needed
	if decPart > 0 || strings.Contains(pattern, ".") {
		decimalPlaces := 2
		if idx := strings.Index(pattern, "."); idx >= 0 {
			decimalPlaces = len(pattern) - idx - 1
		}
		result.WriteString(fmt.Sprintf(".%0*d", decimalPlaces, int(decPart*math.Pow(10, float64(decimalPlaces)))))
	}

	if number < 0 {
		return "-" + result.String()
	}
	return result.String()
}

// Parse parses a formatted number string.
//
// Example:
//
//	apoc.number.parse('1,234.56') => 1234.56
func Parse(str string) (float64, error) {
	// Remove common formatting characters
	cleaned := strings.ReplaceAll(str, ",", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	cleaned = strings.TrimSpace(cleaned)

	return strconv.ParseFloat(cleaned, 64)
}

// ParseInt parses a string to integer.
//
// Example:
//
//	apoc.number.parseInt('42') => 42
func ParseInt(str string, base int) (int64, error) {
	cleaned := strings.TrimSpace(str)
	return strconv.ParseInt(cleaned, base, 64)
}

// ParseFloat parses a string to float.
//
// Example:
//
//	apoc.number.parseFloat('3.14') => 3.14
func ParseFloat(str string) (float64, error) {
	return Parse(str)
}

// Exact returns exact decimal representation.
//
// Example:
//
//	apoc.number.exact(0.1) => exact decimal
func Exact(number float64) string {
	return strconv.FormatFloat(number, 'f', -1, 64)
}

// Arabize converts Roman numerals to Arabic numbers.
//
// Example:
//
//	apoc.number.arabize('XIV') => 14
func Arabize(roman string) int {
	romanMap := map[rune]int{
		'I': 1,
		'V': 5,
		'X': 10,
		'L': 50,
		'C': 100,
		'D': 500,
		'M': 1000,
	}

	result := 0
	prevValue := 0

	for i := len(roman) - 1; i >= 0; i-- {
		value := romanMap[rune(roman[i])]
		if value < prevValue {
			result -= value
		} else {
			result += value
		}
		prevValue = value
	}

	return result
}

// Romanize converts Arabic numbers to Roman numerals.
//
// Example:
//
//	apoc.number.romanize(14) => 'XIV'
func Romanize(number int) string {
	if number <= 0 || number >= 4000 {
		return ""
	}

	values := []int{1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1}
	symbols := []string{"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"}

	var result strings.Builder
	for i := 0; i < len(values); i++ {
		for number >= values[i] {
			result.WriteString(symbols[i])
			number -= values[i]
		}
	}

	return result.String()
}

// ToHex converts a number to hexadecimal.
//
// Example:
//
//	apoc.number.toHex(255) => 'FF'
func ToHex(number int64) string {
	return strings.ToUpper(strconv.FormatInt(number, 16))
}

// FromHex converts hexadecimal to number.
//
// Example:
//
//	apoc.number.fromHex('FF') => 255
func FromHex(hex string) (int64, error) {
	return strconv.ParseInt(hex, 16, 64)
}

// ToOctal converts a number to octal.
//
// Example:
//
//	apoc.number.toOctal(64) => '100'
func ToOctal(number int64) string {
	return strconv.FormatInt(number, 8)
}

// FromOctal converts octal to number.
//
// Example:
//
//	apoc.number.fromOctal('100') => 64
func FromOctal(octal string) (int64, error) {
	return strconv.ParseInt(octal, 8, 64)
}

// ToBinary converts a number to binary.
//
// Example:
//
//	apoc.number.toBinary(10) => '1010'
func ToBinary(number int64) string {
	return strconv.FormatInt(number, 2)
}

// FromBinary converts binary to number.
//
// Example:
//
//	apoc.number.fromBinary('1010') => 10
func FromBinary(binary string) (int64, error) {
	return strconv.ParseInt(binary, 2, 64)
}

// ToBase converts a number to specified base.
//
// Example:
//
//	apoc.number.toBase(100, 36) => '2S'
func ToBase(number int64, base int) string {
	if base < 2 || base > 36 {
		return ""
	}
	return strings.ToUpper(strconv.FormatInt(number, base))
}

// FromBase converts from specified base to decimal.
//
// Example:
//
//	apoc.number.fromBase('2S', 36) => 100
func FromBase(str string, base int) (int64, error) {
	if base < 2 || base > 36 {
		return 0, fmt.Errorf("base must be between 2 and 36")
	}
	return strconv.ParseInt(str, base, 64)
}

// Round rounds a number to specified decimal places.
//
// Example:
//
//	apoc.number.round(3.14159, 2) => 3.14
func Round(number float64, decimals int) float64 {
	multiplier := math.Pow(10, float64(decimals))
	return math.Round(number*multiplier) / multiplier
}

// Ceil rounds up to nearest integer.
//
// Example:
//
//	apoc.number.ceil(3.14) => 4
func Ceil(number float64) float64 {
	return math.Ceil(number)
}

// Floor rounds down to nearest integer.
//
// Example:
//
//	apoc.number.floor(3.14) => 3
func Floor(number float64) float64 {
	return math.Floor(number)
}

// Abs returns absolute value.
//
// Example:
//
//	apoc.number.abs(-5) => 5
func Abs(number float64) float64 {
	return math.Abs(number)
}

// Sign returns the sign of a number (-1, 0, or 1).
//
// Example:
//
//	apoc.number.sign(-5) => -1
func Sign(number float64) int {
	if number > 0 {
		return 1
	} else if number < 0 {
		return -1
	}
	return 0
}

// Clamp clamps a number between min and max.
//
// Example:
//
//	apoc.number.clamp(15, 0, 10) => 10
func Clamp(number, min, max float64) float64 {
	if number < min {
		return min
	}
	if number > max {
		return max
	}
	return number
}

// Lerp performs linear interpolation.
//
// Example:
//
//	apoc.number.lerp(0, 100, 0.5) => 50
func Lerp(start, end, t float64) float64 {
	return start + (end-start)*t
}

// Normalize normalizes a number to 0-1 range.
//
// Example:
//
//	apoc.number.normalize(50, 0, 100) => 0.5
func Normalize(value, min, max float64) float64 {
	if max == min {
		return 0
	}
	return (value - min) / (max - min)
}

// Map maps a number from one range to another.
//
// Example:
//
//	apoc.number.map(50, 0, 100, 0, 1) => 0.5
func Map(value, inMin, inMax, outMin, outMax float64) float64 {
	normalized := Normalize(value, inMin, inMax)
	return Lerp(outMin, outMax, normalized)
}

// IsEven checks if a number is even.
//
// Example:
//
//	apoc.number.isEven(4) => true
func IsEven(number int64) bool {
	return number%2 == 0
}

// IsOdd checks if a number is odd.
//
// Example:
//
//	apoc.number.isOdd(5) => true
func IsOdd(number int64) bool {
	return number%2 != 0
}

// IsPrime checks if a number is prime.
//
// Example:
//
//	apoc.number.isPrime(7) => true
func IsPrime(number int64) bool {
	if number <= 1 {
		return false
	}
	if number <= 3 {
		return true
	}
	if number%2 == 0 || number%3 == 0 {
		return false
	}

	for i := int64(5); i*i <= number; i += 6 {
		if number%i == 0 || number%(i+2) == 0 {
			return false
		}
	}

	return true
}

// GCD calculates greatest common divisor.
//
// Example:
//
//	apoc.number.gcd(48, 18) => 6
func GCD(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// LCM calculates least common multiple.
//
// Example:
//
//	apoc.number.lcm(4, 6) => 12
func LCM(a, b int64) int64 {
	return (a * b) / GCD(a, b)
}

// Factorial calculates factorial.
//
// Example:
//
//	apoc.number.factorial(5) => 120
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

// Fibonacci calculates nth Fibonacci number.
//
// Example:
//
//	apoc.number.fibonacci(10) => 55
func Fibonacci(n int) int64 {
	if n <= 1 {
		return int64(n)
	}

	a, b := int64(0), int64(1)
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

// Power calculates power.
//
// Example:
//
//	apoc.number.power(2, 8) => 256
func Power(base, exponent float64) float64 {
	return math.Pow(base, exponent)
}

// Sqrt calculates square root.
//
// Example:
//
//	apoc.number.sqrt(16) => 4
func Sqrt(number float64) float64 {
	return math.Sqrt(number)
}

// Log calculates natural logarithm.
//
// Example:
//
//	apoc.number.log(math.E) => 1
func Log(number float64) float64 {
	return math.Log(number)
}

// Log10 calculates base-10 logarithm.
//
// Example:
//
//	apoc.number.log10(100) => 2
func Log10(number float64) float64 {
	return math.Log10(number)
}

// Exp calculates e^x.
//
// Example:
//
//	apoc.number.exp(1) => 2.718...
func Exp(x float64) float64 {
	return math.Exp(x)
}

// Random generates a random float between 0 and 1.
//
// Example:
//
//	apoc.number.random() => 0.456...
func Random() float64 {
	// Placeholder - would use crypto/rand or math/rand
	return 0.5
}

// RandomInt generates a random integer in range.
//
// Example:
//
//	apoc.number.randomInt(1, 100) => 42
func RandomInt(min, max int64) int64 {
	if min >= max {
		return min
	}
	// Placeholder - would use crypto/rand or math/rand
	return (min + max) / 2
}
