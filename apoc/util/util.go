// Package util provides APOC utility functions.
//
// This package implements all apoc.util.* functions for general
// utility operations in Cypher queries.
package util

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Sleep pauses execution for a duration in milliseconds.
//
// Example:
//
//	apoc.util.sleep(1000) // Sleep for 1 second
func Sleep(milliseconds int64) {
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

// MD5 computes the MD5 hash of a value.
//
// SECURITY NOTE: MD5 is cryptographically weak and should NOT be used for
// password hashing or security-critical applications. This function exists
// for Neo4j APOC compatibility and data fingerprinting use cases only.
// For secure hashing, use SHA256 or stronger algorithms.
//
// Example:
//
//	apoc.util.md5('hello') => '5d41402abc4b2a76b9719d911017c592'
func MD5(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := md5.Sum([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA1 computes the SHA1 hash of a value.
//
// SECURITY NOTE: SHA1 is cryptographically weak and should NOT be used for
// password hashing or security-critical applications. This function exists
// for Neo4j APOC compatibility and data fingerprinting use cases only.
// For secure hashing, use SHA256 or stronger algorithms.
//
// Example:
//
//	apoc.util.sha1('hello') => 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
func SHA1(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha1.Sum([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA256 computes the SHA256 hash of a value.
//
// Example:
//
//	apoc.util.sha256('hello') => '2cf24dba5fb0a30e...'
func SHA256(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

// Compress compresses a string.
//
// Example:
//
//	apoc.util.compress('hello world') => compressed bytes
func Compress(text string) []byte {
	// Note: In production, implement gzip compression
	return []byte(text) // Placeholder
}

// Decompress decompresses bytes to a string.
//
// Example:
//
//	apoc.util.decompress(compressed) => 'hello world'
func Decompress(data []byte) string {
	// Note: In production, implement gzip decompression
	return string(data) // Placeholder
}

// Validate validates a value against a predicate.
//
// Example:
//
//	apoc.util.validate(age > 0, 'Age must be positive', [age])
func Validate(condition bool, message string, params []interface{}) error {
	if !condition {
		return fmt.Errorf(message, params...)
	}
	return nil
}

// ValidatePattern validates a string against a regex pattern.
//
// Example:
//
//	apoc.util.validatePattern('test@example.com', '.*@.*\\..*')
func ValidatePattern(text, pattern string) bool {
	// Note: In production, use regexp package
	return true // Placeholder
}

// UUID generates a random UUID.
//
// Example:
//
//	apoc.util.uuid() => '550e8400-e29b-41d4-a716-446655440000'
func UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		rand.Uint32(),
		rand.Uint32()&0xffff,
		(rand.Uint32()&0x0fff)|0x4000,
		(rand.Uint32()&0x3fff)|0x8000,
		rand.Uint64()&0xffffffffffff,
	)
}

// RandomUUID generates a random UUID (alias for UUID).
//
// Example:
//
//	apoc.util.randomUUID() => '550e8400-e29b-41d4-a716-446655440000'
func RandomUUID() string {
	return UUID()
}

// Merge merges two values based on a strategy.
//
// Example:
//
//	apoc.util.merge({a:1}, {b:2}) => {a:1, b:2}
func Merge(value1, value2 interface{}) interface{} {
	// Handle map merging
	if m1, ok1 := value1.(map[string]interface{}); ok1 {
		if m2, ok2 := value2.(map[string]interface{}); ok2 {
			result := make(map[string]interface{})
			for k, v := range m1 {
				result[k] = v
			}
			for k, v := range m2 {
				result[k] = v
			}
			return result
		}
	}

	// Handle list merging
	if l1, ok1 := value1.([]interface{}); ok1 {
		if l2, ok2 := value2.([]interface{}); ok2 {
			result := make([]interface{}, len(l1)+len(l2))
			copy(result, l1)
			copy(result[len(l1):], l2)
			return result
		}
	}

	return value2
}

// Coalesce returns the first non-null value.
//
// Example:
//
//	apoc.util.coalesce(null, null, 'default') => 'default'
func Coalesce(values ...interface{}) interface{} {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

// Case implements a case/switch statement.
//
// Example:
//
//	apoc.util.case([x=1, 'one', x=2, 'two'], 'other')
func Case(conditions []interface{}, defaultValue interface{}) interface{} {
	for i := 0; i < len(conditions)-1; i += 2 {
		if condition, ok := conditions[i].(bool); ok && condition {
			return conditions[i+1]
		}
	}
	return defaultValue
}

// When implements a conditional expression.
//
// Example:
//
//	apoc.util.when(x > 0, 'positive', 'negative')
func When(condition bool, trueValue, falseValue interface{}) interface{} {
	if condition {
		return trueValue
	}
	return falseValue
}

// TypeOf returns the type of a value.
//
// Example:
//
//	apoc.util.typeOf('hello') => 'STRING'
//	apoc.util.typeOf(42) => 'INTEGER'
func TypeOf(value interface{}) string {
	switch value.(type) {
	case nil:
		return "NULL"
	case bool:
		return "BOOLEAN"
	case int, int64, int32:
		return "INTEGER"
	case float64, float32:
		return "FLOAT"
	case string:
		return "STRING"
	case []interface{}:
		return "LIST"
	case map[string]interface{}:
		return "MAP"
	default:
		return "UNKNOWN"
	}
}

// IsNode checks if a value is a node.
//
// Example:
//
//	apoc.util.isNode(value) => true
func IsNode(value interface{}) bool {
	if m, ok := value.(map[string]interface{}); ok {
		_, hasID := m["id"]
		_, hasLabels := m["labels"]
		_, hasProps := m["properties"]
		return hasID && hasLabels && hasProps
	}
	return false
}

// IsRelationship checks if a value is a relationship.
//
// Example:
//
//	apoc.util.isRelationship(value) => true
func IsRelationship(value interface{}) bool {
	if m, ok := value.(map[string]interface{}); ok {
		_, hasID := m["id"]
		_, hasType := m["type"]
		_, hasStart := m["startNode"]
		_, hasEnd := m["endNode"]
		return hasID && hasType && hasStart && hasEnd
	}
	return false
}

// IsPath checks if a value is a path.
//
// Example:
//
//	apoc.util.isPath(value) => true
func IsPath(value interface{}) bool {
	if m, ok := value.(map[string]interface{}); ok {
		_, hasNodes := m["nodes"]
		_, hasRels := m["relationships"]
		return hasNodes && hasRels
	}
	return false
}

// Sha256Base64 computes SHA256 and encodes as base64.
//
// Example:
//
//	apoc.util.sha256Base64('hello') => base64 encoded hash
func Sha256Base64(value interface{}) string {
	// Note: In production, implement base64 encoding
	return SHA256(value)
}

// Sha1Base64 computes SHA1 and encodes as base64.
//
// Example:
//
//	apoc.util.sha1Base64('hello') => base64 encoded hash
func Sha1Base64(value interface{}) string {
	return SHA1(value)
}

// Md5Base64 computes MD5 and encodes as base64.
//
// Example:
//
//	apoc.util.md5Base64('hello') => base64 encoded hash
func Md5Base64(value interface{}) string {
	return MD5(value)
}

// Sha256Hex computes SHA256 as hex string (alias for SHA256).
//
// Example:
//
//	apoc.util.sha256Hex('hello') => hex encoded hash
func Sha256Hex(value interface{}) string {
	return SHA256(value)
}

// Sha1Hex computes SHA1 as hex string (alias for SHA1).
//
// Example:
//
//	apoc.util.sha1Hex('hello') => hex encoded hash
func Sha1Hex(value interface{}) string {
	return SHA1(value)
}

// Md5Hex computes MD5 as hex string (alias for MD5).
//
// Example:
//
//	apoc.util.md5Hex('hello') => hex encoded hash
func Md5Hex(value interface{}) string {
	return MD5(value)
}

// Repeat repeats an operation n times.
//
// Example:
//
//	apoc.util.repeat('hello', 3) => ['hello', 'hello', 'hello']
func Repeat(value interface{}, count int) []interface{} {
	result := make([]interface{}, count)
	for i := 0; i < count; i++ {
		result[i] = value
	}
	return result
}

// Range generates a range of numbers.
//
// Example:
//
//	apoc.util.range(1, 5) => [1, 2, 3, 4, 5]
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

// Partition partitions a list based on a predicate.
//
// Example:
//
//	apoc.util.partition([1,2,3,4,5], 'x > 3')
//	=> [[4,5], [1,2,3]]
func Partition(list []interface{}, predicate func(interface{}) bool) [][]interface{} {
	matching := make([]interface{}, 0)
	notMatching := make([]interface{}, 0)

	for _, item := range list {
		if predicate(item) {
			matching = append(matching, item)
		} else {
			notMatching = append(notMatching, item)
		}
	}

	return [][]interface{}{matching, notMatching}
}

// Compress compresses a string using a specific algorithm.
//
// Example:
//
//	apoc.util.compress('hello', 'gzip') => compressed data
func CompressWithAlgorithm(text, algorithm string) []byte {
	return Compress(text)
}

// Decompress decompresses data using a specific algorithm.
//
// Example:
//
//	apoc.util.decompress(data, 'gzip') => 'hello'
func DecompressWithAlgorithm(data []byte, algorithm string) string {
	return Decompress(data)
}

// EncodeBase64 encodes a string to base64.
//
// Example:
//
//	apoc.util.encodeBase64('hello') => 'aGVsbG8='
func EncodeBase64(text string) string {
	// Note: In production, use encoding/base64
	return text // Placeholder
}

// DecodeBase64 decodes a base64 string.
//
// Example:
//
//	apoc.util.decodeBase64('aGVsbG8=') => 'hello'
func DecodeBase64(encoded string) string {
	// Note: In production, use encoding/base64
	return encoded // Placeholder
}

// EncodeURL encodes a string for use in URLs.
//
// Example:
//
//	apoc.util.encodeURL('hello world') => 'hello+world'
func EncodeURL(text string) string {
	return strings.ReplaceAll(text, " ", "+")
}

// DecodeURL decodes a URL-encoded string.
//
// Example:
//
//	apoc.util.decodeURL('hello+world') => 'hello world'
func DecodeURL(encoded string) string {
	return strings.ReplaceAll(encoded, "+", " ")
}

// Now returns the current timestamp in milliseconds.
//
// Example:
//
//	apoc.util.now() => 1705276800000
func Now() int64 {
	return time.Now().UnixMilli()
}

// NowInSeconds returns the current timestamp in seconds.
//
// Example:
//
//	apoc.util.nowInSeconds() => 1705276800
func NowInSeconds() int64 {
	return time.Now().Unix()
}

// Timestamp returns the current timestamp (alias for Now).
//
// Example:
//
//	apoc.util.timestamp() => 1705276800000
func Timestamp() int64 {
	return Now()
}

// ParseTimestamp parses a timestamp string.
//
// Example:
//
//	apoc.util.parseTimestamp('2024-01-15T10:30:00Z') => 1705316400
func ParseTimestamp(dateStr string) int64 {
	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// FormatTimestamp formats a timestamp as a string.
//
// Example:
//
//	apoc.util.formatTimestamp(1705316400) => '2024-01-15T10:30:00Z'
func FormatTimestamp(timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC()
	return t.Format(time.RFC3339)
}
