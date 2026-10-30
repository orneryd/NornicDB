package bolt

import (
	"testing"
)

// TestJavaScriptDriverCompatibility verifies that integer encodings match
// Neo4j's behavior for JavaScript driver compatibility.
//
// BUG REPORT: Reported that usedCount was returned as BigInt instead of Number,
// causing JavaScript errors like "Cannot mix BigInt and other types".
//
// ROOT CAUSE: Neo4j JavaScript driver behavior:
//   - INT32 and smaller (markers 0xC0-0xCA) → decoded as JavaScript Number
//   - INT64 (marker 0xCB) → decoded as JavaScript BigInt
//
// FIX: Always use INT32 or smaller encoding for values in range -2^31 to 2^31-1.
// This ensures JS driver returns regular Numbers, not BigInts.
//
// Test cases cover:
//   - Small integers (should use tiny encoding)
//   - Medium integers (should use INT16)
//   - Larger integers (should use INT32)
//   - Verify INT64 is ONLY used for values > INT32 range
func TestJavaScriptDriverCompatibility(t *testing.T) {
	tests := []struct {
		name           string
		value          int64
		expectedMarker byte   // First byte (marker)
		expectedLen    int    // Total encoded length
		jsType         string // Expected JS type: "Number" or "BigInt"
	}{
		{
			name:           "zero (tiny)",
			value:          0,
			expectedMarker: 0x00,
			expectedLen:    1,
			jsType:         "Number",
		},
		{
			name:           "small positive (tiny)",
			value:          42,
			expectedMarker: 42,
			expectedLen:    1,
			jsType:         "Number",
		},
		{
			name:           "small negative (tiny)",
			value:          -1,
			expectedMarker: 0xFF,
			expectedLen:    1,
			jsType:         "Number",
		},
		{
			name:           "usedCount=1 (typical value)",
			value:          1,
			expectedMarker: 0x01,
			expectedLen:    1,
			jsType:         "Number",
		},
		{
			name:           "usedCount=100",
			value:          100,
			expectedMarker: 100,
			expectedLen:    1,
			jsType:         "Number",
		},
		{
			name:           "INT8 boundary",
			value:          -17,
			expectedMarker: 0xC8,
			expectedLen:    2,
			jsType:         "Number",
		},
		{
			name:           "INT16 needed",
			value:          1000,
			expectedMarker: 0xC9,
			expectedLen:    3,
			jsType:         "Number",
		},
		{
			name:           "INT32 needed",
			value:          100000,
			expectedMarker: 0xCA,
			expectedLen:    5,
			jsType:         "Number",
		},
		{
			name:           "large INT32 (still Number in JS)",
			value:          2147483647, // Max INT32
			expectedMarker: 0xCA,
			expectedLen:    5,
			jsType:         "Number",
		},
		{
			name:           "INT64 boundary (becomes BigInt)",
			value:          2147483648, // Max INT32 + 1
			expectedMarker: 0xCB,
			expectedLen:    9,
			jsType:         "BigInt",
		},
		{
			name:           "large negative INT32 (still Number)",
			value:          -2147483648, // Min INT32
			expectedMarker: 0xCA,
			expectedLen:    5,
			jsType:         "Number",
		},
		{
			name:           "beyond INT32 (becomes BigInt)",
			value:          -2147483649, // Min INT32 - 1
			expectedMarker: 0xCB,
			expectedLen:    9,
			jsType:         "BigInt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodePackStreamInt(tt.value)

			// Verify marker
			if encoded[0] != tt.expectedMarker {
				t.Errorf("marker mismatch: got 0x%02X, want 0x%02X", encoded[0], tt.expectedMarker)
			}

			// Verify length
			if len(encoded) != tt.expectedLen {
				t.Errorf("length mismatch: got %d bytes, want %d bytes", len(encoded), tt.expectedLen)
			}

			// Document expected JS behavior
			t.Logf("Value %d → PackStream marker 0x%02X (%d bytes) → JavaScript %s",
				tt.value, encoded[0], len(encoded), tt.jsType)
		})
	}
}

// TestUsedCountScenario specifically tests the reported bug scenario.
//
// Typical query:
//
//	MATCH (n:Node {type: 'preamble'})
//	WHERE n.roleHash = $roleHash
//	RETURN n.content as content, n.id as id, n.usedCount as usedCount
//
// Problem: usedCount (typically 0-100) was returned as BigInt
// Cause: Incorrect PackStream encoding (INT64 instead of INT32/tiny)
// Fix: Use smallest encoding (typically tiny int for values 0-127)
func TestUsedCountScenario(t *testing.T) {
	// Common usedCount values
	usedCounts := []int64{0, 1, 2, 5, 10, 50, 100}

	for _, count := range usedCounts {
		t.Run("usedCount="+string(rune(count)), func(t *testing.T) {
			encoded := encodePackStreamInt(count)

			// All these values should use tiny encoding (1 byte)
			if len(encoded) != 1 {
				t.Errorf("usedCount=%d should use tiny encoding (1 byte), got %d bytes", count, len(encoded))
			}

			// Verify marker is in tiny range (0x00-0x7F for positive)
			if encoded[0] > 0x7F {
				t.Errorf("usedCount=%d should have tiny marker (0x00-0x7F), got 0x%02X", count, encoded[0])
			}

			// Tiny encoding → JavaScript Number (not BigInt) ✅
			t.Logf("✅ usedCount=%d → tiny int (1 byte, marker 0x%02X) → JavaScript Number", count, encoded[0])
		})
	}
}

// TestPackStreamEncodingRanges documents the PackStream encoding boundaries
// and their JavaScript driver implications.
func TestPackStreamEncodingRanges(t *testing.T) {
	type encodingRange struct {
		name       string
		min        int64
		max        int64
		marker     string
		bytes      int
		jsDecoding string
	}

	ranges := []encodingRange{
		{
			name:       "Tiny Int",
			min:        -16,
			max:        127,
			marker:     "inline (0xF0-0x7F)",
			bytes:      1,
			jsDecoding: "Number",
		},
		{
			name:       "INT8",
			min:        -128,
			max:        -17,
			marker:     "0xC8",
			bytes:      2,
			jsDecoding: "Number",
		},
		{
			name:       "INT16",
			min:        -32768,
			max:        32767,
			marker:     "0xC9",
			bytes:      3,
			jsDecoding: "Number",
		},
		{
			name:       "INT32",
			min:        -2147483648,
			max:        2147483647,
			marker:     "0xCA",
			bytes:      5,
			jsDecoding: "Number",
		},
		{
			name:       "INT64",
			min:        -9223372036854775808,
			max:        9223372036854775807,
			marker:     "0xCB",
			bytes:      9,
			jsDecoding: "BigInt ⚠️",
		},
	}

	t.Log("PackStream Integer Encoding Ranges:")
	t.Log("=====================================")
	for _, r := range ranges {
		t.Logf("%-10s | %20d to %20d | Marker: %-20s | Size: %d bytes | JS: %s",
			r.name, r.min, r.max, r.marker, r.bytes, r.jsDecoding)

		// Verify min boundary
		encodedMin := encodePackStreamInt(r.min)
		if len(encodedMin) != r.bytes {
			// Allow flexibility for boundary conditions (tiny int overlaps)
			if r.name != "Tiny Int" && r.name != "INT8" {
				t.Errorf("%s min value encoded incorrectly: got %d bytes, want %d bytes",
					r.name, len(encodedMin), r.bytes)
			}
		}

		// Verify max boundary
		encodedMax := encodePackStreamInt(r.max)
		if len(encodedMax) != r.bytes {
			if r.name != "Tiny Int" {
				t.Errorf("%s max value encoded incorrectly: got %d bytes, want %d bytes",
					r.name, len(encodedMax), r.bytes)
			}
		}
	}

	t.Log("\n🔑 Key Insight:")
	t.Log("   INT32 and smaller → JavaScript Number (safe for arithmetic)")
	t.Log("   INT64 → JavaScript BigInt (requires BigInt arithmetic)")
	t.Log("\n💡 For JavaScript compatibility: Always use INT32 or smaller when possible!")
}

// TestNeo4jCompatibilityDocumentation is a documentation test that explains
// the Neo4j driver behavior across different languages.
func TestNeo4jCompatibilityDocumentation(t *testing.T) {
	t.Log("Neo4j Driver Integer Handling by Language:")
	t.Log("===========================================")
	t.Log("")
	t.Log("JavaScript/TypeScript (neo4j-driver):")
	t.Log("  • INT32 and smaller → Number (standard JS number)")
	t.Log("  • INT64 → BigInt (requires BigInt arithmetic)")
	t.Log("  • Issue: Cannot mix Number and BigInt in operations")
	t.Log("  • Fix: Use INT32 encoding for typical integer values")
	t.Log("")
	t.Log("Python (neo4j-driver):")
	t.Log("  • All integer encodings → int (Python handles arbitrarily large integers)")
	t.Log("  • No compatibility issues")
	t.Log("")
	t.Log("Go (neo4j-driver):")
	t.Log("  • All integer encodings → int64")
	t.Log("  • No compatibility issues")
	t.Log("")
	t.Log("Java (neo4j-driver):")
	t.Log("  • INT32 and smaller → Integer or Long")
	t.Log("  • INT64 → Long")
	t.Log("  • No compatibility issues (autoboxing)")
	t.Log("")
	t.Log("🎯 NornicDB Strategy:")
	t.Log("   Use smallest encoding possible:")
	t.Log("     -16 to 127 → Tiny (1 byte)")
	t.Log("     -128 to -17 → INT8 (2 bytes)")
	t.Log("     -32768 to 32767 → INT16 (3 bytes)")
	t.Log("     -2147483648 to 2147483647 → INT32 (5 bytes)")
	t.Log("     Everything else → INT64 (9 bytes)")
	t.Log("")
	t.Log("   This matches Neo4j behavior and ensures JavaScript compatibility!")
}
