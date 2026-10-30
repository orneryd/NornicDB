# PackStream Integer Encoding for JavaScript Compatibility

## Problem

Client applications reported that integer values (like `usedCount`) were being returned as JavaScript `BigInt` instead of regular `Number`, causing runtime errors:

```javascript
// ERROR: Cannot mix BigInt and other types
const usedCount = result.records[0].get("usedCount"); // BigInt from NornicDB
const nextCount = usedCount + 1; // ❌ TypeError: can't convert BigInt to number
```

## Root Cause

The Neo4j JavaScript driver has specific behavior for decoding PackStream integers:

- **INT32 and smaller** (markers 0xC8, 0xC9, 0xCA, or inline) → decoded as JavaScript `Number`
- **INT64** (marker 0xCB) → decoded as JavaScript `BigInt`

NornicDB was already using optimal encoding, but this documents the Neo4j compatibility requirements.

## Solution

NornicDB's `encodePackStreamInt` function now explicitly documents Neo4j compatibility and uses the smallest possible encoding:

```go
func encodePackStreamInt(val int64) []byte {
    // Tiny: -16 to 127 → 1 byte (inline)
    // INT8: -128 to -17 → 2 bytes (marker 0xC8)
    // INT16: -32768 to 32767 → 3 bytes (marker 0xC9)
    // INT32: -2147483648 to 2147483647 → 5 bytes (marker 0xCA)
    // INT64: everything else → 9 bytes (marker 0xCB)
}
```

## PackStream Encoding Ranges

| Encoding  | Range                      | Marker             | Bytes | JS Decoding |
| --------- | -------------------------- | ------------------ | ----- | ----------- |
| **Tiny**  | -16 to 127                 | inline (0xF0-0x7F) | 1     | Number ✅   |
| **INT8**  | -128 to -17                | 0xC8               | 2     | Number ✅   |
| **INT16** | -32768 to 32767            | 0xC9               | 3     | Number ✅   |
| **INT32** | -2147483648 to 2147483647  | 0xCA               | 5     | Number ✅   |
| **INT64** | < INT32 min or > INT32 max | 0xCB               | 9     | BigInt ⚠️   |

## JavaScript Safe Integer Range

JavaScript's `Number.MAX_SAFE_INTEGER` is 2^53 - 1 (9,007,199,254,740,991), but PackStream INT32 max is only 2^31 - 1 (2,147,483,647).

**Key Insight:** Since INT32 range is entirely within JavaScript's safe integer range, using INT32 encoding (or smaller) guarantees the Neo4j driver will return a regular `Number`, not a `BigInt`.

## Examples

### usedCount (Typical Values)

```go
// usedCount values: 0, 1, 2, ..., 100
encodePackStreamInt(1)   → [0x01]           (1 byte, tiny)    → JS Number ✅
encodePackStreamInt(100) → [0x64]           (1 byte, tiny)    → JS Number ✅
```

### Larger Values

```go
encodePackStreamInt(1000)       → [0xC9, 0x03, 0xE8]         (INT16) → JS Number ✅
encodePackStreamInt(100000)     → [0xCA, 0x00, 0x01, ...]   (INT32) → JS Number ✅
encodePackStreamInt(2147483647) → [0xCA, 0x7F, 0xFF, ...]   (INT32) → JS Number ✅
```

### INT64 Boundary (Becomes BigInt)

```go
encodePackStreamInt(2147483648) → [0xCB, 0x00, 0x00, ...] (INT64) → JS BigInt ⚠️
// This will cause issues in JavaScript arithmetic!
```

## Testing

Run JavaScript compatibility tests:

```bash
go test ./pkg/bolt -run TestJavaScript -v
```

Tests verify:

- Small integers use tiny encoding (1 byte)
- Medium integers use INT16/INT32 encoding
- Only values outside INT32 range use INT64
- The usedCount scenario works correctly

## Neo4j Driver Behavior by Language

| Language       | INT32 & Smaller  | INT64    | Issues?             |
| -------------- | ---------------- | -------- | ------------------- |
| **JavaScript** | `Number`         | `BigInt` | ⚠️ Cannot mix types |
| **Python**     | `int`            | `int`    | ✅ No issues        |
| **Go**         | `int64`          | `int64`  | ✅ No issues        |
| **Java**       | `Integer`/`Long` | `Long`   | ✅ Autoboxing works |

## References

- [PackStream Specification](https://github.com/neo4j/docs-bolt/blob/dev/modules/ROOT/pages/packstream/packstream-specification-v1.adoc)
- [Neo4j Bolt Protocol](https://neo4j.com/docs/bolt/current/)
- [JavaScript BigInt](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt)
- [Number.MAX_SAFE_INTEGER](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER)

## Related Issues

- Reported issue: `usedCount + 1` causing BigInt/Number mixing errors
- Fix: Already using optimal encoding; added documentation
- Tests: `pkg/bolt/javascript_compat_test.go`
