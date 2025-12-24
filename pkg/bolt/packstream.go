// Package bolt implements the Neo4j Bolt protocol server for NornicDB.
package bolt

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var packstreamZero8 [8]byte
var packstreamFallbackHook func(v any)

// ============================================================================
// PackStream Encoding
// ============================================================================

func encodePackStreamMapInto(dst []byte, m map[string]any) []byte {
	if len(m) == 0 {
		return append(dst, 0xA0)
	}

	// Some Cypher internals use helper sentinel keys that should not be serialized
	// to clients. Keep these out of the wire format.
	//
	// Note: we cannot skip all "_" keys because Bolt-compatible node/edge maps use
	// `_nodeId` / `_edgeId`.
	skipPathResult := false
	if _, ok := m["_pathResult"]; ok {
		skipPathResult = true
	}

	size := len(m)
	if skipPathResult {
		size--
	}
	if size < 16 {
		dst = append(dst, byte(0xA0+size))
	} else if size < 256 {
		dst = append(dst, 0xD8, byte(size))
	} else {
		dst = append(dst, 0xD9, byte(size>>8), byte(size))
	}

	for k, v := range m {
		if k == "_pathResult" {
			continue
		}
		dst = encodePackStreamStringInto(dst, k)
		dst = encodePackStreamValueInto(dst, v)
	}

	return dst
}

func encodePackStreamMap(m map[string]any) []byte {
	return encodePackStreamMapInto(nil, m)
}

func encodePackStreamListInto(dst []byte, items []any) []byte {
	if len(items) == 0 {
		return append(dst, 0x90)
	}

	size := len(items)
	if size < 16 {
		dst = append(dst, byte(0x90+size))
	} else if size < 256 {
		dst = append(dst, 0xD4, byte(size))
	} else {
		dst = append(dst, 0xD5, byte(size>>8), byte(size))
	}

	for _, item := range items {
		dst = encodePackStreamValueInto(dst, item)
	}

	return dst
}

func encodePackStreamList(items []any) []byte {
	return encodePackStreamListInto(nil, items)
}

func encodePackStreamStringInto(dst []byte, s string) []byte {
	length := len(s)

	if length < 16 {
		dst = append(dst, byte(0x80+length))
	} else if length < 256 {
		dst = append(dst, 0xD0, byte(length))
	} else if length < 65536 {
		dst = append(dst, 0xD1, byte(length>>8), byte(length))
	} else {
		dst = append(dst, 0xD2, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}

	return append(dst, s...)
}

func encodePackStreamString(s string) []byte {
	return encodePackStreamStringInto(nil, s)
}

func encodePackStreamBytesInto(dst []byte, b []byte) []byte {
	size := len(b)
	if size < 256 {
		dst = append(dst, 0xCC, byte(size))
	} else if size < 65536 {
		dst = append(dst, 0xCD, byte(size>>8), byte(size))
	} else {
		dst = append(dst, 0xCE, byte(size>>24), byte(size>>16), byte(size>>8), byte(size))
	}
	return append(dst, b...)
}

func encodePackStreamValueInto(dst []byte, v any) []byte {
	switch val := v.(type) {
	case nil:
		return append(dst, 0xC0)
	case bool:
		if val {
			return append(dst, 0xC3)
		}
		return append(dst, 0xC2)
	// All integer types - encode as INT64 for Neo4j driver compatibility
	case int:
		return encodePackStreamIntInto(dst, int64(val))
	case int8:
		return encodePackStreamIntInto(dst, int64(val))
	case int16:
		return encodePackStreamIntInto(dst, int64(val))
	case int32:
		return encodePackStreamIntInto(dst, int64(val))
	case int64:
		return encodePackStreamIntInto(dst, val)
	case uint:
		return encodePackStreamIntInto(dst, int64(val))
	case uint8:
		return encodePackStreamIntInto(dst, int64(val))
	case uint16:
		return encodePackStreamIntInto(dst, int64(val))
	case uint32:
		return encodePackStreamIntInto(dst, int64(val))
	case uint64:
		return encodePackStreamIntInto(dst, int64(val))
	// Float types
	case float32:
		dst = append(dst, 0xC1)
		dst = append(dst, packstreamZero8[:]...)
		binary.BigEndian.PutUint64(dst[len(dst)-8:], math.Float64bits(float64(val)))
		return dst
	case float64:
		dst = append(dst, 0xC1)
		dst = append(dst, packstreamZero8[:]...)
		binary.BigEndian.PutUint64(dst[len(dst)-8:], math.Float64bits(val))
		return dst
	case string:
		return encodePackStreamStringInto(dst, val)
	case []byte:
		return encodePackStreamBytesInto(dst, val)
	case storage.NodeID:
		return encodePackStreamStringInto(dst, string(val))
	case storage.EdgeID:
		return encodePackStreamStringInto(dst, string(val))
	// Map types
	case map[string]any:
		// Check if this is a node (has _nodeId and labels)
		if nodeId, hasNodeId := val["_nodeId"]; hasNodeId {
			if labels, hasLabels := val["labels"]; hasLabels {
				return encodeNodeInto(dst, nodeId, labels, val)
			}
		}
		return encodePackStreamMapInto(dst, val)
	case map[string]string:
		if len(val) == 0 {
			return append(dst, 0xA0)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0xA0+size))
		} else if size < 256 {
			dst = append(dst, 0xD8, byte(size))
		} else {
			dst = append(dst, 0xD9, byte(size>>8), byte(size))
		}
		for k, v := range val {
			dst = encodePackStreamStringInto(dst, k)
			dst = encodePackStreamStringInto(dst, v)
		}
		return dst
	// Storage types - Neo4j compatible node/edge encoding
	case storage.Node:
		return encodeStorageNodeInto(dst, &val)
	case *storage.Node:
		if val == nil {
			return append(dst, 0xC0)
		}
		return encodeStorageNodeInto(dst, val)
	case []storage.Node:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for i := range val {
			dst = encodeStorageNodeInto(dst, &val[i])
		}
		return dst
	case []*storage.Node:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, n := range val {
			dst = encodePackStreamValueInto(dst, n)
		}
		return dst
	case storage.Edge:
		return encodeStorageEdgeInto(dst, &val)
	case *storage.Edge:
		if val == nil {
			return append(dst, 0xC0)
		}
		return encodeStorageEdgeInto(dst, val)
	case []storage.Edge:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for i := range val {
			dst = encodeStorageEdgeInto(dst, &val[i])
		}
		return dst
	case []*storage.Edge:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, e := range val {
			dst = encodePackStreamValueInto(dst, e)
		}
		return dst
	// List types
	case []string:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, s := range val {
			dst = encodePackStreamStringInto(dst, s)
		}
		return dst
	case []any:
		return encodePackStreamListInto(dst, val)
	case []int:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, n := range val {
			dst = encodePackStreamIntInto(dst, int64(n))
		}
		return dst
	case []int64:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, n := range val {
			dst = encodePackStreamIntInto(dst, n)
		}
		return dst
	case []float64:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, n := range val {
			dst = append(dst, 0xC1)
			dst = append(dst, packstreamZero8[:]...)
			binary.BigEndian.PutUint64(dst[len(dst)-8:], math.Float64bits(n))
		}
		return dst
	case []float32:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, n := range val {
			dst = append(dst, 0xC1)
			dst = append(dst, packstreamZero8[:]...)
			binary.BigEndian.PutUint64(dst[len(dst)-8:], math.Float64bits(float64(n)))
		}
		return dst
	case []map[string]any:
		if len(val) == 0 {
			return append(dst, 0x90)
		}
		size := len(val)
		if size < 16 {
			dst = append(dst, byte(0x90+size))
		} else if size < 256 {
			dst = append(dst, 0xD4, byte(size))
		} else {
			dst = append(dst, 0xD5, byte(size>>8), byte(size))
		}
		for _, m := range val {
			dst = encodePackStreamMapInto(dst, m)
		}
		return dst
	case time.Time:
		// Encode as Unix millis to avoid allocations and keep a stable scalar representation.
		// NOTE: Neo4j has native temporal types; we can upgrade to proper PackStream
		// temporal structures later without changing the executor.
		return encodePackStreamIntInto(dst, val.UnixNano()/int64(time.Millisecond))
	case time.Duration:
		// Encode duration as milliseconds (signed).
		return encodePackStreamIntInto(dst, val.Milliseconds())
	}

	// Fall back to existing implementation for less common types (nodes, relationships, etc.)
	if packstreamFallbackHook != nil {
		packstreamFallbackHook(v)
	}
	return append(dst, encodePackStreamValue(v)...)
}

func encodePackStreamValue(v any) []byte {
	switch val := v.(type) {
	case nil:
		return []byte{0xC0}
	case bool:
		if val {
			return []byte{0xC3}
		}
		return []byte{0xC2}
	// All integer types - encode as INT64 for Neo4j driver compatibility
	case int:
		return encodePackStreamInt(int64(val))
	case int8:
		return encodePackStreamInt(int64(val))
	case int16:
		return encodePackStreamInt(int64(val))
	case int32:
		return encodePackStreamInt(int64(val))
	case int64:
		return encodePackStreamInt(val)
	case uint:
		return encodePackStreamInt(int64(val))
	case uint8:
		return encodePackStreamInt(int64(val))
	case uint16:
		return encodePackStreamInt(int64(val))
	case uint32:
		return encodePackStreamInt(int64(val))
	case uint64:
		return encodePackStreamInt(int64(val))
	// Float types
	case float32:
		buf := make([]byte, 9)
		buf[0] = 0xC1
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(float64(val)))
		return buf
	case float64:
		buf := make([]byte, 9)
		buf[0] = 0xC1
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(val))
		return buf
	case string:
		return encodePackStreamString(val)
	// List types
	case []string:
		items := make([]any, len(val))
		for i, s := range val {
			items[i] = s
		}
		return encodePackStreamList(items)
	case []any:
		return encodePackStreamList(val)
	case []int:
		items := make([]any, len(val))
		for i, n := range val {
			items[i] = int64(n)
		}
		return encodePackStreamList(items)
	case []int64:
		items := make([]any, len(val))
		for i, n := range val {
			items[i] = n
		}
		return encodePackStreamList(items)
	case []float64:
		items := make([]any, len(val))
		for i, n := range val {
			items[i] = n
		}
		return encodePackStreamList(items)
	case []float32:
		items := make([]any, len(val))
		for i, n := range val {
			items[i] = float64(n)
		}
		return encodePackStreamList(items)
	case []map[string]any:
		items := make([]any, len(val))
		for i, m := range val {
			items[i] = m
		}
		return encodePackStreamList(items)
	// Map types
	case map[string]any:
		// Check if this is a node (has _nodeId and labels)
		if nodeId, hasNodeId := val["_nodeId"]; hasNodeId {
			if labels, hasLabels := val["labels"]; hasLabels {
				return encodeNode(nodeId, labels, val)
			}
		}
		return encodePackStreamMap(val)
	// Storage types - Neo4j compatible node/edge encoding
	case *storage.Node:
		if val == nil {
			return []byte{0xC0}
		}
		return encodeStorageNode(val)
	case *storage.Edge:
		if val == nil {
			return []byte{0xC0}
		}
		return encodeStorageEdge(val)
	default:
		// Unknown type - encode as null
		return []byte{0xC0}
	}
}

func encodePackStreamIntInto(dst []byte, val int64) []byte {
	// Tiny int: -16 to 127 (inline, 1 byte)
	if val >= -16 && val <= 127 {
		return append(dst, byte(val))
	}
	// INT8: -128 to -17 (marker + 1 byte)
	if val >= -128 && val < -16 {
		return append(dst, 0xC8, byte(val))
	}
	// INT16: -32768 to 32767 (marker + 2 bytes)
	if val >= -32768 && val <= 32767 {
		return append(dst, 0xC9, byte(val>>8), byte(val))
	}
	// INT32: -2147483648 to 2147483647
	if val >= -2147483648 && val <= 2147483647 {
		return append(dst, 0xCA, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	}
	// INT64: everything else
	return append(dst, 0xCB,
		byte(val>>56), byte(val>>48), byte(val>>40), byte(val>>32),
		byte(val>>24), byte(val>>16), byte(val>>8), byte(val),
	)
}

// encodeNodeInto encodes a map-based Cypher node as a proper Bolt Node structure (signature 0x4E).
// Format: STRUCT(3 fields, signature 0x4E) + id + labels + properties
func encodeNodeInto(dst []byte, nodeId any, labels any, props map[string]any) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	dst = append(dst, 0xB3, 0x4E)

	// Field 1: Node ID (as int64 for Neo4j compatibility)
	switch idVal := nodeId.(type) {
	case int64:
		dst = encodePackStreamIntInto(dst, idVal)
	case int:
		dst = encodePackStreamIntInto(dst, int64(idVal))
	case string:
		dst = encodePackStreamIntInto(dst, hashStringToInt64(idVal))
	default:
		dst = encodePackStreamIntInto(dst, 0)
	}

	// Field 2: Labels (list of strings)
	switch l := labels.(type) {
	case []string:
		if len(l) == 0 {
			dst = append(dst, 0x90)
		} else if len(l) < 16 {
			dst = append(dst, byte(0x90+len(l)))
			for _, s := range l {
				dst = encodePackStreamStringInto(dst, s)
			}
		} else if len(l) < 256 {
			dst = append(dst, 0xD4, byte(len(l)))
			for _, s := range l {
				dst = encodePackStreamStringInto(dst, s)
			}
		} else {
			dst = append(dst, 0xD5, byte(len(l)>>8), byte(len(l)))
			for _, s := range l {
				dst = encodePackStreamStringInto(dst, s)
			}
		}
	case []any:
		dst = encodePackStreamListInto(dst, l)
	default:
		dst = append(dst, 0x90)
	}

	// Field 3: Properties (map), skipping internal fields
	propCount := 0
	for k := range props {
		if k == "_nodeId" || k == "labels" {
			continue
		}
		propCount++
	}

	if propCount == 0 {
		dst = append(dst, 0xA0)
		return dst
	}

	if propCount < 16 {
		dst = append(dst, byte(0xA0+propCount))
	} else if propCount < 256 {
		dst = append(dst, 0xD8, byte(propCount))
	} else {
		dst = append(dst, 0xD9, byte(propCount>>8), byte(propCount))
	}

	for k, v := range props {
		if k == "_nodeId" || k == "labels" {
			continue
		}
		dst = encodePackStreamStringInto(dst, k)
		dst = encodePackStreamValueInto(dst, v)
	}

	return dst
}

func encodeStorageNodeInto(dst []byte, node *storage.Node) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	dst = append(dst, 0xB3, 0x4E)

	// Field 1: Node ID (as int64)
	dst = encodePackStreamIntInto(dst, hashStringToInt64(string(node.ID)))

	// Field 2: Labels (list of strings)
	labels := node.Labels
	if len(labels) == 0 {
		dst = append(dst, 0x90)
	} else if len(labels) < 16 {
		dst = append(dst, byte(0x90+len(labels)))
		for _, s := range labels {
			dst = encodePackStreamStringInto(dst, s)
		}
	} else if len(labels) < 256 {
		dst = append(dst, 0xD4, byte(len(labels)))
		for _, s := range labels {
			dst = encodePackStreamStringInto(dst, s)
		}
	} else {
		dst = append(dst, 0xD5, byte(len(labels)>>8), byte(len(labels)))
		for _, s := range labels {
			dst = encodePackStreamStringInto(dst, s)
		}
	}

	// Field 3: Properties (map)
	props := node.Properties
	if len(props) == 0 {
		return append(dst, 0xA0)
	}

	propCount := len(props)
	if propCount < 16 {
		dst = append(dst, byte(0xA0+propCount))
	} else if propCount < 256 {
		dst = append(dst, 0xD8, byte(propCount))
	} else {
		dst = append(dst, 0xD9, byte(propCount>>8), byte(propCount))
	}
	for k, v := range props {
		dst = encodePackStreamStringInto(dst, k)
		dst = encodePackStreamValueInto(dst, v)
	}
	return dst
}

func encodeStorageEdgeInto(dst []byte, edge *storage.Edge) []byte {
	// Bolt Relationship structure: B5 52 (tiny struct, 5 fields, signature 'R')
	dst = append(dst, 0xB5, 0x52)

	// Field 1: Relationship ID (as int64)
	dst = encodePackStreamIntInto(dst, hashStringToInt64(string(edge.ID)))
	// Field 2: Start Node ID (as int64)
	dst = encodePackStreamIntInto(dst, hashStringToInt64(string(edge.StartNode)))
	// Field 3: End Node ID (as int64)
	dst = encodePackStreamIntInto(dst, hashStringToInt64(string(edge.EndNode)))
	// Field 4: Relationship Type (string)
	dst = encodePackStreamStringInto(dst, edge.Type)

	// Field 5: Properties (map)
	props := edge.Properties
	if len(props) == 0 {
		return append(dst, 0xA0)
	}

	propCount := len(props)
	if propCount < 16 {
		dst = append(dst, byte(0xA0+propCount))
	} else if propCount < 256 {
		dst = append(dst, 0xD8, byte(propCount))
	} else {
		dst = append(dst, 0xD9, byte(propCount>>8), byte(propCount))
	}
	for k, v := range props {
		dst = encodePackStreamStringInto(dst, k)
		dst = encodePackStreamValueInto(dst, v)
	}
	return dst
}

// hashStringToInt64 converts a string ID to an int64 for Neo4j Bolt protocol compatibility.
// NornicDB uses string IDs (UUIDs or custom strings), but Bolt protocol expects int64 IDs.
// This function uses FNV-1a hash algorithm for deterministic conversion with good distribution.
//
// FNV-1a (Fowler-Noll-Vo) is a fast, non-cryptographic hash function that:
//   - Produces deterministic results (same input = same output)
//   - Has good distribution (fewer collisions than simple multiplicative hashing)
//   - Is fast and suitable for high-frequency operations
//
// The hash uses FNV-1a 64-bit variant with offset basis 14695981039346656037.
//
// Example:
//
//	hashStringToInt64("user-123") => 1234567890123456789
//	hashStringToInt64("user-123") => 1234567890123456789 (same result)
func hashStringToInt64(s string) int64 {
	// FNV-1a 64-bit hash
	// Use uint64 for calculation to avoid overflow, then convert to int64
	const offsetBasis uint64 = 14695981039346656037
	const prime uint64 = 1099511628211

	hash := offsetBasis
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}

	// Convert to int64 and ensure positive value (Bolt protocol expects non-negative int64)
	result := int64(hash)
	if result < 0 {
		// If the high bit is set, mask it to ensure positive
		result = result & 0x7FFFFFFFFFFFFFFF
	}

	return result
}

// encodeNode encodes a node as a proper Bolt Node structure (signature 0x4E).
// This makes nodes compatible with Neo4j drivers that expect Node instances with .properties.
// Format: STRUCT(3 fields, signature 0x4E) + id + labels + properties
func encodeNode(nodeId any, labels any, nodeMap map[string]any) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	buf := []byte{0xB3, 0x4E}

	// Field 1: Node ID (as int64 for Neo4j compatibility)
	// Convert string ID to int64 using deterministic hash function
	var id int64
	switch v := nodeId.(type) {
	case string:
		id = hashStringToInt64(v)
	case int64:
		id = v
	case int:
		id = int64(v)
	default:
		// Fallback: convert to string and hash
		id = hashStringToInt64(fmt.Sprintf("%v", v))
	}
	buf = append(buf, encodePackStreamInt(id)...)

	// Field 2: Labels (list of strings)
	labelList := make([]any, 0)
	switch l := labels.(type) {
	case []string:
		for _, s := range l {
			labelList = append(labelList, s)
		}
	case []any:
		labelList = l
	}
	buf = append(buf, encodePackStreamList(labelList)...)

	// Field 3: Properties (map) - exclude internal fields
	props := make(map[string]any)
	for k, v := range nodeMap {
		// Skip internal fields
		if k == "_nodeId" || k == "labels" {
			continue
		}
		props[k] = v
	}
	buf = append(buf, encodePackStreamMap(props)...)

	return buf
}

// encodeStorageNode encodes a *storage.Node as a proper Bolt Node structure (signature 0x4E).
// This enables Neo4j drivers to receive nodes with .properties accessor.
// Format: STRUCT(3 fields, signature 0x4E) + id + labels + properties
func encodeStorageNode(node *storage.Node) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	buf := []byte{0xB3, 0x4E}

	// Field 1: Node ID (as int64 for Neo4j compatibility)
	// Convert string ID to int64 using deterministic hash function
	id := hashStringToInt64(string(node.ID))
	buf = append(buf, encodePackStreamInt(id)...)

	// Field 2: Labels (list of strings)
	labelList := make([]any, len(node.Labels))
	for i, label := range node.Labels {
		labelList[i] = label
	}
	buf = append(buf, encodePackStreamList(labelList)...)

	// Field 3: Properties (map)
	props := make(map[string]any)
	for k, v := range node.Properties {
		props[k] = v
	}
	buf = append(buf, encodePackStreamMap(props)...)

	return buf
}

// encodeStorageEdge encodes a *storage.Edge as a proper Bolt Relationship structure (signature 0x52).
// This enables Neo4j drivers to receive relationships with proper structure.
// Format: STRUCT(5 fields, signature 0x52) + id + startNodeId + endNodeId + type + properties
func encodeStorageEdge(edge *storage.Edge) []byte {
	// Bolt Relationship structure: B5 52 (tiny struct, 5 fields, signature 'R')
	buf := []byte{0xB5, 0x52}

	// Field 1: Relationship ID (as int64)
	// Convert string ID to int64 using deterministic hash function
	id := hashStringToInt64(string(edge.ID))
	buf = append(buf, encodePackStreamInt(id)...)

	// Field 2: Start Node ID (as int64)
	// Convert string ID to int64 using deterministic hash function
	startId := hashStringToInt64(string(edge.StartNode))
	buf = append(buf, encodePackStreamInt(startId)...)

	// Field 3: End Node ID (as int64)
	// Convert string ID to int64 using deterministic hash function
	endId := hashStringToInt64(string(edge.EndNode))
	buf = append(buf, encodePackStreamInt(endId)...)

	// Field 4: Relationship Type (string)
	buf = append(buf, encodePackStreamString(edge.Type)...)

	// Field 5: Properties (map)
	props := make(map[string]any)
	for k, v := range edge.Properties {
		props[k] = v
	}
	buf = append(buf, encodePackStreamMap(props)...)

	return buf
}

// encodePackStreamInt encodes an integer using the smallest PackStream representation.
//
// PackStream integer encoding (from Bolt spec):
//   - Tiny: -16 to 127 (1 byte, inline)
//   - INT8: -128 to -17 (2 bytes, marker 0xC8)
//   - INT16: -32768 to 32767 (3 bytes, marker 0xC9)
//   - INT32: -2147483648 to 2147483647 (5 bytes, marker 0xCA)
//   - INT64: all other values (9 bytes, marker 0xCB)
//
// JavaScript Driver Behavior:
//   - Tiny, INT8, INT16, INT32 → decoded as JavaScript Number
//   - INT64 (0xCB) → decoded as JavaScript BigInt
//
// For Neo4j compatibility, we MUST use INT32 or smaller for values
// within JavaScript's safe integer range to avoid BigInt conversion issues.
//
// JavaScript safe integer range: -2^53 to 2^53 (-9007199254740991 to 9007199254740991)
// INT32 range: -2^31 to 2^31-1 (-2147483648 to 2147483647)
//
// Since INT32 range is within JS safe range, using INT32 encoding ensures
// the Neo4j JS driver will return regular Numbers, not BigInts.
func encodePackStreamInt(val int64) []byte {
	// Tiny int: -16 to 127 (inline, 1 byte)
	if val >= -16 && val <= 127 {
		return []byte{byte(val)}
	}
	// INT8: -128 to -17 (marker + 1 byte)
	if val >= -128 && val < -16 {
		return []byte{0xC8, byte(val)}
	}
	// INT16: -32768 to 32767 (marker + 2 bytes)
	if val >= -32768 && val <= 32767 {
		return []byte{0xC9, byte(val >> 8), byte(val)}
	}
	// INT32: -2147483648 to 2147483647 (marker + 4 bytes)
	// This is the largest encoding that Neo4j JS driver decodes as Number (not BigInt)
	if val >= -2147483648 && val <= 2147483647 {
		return []byte{0xCA, byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
	}
	// INT64: everything else (marker + 8 bytes)
	// Neo4j JS driver will decode this as BigInt
	return []byte{0xCB, byte(val >> 56), byte(val >> 48), byte(val >> 40), byte(val >> 32),
		byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
}

// ============================================================================
// PackStream Decoding
// ============================================================================

func decodePackStreamString(data []byte, offset int) (string, int, error) {
	if offset >= len(data) {
		return "", 0, fmt.Errorf("offset out of bounds")
	}

	startOffset := offset
	marker := data[offset]
	offset++

	var length int

	// Tiny string (0x80-0x8F)
	if marker >= 0x80 && marker <= 0x8F {
		length = int(marker - 0x80)
	} else if marker == 0xD0 { // STRING8
		if offset >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING8")
		}
		length = int(data[offset])
		offset++
	} else if marker == 0xD1 { // STRING16
		if offset+1 >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING16")
		}
		length = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else if marker == 0xD2 { // STRING32
		if offset+3 >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING32")
		}
		length = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
	} else {
		return "", 0, fmt.Errorf("not a string marker: 0x%02X", marker)
	}

	if offset+length > len(data) {
		return "", 0, fmt.Errorf("string data out of bounds")
	}

	str := string(data[offset : offset+length])
	return str, (offset + length) - startOffset, nil
}

func decodePackStreamMap(data []byte, offset int) (map[string]any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]
	startOffset := offset
	offset++

	var size int

	// Tiny map (0xA0-0xAF)
	if marker >= 0xA0 && marker <= 0xAF {
		size = int(marker - 0xA0)
	} else if marker == 0xD8 { // MAP8
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("incomplete MAP8")
		}
		size = int(data[offset])
		offset++
	} else if marker == 0xD9 { // MAP16
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete MAP16")
		}
		size = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else {
		return nil, 0, fmt.Errorf("not a map marker: 0x%02X", marker)
	}

	result := make(map[string]any)

	for i := 0; i < size; i++ {
		// Decode key (must be string)
		key, n, err := decodePackStreamString(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode map key: %w", err)
		}
		offset += n

		// Decode value
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode map value for key %s: %w", key, err)
		}
		offset += n

		result[key] = value
	}

	return result, offset - startOffset, nil
}

func decodePackStreamValue(data []byte, offset int) (any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]

	// Null
	if marker == 0xC0 {
		return nil, 1, nil
	}

	// Boolean
	if marker == 0xC2 {
		return false, 1, nil
	}
	if marker == 0xC3 {
		return true, 1, nil
	}

	// Tiny positive int (0x00-0x7F)
	if marker <= 0x7F {
		return int64(marker), 1, nil
	}

	// Tiny negative int (0xF0-0xFF = -16 to -1)
	if marker >= 0xF0 {
		return int64(int8(marker)), 1, nil
	}

	// INT8
	if marker == 0xC8 {
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT8")
		}
		return int64(int8(data[offset+1])), 2, nil
	}

	// INT16
	if marker == 0xC9 {
		if offset+2 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT16")
		}
		val := int16(data[offset+1])<<8 | int16(data[offset+2])
		return int64(val), 3, nil
	}

	// INT32
	if marker == 0xCA {
		if offset+4 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT32")
		}
		val := int32(data[offset+1])<<24 | int32(data[offset+2])<<16 | int32(data[offset+3])<<8 | int32(data[offset+4])
		return int64(val), 5, nil
	}

	// INT64
	if marker == 0xCB {
		if offset+8 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT64")
		}
		val := int64(data[offset+1])<<56 | int64(data[offset+2])<<48 | int64(data[offset+3])<<40 | int64(data[offset+4])<<32 |
			int64(data[offset+5])<<24 | int64(data[offset+6])<<16 | int64(data[offset+7])<<8 | int64(data[offset+8])
		return val, 9, nil
	}

	// Float64
	if marker == 0xC1 {
		if offset+8 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete Float64")
		}
		bits := binary.BigEndian.Uint64(data[offset+1 : offset+9])
		return math.Float64frombits(bits), 9, nil
	}

	// Bytes
	if marker == 0xCC || marker == 0xCD || marker == 0xCE {
		var size int
		var headerLen int
		switch marker {
		case 0xCC:
			if offset+1 >= len(data) {
				return nil, 0, fmt.Errorf("incomplete BYTES8")
			}
			size = int(data[offset+1])
			headerLen = 2
		case 0xCD:
			if offset+2 >= len(data) {
				return nil, 0, fmt.Errorf("incomplete BYTES16")
			}
			size = int(data[offset+1])<<8 | int(data[offset+2])
			headerLen = 3
		case 0xCE:
			if offset+4 >= len(data) {
				return nil, 0, fmt.Errorf("incomplete BYTES32")
			}
			size = int(data[offset+1])<<24 | int(data[offset+2])<<16 | int(data[offset+3])<<8 | int(data[offset+4])
			headerLen = 5
		}

		start := offset + headerLen
		end := start + size
		if end > len(data) {
			return nil, 0, fmt.Errorf("incomplete BYTES payload")
		}
		out := make([]byte, size)
		copy(out, data[start:end])
		return out, headerLen + size, nil
	}

	// String
	if marker >= 0x80 && marker <= 0x8F || marker == 0xD0 || marker == 0xD1 || marker == 0xD2 {
		return decodePackStreamString(data, offset)
	}

	// List
	if marker >= 0x90 && marker <= 0x9F || marker == 0xD4 || marker == 0xD5 || marker == 0xD6 {
		return decodePackStreamList(data, offset)
	}

	// Map
	if marker >= 0xA0 && marker <= 0xAF || marker == 0xD8 || marker == 0xD9 || marker == 0xDA {
		return decodePackStreamMap(data, offset)
	}

	// Structure (for nodes, relationships, paths, etc.)
	// Format: [marker] [signature] [field1] [field2] ...
	// Tiny structures: 0xB0-0xBF (0-15 fields)
	// Larger structures: 0xDC (STRUCT8), 0xDD (STRUCT16)
	if marker >= 0xB0 && marker <= 0xBF {
		return decodePackStreamStructure(data, offset)
	}

	// STRUCT8: 0xDC [size: 1 byte] [signature: 1 byte] [fields...]
	if marker == 0xDC {
		if offset+2 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete STRUCT8")
		}
		size := int(data[offset+1])
		signature := data[offset+2]
		fieldOffset := offset + 3
		result, fieldsConsumed, err := decodeStructureFields(data, fieldOffset, size, signature)
		if err != nil {
			return nil, 0, err
		}
		// Total consumed: marker (1) + size (1) + signature (1) + fields
		return result, 1 + 1 + 1 + fieldsConsumed, nil
	}

	// STRUCT16: 0xDD [size: 2 bytes] [signature: 1 byte] [fields...]
	if marker == 0xDD {
		if offset+4 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete STRUCT16")
		}
		size := int(data[offset+1])<<8 | int(data[offset+2])
		signature := data[offset+3]
		fieldOffset := offset + 4
		result, fieldsConsumed, err := decodeStructureFields(data, fieldOffset, size, signature)
		if err != nil {
			return nil, 0, err
		}
		// Total consumed: marker (1) + size (2) + signature (1) + fields
		return result, 1 + 2 + 1 + fieldsConsumed, nil
	}

	return nil, 0, fmt.Errorf("unknown marker: 0x%02X", marker)
}

// decodePackStreamStructure decodes a tiny structure (0xB0-0xBF).
// Format: [marker] [signature] [field1] [field2] ...
func decodePackStreamStructure(data []byte, offset int) (any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]

	// Extract field count from marker (0xB0 = 0 fields, 0xB1 = 1 field, etc.)
	fieldCount := int(marker - 0xB0)
	offset++

	// Read signature byte
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("incomplete structure: missing signature")
	}
	signature := data[offset]
	offset++

	// Decode fields based on signature
	// Note: offset already accounts for marker (1 byte) and signature (1 byte)
	result, fieldsConsumed, err := decodeStructureFields(data, offset, fieldCount, signature)
	if err != nil {
		return nil, 0, err
	}
	// Total consumed: marker (1) + signature (1) + fields
	return result, 1 + 1 + fieldsConsumed, nil
}

// decodeStructureFields decodes structure fields based on signature.
// Common signatures:
//
//	0x4E (N) = Node: [id, labels, properties]
//	0x52 (R) = Relationship: [id, startNodeId, endNodeId, type, properties]
//	0x50 (P) = Path: [nodes, relationships, sequence]
//	Other signatures are decoded as generic structures (map with fields)
func decodeStructureFields(data []byte, offset int, fieldCount int, signature byte) (any, int, error) {
	startOffset := offset
	fields := make([]any, fieldCount)

	// Decode all fields
	for i := 0; i < fieldCount; i++ {
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode structure field %d: %w", i, err)
		}
		fields[i] = value
		offset += n
	}

	fieldsConsumed := offset - startOffset

	// Convert to appropriate type based on signature
	switch signature {
	case 0x4E: // Node (N)
		// Node structure: [id, labels, properties]
		if fieldCount >= 3 {
			return map[string]any{
				"_type":      "Node",
				"id":         fields[0],
				"labels":     fields[1],
				"properties": fields[2],
			}, fieldsConsumed, nil
		}
		return map[string]any{"_type": "Node", "fields": fields}, fieldsConsumed, nil

	case 0x52: // Relationship (R)
		// Relationship structure: [id, startNodeId, endNodeId, type, properties]
		if fieldCount >= 5 {
			return map[string]any{
				"_type":       "Relationship",
				"id":          fields[0],
				"startNodeId": fields[1],
				"endNodeId":   fields[2],
				"type":        fields[3],
				"properties":  fields[4],
			}, fieldsConsumed, nil
		}
		return map[string]any{"_type": "Relationship", "fields": fields}, fieldsConsumed, nil

	case 0x50: // Path (P)
		// Path structure: [nodes, relationships, sequence]
		if fieldCount >= 3 {
			return map[string]any{
				"_type":         "Path",
				"nodes":         fields[0],
				"relationships": fields[1],
				"sequence":      fields[2],
			}, fieldsConsumed, nil
		}
		return map[string]any{"_type": "Path", "fields": fields}, fieldsConsumed, nil

	default:
		// Generic structure - return as map with signature and fields
		result := map[string]any{
			"_type":     fmt.Sprintf("Structure_0x%02X", signature),
			"signature": signature,
			"fields":    fields,
		}
		return result, fieldsConsumed, nil
	}
}

func decodePackStreamList(data []byte, offset int) ([]any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]
	startOffset := offset
	offset++

	var size int

	// Tiny list (0x90-0x9F)
	if marker >= 0x90 && marker <= 0x9F {
		size = int(marker - 0x90)
	} else if marker == 0xD4 { // LIST8
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST8")
		}
		size = int(data[offset])
		offset++
	} else if marker == 0xD5 { // LIST16
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST16")
		}
		size = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else if marker == 0xD6 { // LIST32
		if offset+3 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST32")
		}
		size = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
	} else {
		return nil, 0, fmt.Errorf("not a list marker: 0x%02X", marker)
	}

	result := make([]any, size)

	for i := 0; i < size; i++ {
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode list item %d: %w", i, err)
		}
		result[i] = value
		offset += n
	}

	return result, offset - startOffset, nil
}
