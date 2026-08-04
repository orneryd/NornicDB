// Package bolt implements the Neo4j Bolt protocol server for NornicDB.
package bolt

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

var packstreamZero8 [8]byte
var packstreamFallbackHook func(v any)

// ============================================================================
// PackStream Encoding
// ============================================================================

func encodePackStreamMapInto(dst []byte, m map[string]any) []byte {
	return encodePackStreamMapIntoWithUTC(dst, m, true)
}

func encodePackStreamMapIntoWithUTC(dst []byte, m map[string]any, useUTCDateTimeStructs bool) []byte {
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
		dst = encodePackStreamValueIntoWithUTC(dst, v, useUTCDateTimeStructs)
	}

	return dst
}

func encodePackStreamMap(m map[string]any) []byte {
	return encodePackStreamMapInto(nil, m)
}

func encodePackStreamListInto(dst []byte, items []any) []byte {
	return encodePackStreamListIntoWithUTC(dst, items, true)
}

func encodePackStreamListIntoWithUTC(dst []byte, items []any, useUTCDateTimeStructs bool) []byte {
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
		dst = encodePackStreamValueIntoWithUTC(dst, item, useUTCDateTimeStructs)
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
	return encodePackStreamValueIntoWithUTC(dst, v, true)
}

func encodePackStreamValueIntoWithUTC(dst []byte, v any, useUTCDateTimeStructs bool) []byte {
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
		if path, ok := extractPathFromMap(val); ok {
			return encodePathIntoWithUTC(dst, path.Nodes, path.Relationships, useUTCDateTimeStructs)
		}
		// Check if this is a node (has _nodeId and labels)
		if nodeId, hasNodeId := val["_nodeId"]; hasNodeId {
			if labels, hasLabels := val["labels"]; hasLabels {
				return encodeNodeIntoWithUTC(dst, nodeId, labels, val, useUTCDateTimeStructs)
			}
		}
		return encodePackStreamMapIntoWithUTC(dst, val, useUTCDateTimeStructs)
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
		return encodeStorageNodeIntoWithUTC(dst, &val, useUTCDateTimeStructs)
	case *storage.Node:
		if val == nil {
			return append(dst, 0xC0)
		}
		return encodeStorageNodeIntoWithUTC(dst, val, useUTCDateTimeStructs)
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
			dst = encodeStorageNodeIntoWithUTC(dst, &val[i], useUTCDateTimeStructs)
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
			dst = encodePackStreamValueIntoWithUTC(dst, n, useUTCDateTimeStructs)
		}
		return dst
	case storage.Edge:
		return encodeStorageEdgeIntoWithUTC(dst, &val, useUTCDateTimeStructs)
	case *storage.Edge:
		if val == nil {
			return append(dst, 0xC0)
		}
		return encodeStorageEdgeIntoWithUTC(dst, val, useUTCDateTimeStructs)
	case unboundRelationship:
		return encodeUnboundRelationshipInto(dst, &val)
	case *unboundRelationship:
		if val == nil {
			return append(dst, 0xC0)
		}
		return encodeUnboundRelationshipInto(dst, val)
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
			dst = encodeStorageEdgeIntoWithUTC(dst, &val[i], useUTCDateTimeStructs)
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
			dst = encodePackStreamValueIntoWithUTC(dst, e, useUTCDateTimeStructs)
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
		return encodePackStreamListIntoWithUTC(dst, val, useUTCDateTimeStructs)
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
			dst = encodePackStreamMapIntoWithUTC(dst, m, useUTCDateTimeStructs)
		}
		return dst
	case time.Time:
		return encodePackStreamDateTimeIntoWithUTC(dst, val, useUTCDateTimeStructs)
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
	case time.Time:
		return encodePackStreamDateTime(val)
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

func encodePackStreamDateTimeInto(dst []byte, t time.Time) []byte {
	return encodePackStreamDateTimeIntoWithUTC(dst, t, true)
}

func encodePackStreamDateTimeIntoWithUTC(dst []byte, t time.Time, useUTCDateTimeStructs bool) []byte {
	_, offsetSeconds := t.Zone()
	sig := byte(0x49)
	if !useUTCDateTimeStructs {
		sig = 0x46
	}
	dst = append(dst, 0xB3, sig)
	dst = encodePackStreamIntInto(dst, t.Unix())
	dst = encodePackStreamIntInto(dst, int64(t.Nanosecond()))
	dst = encodePackStreamIntInto(dst, int64(offsetSeconds))
	return dst
}

func encodePackStreamDateTime(t time.Time) []byte {
	return encodePackStreamDateTimeInto(nil, t)
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
	return encodeNodeIntoWithUTC(dst, nodeId, labels, props, true)
}

func encodeNodeIntoWithUTC(dst []byte, nodeId any, labels any, props map[string]any, useUTCDateTimeStructs bool) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	dst = append(dst, 0xB3, 0x4E)

	// Field 1: Node ID (as int64 for Neo4j compatibility)
	switch idVal := nodeId.(type) {
	case int64:
		dst = encodePackStreamIntInto(dst, idVal)
	case int:
		dst = encodePackStreamIntInto(dst, int64(idVal))
	case string:
		dst = encodePackStreamIntInto(dst, util.HashStringToInt64(idVal))
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
		dst = encodePackStreamListIntoWithUTC(dst, l, useUTCDateTimeStructs)
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
		dst = encodePackStreamValueIntoWithUTC(dst, v, useUTCDateTimeStructs)
	}

	return dst
}

func encodeStorageNodeInto(dst []byte, node *storage.Node) []byte {
	return encodeStorageNodeIntoWithUTC(dst, node, true)
}

func encodeStorageNodeIntoWithUTC(dst []byte, node *storage.Node, useUTCDateTimeStructs bool) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	dst = append(dst, 0xB3, 0x4E)

	// Field 1: Node ID (as int64)
	dst = encodePackStreamIntInto(dst, util.HashStringToInt64(string(node.ID)))

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
		dst = encodePackStreamValueIntoWithUTC(dst, v, useUTCDateTimeStructs)
	}
	return dst
}

func encodeStorageEdgeInto(dst []byte, edge *storage.Edge) []byte {
	return encodeStorageEdgeIntoWithUTC(dst, edge, true)
}

func encodeStorageEdgeIntoWithUTC(dst []byte, edge *storage.Edge, useUTCDateTimeStructs bool) []byte {
	// Bolt Relationship structure: B5 52 (tiny struct, 5 fields, signature 'R')
	dst = append(dst, 0xB5, 0x52)

	// Field 1: Relationship ID (as int64)
	dst = encodePackStreamIntInto(dst, util.HashStringToInt64(string(edge.ID)))
	// Field 2: Start Node ID (as int64)
	dst = encodePackStreamIntInto(dst, util.HashStringToInt64(string(edge.StartNode)))
	// Field 3: End Node ID (as int64)
	dst = encodePackStreamIntInto(dst, util.HashStringToInt64(string(edge.EndNode)))
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
		dst = encodePackStreamValueIntoWithUTC(dst, v, useUTCDateTimeStructs)
	}
	return dst
}

type unboundRelationship struct {
	id         storage.EdgeID
	relType    string
	properties map[string]any
}

func encodeUnboundRelationshipInto(dst []byte, rel *unboundRelationship) []byte {
	return encodeUnboundRelationshipIntoWithUTC(dst, rel, true)
}

func encodeUnboundRelationshipIntoWithUTC(dst []byte, rel *unboundRelationship, useUTCDateTimeStructs bool) []byte {
	// Bolt Unbound Relationship structure: B3 72 (tiny struct, 3 fields, signature 'r')
	dst = append(dst, 0xB3, 0x72)
	dst = encodePackStreamIntInto(dst, util.HashStringToInt64(string(rel.id)))
	dst = encodePackStreamStringInto(dst, rel.relType)

	if len(rel.properties) == 0 {
		return append(dst, 0xA0)
	}
	return encodePackStreamMapIntoWithUTC(dst, rel.properties, useUTCDateTimeStructs)
}

func encodePathInto(dst []byte, pathNodes []*storage.Node, pathRels []*storage.Edge) []byte {
	return encodePathIntoWithUTC(dst, pathNodes, pathRels, true)
}

func encodePathIntoWithUTC(dst []byte, pathNodes []*storage.Node, pathRels []*storage.Edge, useUTCDateTimeStructs bool) []byte {
	// Bolt Path structure: B3 50 (tiny struct, 3 fields, signature 'P')
	dst = append(dst, 0xB3, 0x50)

	if len(pathNodes) == 0 {
		// Invalid path; encode empty lists to avoid malformed structures.
		dst = append(dst, 0x90, 0x90, 0x90)
		return dst
	}

	if len(pathRels) == 0 {
		// Path length 0 should contain only the first node.
		dst = encodePackStreamListIntoWithUTC(dst, []any{pathNodes[0]}, useUTCDateTimeStructs)
		dst = append(dst, 0x90, 0x90)
		return dst
	}

	uniqueNodes, nodeIndex := uniquePathNodes(pathNodes)
	uniqueRels, relIndex := uniquePathRels(pathRels)

	// Field 1: Nodes list
	dst = encodePackStreamValueIntoWithUTC(dst, uniqueNodes, useUTCDateTimeStructs)

	// Field 2: Unbound relationships list
	rels := make([]any, len(uniqueRels))
	for i, rel := range uniqueRels {
		rels[i] = unboundRelationship{
			id:         rel.ID,
			relType:    rel.Type,
			properties: rel.Properties,
		}
	}
	dst = encodePackStreamListIntoWithUTC(dst, rels, useUTCDateTimeStructs)

	// Field 3: Sequence list
	sequence := buildPathSequence(pathNodes, pathRels, nodeIndex, relIndex)
	dst = encodePackStreamValueIntoWithUTC(dst, sequence, useUTCDateTimeStructs)

	return dst
}

func uniquePathNodes(nodes []*storage.Node) ([]*storage.Node, map[storage.NodeID]int) {
	if len(nodes) == 0 {
		return nil, map[storage.NodeID]int{}
	}
	unique := make([]*storage.Node, 0, len(nodes))
	index := make(map[storage.NodeID]int, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if _, ok := index[node.ID]; ok {
			continue
		}
		index[node.ID] = len(unique)
		unique = append(unique, node)
	}
	return unique, index
}

func uniquePathRels(rels []*storage.Edge) ([]*storage.Edge, map[storage.EdgeID]int) {
	if len(rels) == 0 {
		return nil, map[storage.EdgeID]int{}
	}
	unique := make([]*storage.Edge, 0, len(rels))
	index := make(map[storage.EdgeID]int, len(rels))
	for _, rel := range rels {
		if rel == nil {
			continue
		}
		if _, ok := index[rel.ID]; ok {
			continue
		}
		index[rel.ID] = len(unique)
		unique = append(unique, rel)
	}
	return unique, index
}

func buildPathSequence(pathNodes []*storage.Node, pathRels []*storage.Edge, nodeIndex map[storage.NodeID]int, relIndex map[storage.EdgeID]int) []int64 {
	if len(pathNodes) == 0 || len(pathRels) == 0 {
		return []int64{}
	}

	if len(pathNodes) < len(pathRels)+1 {
		return []int64{}
	}

	sequence := make([]int64, 0, len(pathRels)*2)
	current := pathNodes[0]
	for i := 0; i < len(pathRels); i++ {
		rel := pathRels[i]
		next := pathNodes[i+1]
		if rel == nil || current == nil || next == nil {
			return []int64{}
		}
		relPos, ok := relIndex[rel.ID]
		if !ok {
			return []int64{}
		}
		nodePos, ok := nodeIndex[next.ID]
		if !ok {
			return []int64{}
		}

		relSeq := int64(relPos + 1)
		if current.ID != rel.StartNode {
			relSeq = -relSeq
		}

		sequence = append(sequence, relSeq, int64(nodePos))
		current = next
	}
	return sequence
}

func extractPathFromMap(val map[string]any) (*cypher.PathResult, bool) {
	raw, ok := val["_pathResult"]
	if !ok {
		return nil, false
	}

	switch path := raw.(type) {
	case cypher.PathResult:
		return &path, true
	case *cypher.PathResult:
		if path != nil {
			return path, true
		}
	}

	nodes := coercePathNodes(val["nodes"])
	rels := coercePathRels(val["relationships"])
	if len(rels) == 0 {
		rels = coercePathRels(val["rels"])
	}
	if len(nodes) == 0 && len(rels) == 0 {
		return nil, false
	}

	return &cypher.PathResult{
		Nodes:         nodes,
		Relationships: rels,
		Length:        len(rels),
	}, true
}

func coercePathNodes(val any) []*storage.Node {
	switch nodes := val.(type) {
	case []*storage.Node:
		return nodes
	case []storage.Node:
		converted := make([]*storage.Node, 0, len(nodes))
		for i := range nodes {
			node := nodes[i]
			converted = append(converted, &node)
		}
		return converted
	case []any:
		converted := make([]*storage.Node, 0, len(nodes))
		for _, item := range nodes {
			switch node := item.(type) {
			case *storage.Node:
				converted = append(converted, node)
			case storage.Node:
				n := node
				converted = append(converted, &n)
			}
		}
		return converted
	default:
		return nil
	}
}

func coercePathRels(val any) []*storage.Edge {
	switch rels := val.(type) {
	case []*storage.Edge:
		return rels
	case []storage.Edge:
		converted := make([]*storage.Edge, 0, len(rels))
		for i := range rels {
			rel := rels[i]
			converted = append(converted, &rel)
		}
		return converted
	case []any:
		converted := make([]*storage.Edge, 0, len(rels))
		for _, item := range rels {
			switch rel := item.(type) {
			case *storage.Edge:
				converted = append(converted, rel)
			case storage.Edge:
				r := rel
				converted = append(converted, &r)
			}
		}
		return converted
	default:
		return nil
	}
}

// encodeNode encodes a node as a proper Bolt Node structure (signature 0x4E).
// This makes nodes compatible with Neo4j drivers that expect Node instances with .properties.
// Format: STRUCT(3 fields, signature 0x4E) + id + labels + properties
func encodeNode(nodeId any, labels any, nodeMap map[string]any) []byte {
	return encodeNodeWithUTC(nodeId, labels, nodeMap, true)
}

func encodeNodeWithUTC(nodeId any, labels any, nodeMap map[string]any, useUTCDateTimeStructs bool) []byte {
	// Bolt Node structure: B3 4E (tiny struct, 3 fields, signature 'N')
	buf := []byte{0xB3, 0x4E}

	// Field 1: Node ID (as int64 for Neo4j compatibility)
	// Convert string ID to int64 using deterministic hash function
	var id int64
	switch v := nodeId.(type) {
	case string:
		id = util.HashStringToInt64(v)
	case int64:
		id = v
	case int:
		id = int64(v)
	default:
		// Fallback: convert to string and hash
		id = util.HashStringToInt64(fmt.Sprintf("%v", v))
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
	buf = encodePackStreamListIntoWithUTC(buf, labelList, useUTCDateTimeStructs)

	// Field 3: Properties (map) - exclude internal fields
	props := make(map[string]any)
	for k, v := range nodeMap {
		// Skip internal fields
		if k == "_nodeId" || k == "labels" {
			continue
		}
		props[k] = v
	}
	buf = encodePackStreamMapIntoWithUTC(buf, props, useUTCDateTimeStructs)

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
	id := util.HashStringToInt64(string(node.ID))
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
	id := util.HashStringToInt64(string(edge.ID))
	buf = append(buf, encodePackStreamInt(id)...)

	// Field 2: Start Node ID (as int64)
	// Convert string ID to int64 using deterministic hash function
	startId := util.HashStringToInt64(string(edge.StartNode))
	buf = append(buf, encodePackStreamInt(startId)...)

	// Field 3: End Node ID (as int64)
	// Convert string ID to int64 using deterministic hash function
	endId := util.HashStringToInt64(string(edge.EndNode))
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

	case 0x49, 0x46: // DateTime (utc-patched and legacy): [seconds, nanos, offsetSeconds]
		if fieldCount >= 3 {
			sec, okSec := toInt64Field(fields[0])
			nsec, okNsec := toInt64Field(fields[1])
			offsetSec, okOffset := toInt64Field(fields[2])
			if okSec && okNsec && okOffset {
				loc := time.FixedZone("", int(offsetSec))
				return time.Unix(sec, nsec).In(loc), fieldsConsumed, nil
			}
		}
		return map[string]any{"_type": "DateTime", "fields": fields}, fieldsConsumed, nil

	case 0x64: // LocalDateTime: [seconds, nanos]
		if fieldCount >= 2 {
			sec, okSec := toInt64Field(fields[0])
			nsec, okNsec := toInt64Field(fields[1])
			if okSec && okNsec {
				// NornicDB's typed temporal param path is time.Time-based. Normalize
				// naive/local datetimes to UTC on ingress so they round-trip as a
				// hydratable DateTime instead of leaking an opaque 0x64 structure.
				return time.Unix(sec, nsec).UTC(), fieldsConsumed, nil
			}
		}
		return map[string]any{"_type": "LocalDateTime", "fields": fields}, fieldsConsumed, nil

	case 0x69, 0x66: // DateTime with zone id: [seconds, nanos, zoneId]
		if fieldCount >= 3 {
			sec, okSec := toInt64Field(fields[0])
			nsec, okNsec := toInt64Field(fields[1])
			zoneID, okZone := fields[2].(string)
			if okSec && okNsec && okZone {
				loc, err := time.LoadLocation(zoneID)
				if err != nil {
					loc = time.UTC
				}
				return time.Unix(sec, nsec).In(loc), fieldsConsumed, nil
			}
		}
		return map[string]any{"_type": "DateTimeZoneId", "fields": fields}, fieldsConsumed, nil

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

func toInt64Field(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint:
		return int64(n), true
	default:
		return 0, false
	}
}

func decodePackStreamList(data []byte, offset int) ([]any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]
	startOffset := offset
	offset++

	var size uint64

	// Tiny list (0x90-0x9F)
	if marker >= 0x90 && marker <= 0x9F {
		size = uint64(marker - 0x90)
	} else if marker == 0xD4 { // LIST8
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST8")
		}
		size = uint64(data[offset])
		offset++
	} else if marker == 0xD5 { // LIST16
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST16")
		}
		size = uint64(data[offset])<<8 | uint64(data[offset+1])
		offset += 2
	} else if marker == 0xD6 { // LIST32
		if offset+3 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST32")
		}
		size = uint64(data[offset])<<24 | uint64(data[offset+1])<<16 | uint64(data[offset+2])<<8 | uint64(data[offset+3])
		offset += 4
	} else {
		return nil, 0, fmt.Errorf("not a list marker: 0x%02X", marker)
	}

	if size > uint64(len(data)-offset) {
		return nil, 0, fmt.Errorf("list data out of bounds")
	}

	validatedSize := int(size)
	result := make([]any, validatedSize)

	for i := 0; i < validatedSize; i++ {
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode list item %d: %w", i, err)
		}
		result[i] = value
		offset += n
	}

	return result, offset - startOffset, nil
}
