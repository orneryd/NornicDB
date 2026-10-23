package cypher

import (
	"reflect"
	"time"
	"unsafe"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Estimated sizes of the values a statement keeps (#1343), as Neo4j's
// HeapEstimator does: a fixed cost for the value's slot and header plus
// what it holds. A node or relationship in a row, list or group is a
// reference to the entity the storage layer and its caches own, as Neo4j's
// entity values are: its properties count where the statement copies them
// into values of its own (n.text, properties(n), a map projection).
const (
	valueSlotBytes        = 16 // an interface value
	stringHeaderBytes     = 16 // a string header
	sliceHeaderBytes      = 24 // a slice header
	mapHeaderBytes        = 48 // a map header and its first bucket
	mapEntryBytes         = 16 // a map entry's share of buckets, past its key and value
	entityReferenceBytes  = 64 // a node or relationship reference
	pathReferenceBytes    = 48 // a path's header, past its entity references
	unknownValueBytes     = 32 // any other value
	pipelineRowExtraBytes = 8  // a row's slot in its slice
	maxEstimateDepth      = 32 // nesting past which a value counts as unknown
)

// estimateValueBytes is the estimated size of a value a statement keeps.
func estimateValueBytes(walk *materializedRowCheckpoint, value interface{}) int64 {
	var total int64
	cypherfn.VisitValueDepth(walk, value, func(value interface{}, depth int) (descend, stop bool) {
		bytes, members := estimateValueOwnBytes(walk, value, depth)
		total += bytes
		return members, false
	})
	return total
}

// estimateValueOwnBytes is the estimate of one value itself, without its
// members, and whether its members count (a list or map within
// maxEstimateDepth): the value walker adds theirs.
func estimateValueOwnBytes(walk *materializedRowCheckpoint, value interface{}, depth int) (int64, bool) {
	if depth > maxEstimateDepth {
		return unknownValueBytes, false
	}
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return valueSlotBytes, false
	case string:
		return valueSlotBytes + stringHeaderBytes + int64(len(v)), false
	case []byte:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v)), false
	case []interface{}:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v)-len(v))*valueSlotBytes, true
	case []string:
		total := valueSlotBytes + sliceHeaderBytes + int64(cap(v))*stringHeaderBytes
		for _, element := range observedItems(walk, v) {
			total += int64(len(element))
		}
		return total, false
	case []int64:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*8, false
	case []float64:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*8, false
	case []float32:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*4, false
	case map[string]interface{}:
		total := int64(valueSlotBytes + mapHeaderBytes)
		for key := range observedEntries(walk, v) {
			total += mapEntryBytes + stringHeaderBytes + int64(len(key))
		}
		return total, true
	case *storage.Node, *storage.Edge, storage.Node, storage.Edge:
		return valueSlotBytes + entityReferenceBytes, false
	case []*storage.Node:
		return valueSlotBytes + sliceHeaderBytes + int64(len(v))*entityReferenceBytes, false
	case []*storage.Edge:
		return valueSlotBytes + sliceHeaderBytes + int64(len(v))*entityReferenceBytes, false
	case time.Time, time.Duration:
		return valueSlotBytes + 24, false
	default:
		return unknownValueBytes, false
	}
}

// rowEstimator sizes the binding rows one operator keeps, every row in
// full: a sampled estimate would undercount rows that grow late (long
// strings, lists built per row). A list or map that the rows of a column
// share counts once, the first time the operator sees it in that column:
// UNWIND documents AS d gives every row the same documents, and WITH and a
// CALL subquery's imports pass it on without copying it. A list or map
// built per row (range(), a list comprehension, a map projection) is a
// different one in every row and counts in full in each. Columns past the
// first rowEstimatorColumns count every list and map in full (an
// overcount, never an undercount).
type rowEstimator struct {
	columns [rowEstimatorColumns]struct {
		name     string
		identity unsafe.Pointer
	}
	count int
	// known sizes the lists collect() counted (valueBytes); nil walks them.
	known *transactionMemoryTracker
}

const rowEstimatorColumns = 8

// bytes is the estimated size of row.
func (r *rowEstimator) bytes(row pipelineRow) int64 {
	total := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range row {
		total += r.entry(name, value)
	}
	return total
}

// entry is the estimated size of one binding of a row.
func (r *rowEstimator) entry(name string, value interface{}) int64 {
	return mapEntryBytes + stringHeaderBytes + int64(len(name)) + r.value(name, value)
}

// value is the estimated size of a value in a row's column.
func (r *rowEstimator) value(column string, value interface{}) int64 {
	var identity unsafe.Pointer
	switch v := value.(type) {
	case []interface{}:
		identity = unsafe.Pointer(unsafe.SliceData(v))
	case []string:
		identity = unsafe.Pointer(unsafe.SliceData(v))
	case []int64:
		identity = unsafe.Pointer(unsafe.SliceData(v))
	case []float64:
		identity = unsafe.Pointer(unsafe.SliceData(v))
	case []float32:
		identity = unsafe.Pointer(unsafe.SliceData(v))
	case map[string]interface{}:
		if len(v) > 0 {
			identity = reflect.ValueOf(v).UnsafePointer()
		}
	default:
		return estimateValueBytes(nil, value)
	}
	if identity == nil {
		return estimateValueBytes(nil, value)
	}
	for index := 0; index < r.count; index++ {
		if r.columns[index].name == column {
			if r.columns[index].identity == identity {
				return valueSlotBytes + sliceHeaderBytes
			}
			r.columns[index].identity = identity
			return r.known.valueBytes(value)
		}
	}
	if r.count < rowEstimatorColumns {
		r.columns[r.count].name, r.columns[r.count].identity = column, identity
		r.count++
	}
	return r.known.valueBytes(value)
}

// estimateResultRowBytes is the estimated size of a result row; lists
// known's collect() counted are sized from what it counted.
func estimateResultRowBytes(row []interface{}, known *transactionMemoryTracker) int64 {
	total := int64(pipelineRowExtraBytes + sliceHeaderBytes)
	for _, value := range row {
		total += known.valueBytes(value)
	}
	return total
}

// estimatePathResultBytes is the estimated size of a matched path.
func estimatePathResultBytes(nodes, relationships int) int64 {
	return pathReferenceBytes + 2*sliceHeaderBytes + int64(nodes+relationships)*8
}

// aggregateStateBytes is the estimated size of one aggregation's running
// state in a group.
const aggregateStateBytes = 160
