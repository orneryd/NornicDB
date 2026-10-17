package cypher

import (
	"reflect"
	"time"
	"unsafe"

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
func estimateValueBytes(value interface{}) int64 {
	observeSizing()
	if estimateObserver != nil {
		estimateObserver()
	}
	return estimateValueBytesAt(value, 0)
}

func estimateValueBytesAt(value interface{}, depth int) int64 {
	if depth > maxEstimateDepth {
		return unknownValueBytes
	}
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return valueSlotBytes
	case string:
		return valueSlotBytes + stringHeaderBytes + int64(len(v))
	case []byte:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))
	case []interface{}:
		total := valueSlotBytes + sliceHeaderBytes + int64(cap(v)-len(v))*valueSlotBytes
		for _, element := range v {
			total += estimateValueBytesAt(element, depth+1)
		}
		return total
	case []string:
		total := valueSlotBytes + sliceHeaderBytes + int64(cap(v))*stringHeaderBytes
		for _, element := range v {
			total += int64(len(element))
		}
		return total
	case []int64:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*8
	case []float64:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*8
	case []float32:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*4
	case map[string]interface{}:
		total := int64(valueSlotBytes + mapHeaderBytes)
		for key, element := range v {
			total += mapEntryBytes + stringHeaderBytes + int64(len(key)) + estimateValueBytesAt(element, depth+1)
		}
		return total
	case *storage.Node, *storage.Edge, storage.Node, storage.Edge:
		return valueSlotBytes + entityReferenceBytes
	case []*storage.Node:
		return valueSlotBytes + sliceHeaderBytes + int64(len(v))*entityReferenceBytes
	case []*storage.Edge:
		return valueSlotBytes + sliceHeaderBytes + int64(len(v))*entityReferenceBytes
	case time.Time, time.Duration:
		return valueSlotBytes + 24
	default:
		return unknownValueBytes
	}
}

// rowEstimator sizes the binding rows one operator keeps, every row in
// full: a sampled estimate would undercount rows that grow late (long
// strings, lists built per row). A list or map the operator's rows share
// counts once, the first time the operator sees it, in whichever column:
// UNWIND documents AS d gives every row the same documents, and WITH and a
// CALL subquery's imports pass it on without copying it, however many
// other bindings the rows carry. A list or map built per row (range(), a
// list comprehension, a map projection) is a different one in every row
// and counts in full in each. Past rowEstimatorShared distinct lists and
// maps, a value repeated in the same column still counts once; any other
// counts in full (an overcount, never an undercount).
type rowEstimator struct {
	// shared are the lists and maps the operator counted.
	shared  map[sizedValueKey]struct{}
	columns [rowEstimatorColumns]struct {
		name     string
		identity unsafe.Pointer
	}
	count int
	// known sizes the lists collect() counted (valueBytes); nil walks them.
	known *transactionMemoryTracker
}

const (
	rowEstimatorColumns = 8
	rowEstimatorShared  = 1 << 14
)

// rowBytes is the estimated size of row.
func (r *rowEstimator) rowBytes(row pipelineRow) int64 {
	observeSizing()
	total := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range row {
		total += r.bindingBytes(name, value)
	}
	return total
}

// bindingBytes is the estimated size of one binding of a row.
func (r *rowEstimator) bindingBytes(name string, value interface{}) int64 {
	return mapEntryBytes + stringHeaderBytes + int64(len(name)) + r.columnBytes(name, value)
}

// columnBytes is the estimated size of a value in a row's column.
func (r *rowEstimator) columnBytes(column string, value interface{}) int64 {
	key, shared := sharedValueKey(value)
	if !shared {
		return estimateValueBytes(value)
	}
	identity, length := key.data, key.length
	if _, counted := r.shared[key]; counted {
		return valueSlotBytes + sliceHeaderBytes
	}
	if len(r.shared) < rowEstimatorShared {
		if r.shared == nil {
			r.shared = make(map[sizedValueKey]struct{})
		}
		r.shared[key] = struct{}{}
		return r.known.sizedValue(identity, length, value)
	}
	for index := 0; index < r.count; index++ {
		if r.columns[index].name == column {
			if r.columns[index].identity == identity {
				return valueSlotBytes + sliceHeaderBytes
			}
			r.columns[index].identity = identity
			return r.known.sizedValue(identity, length, value)
		}
	}
	if r.count < rowEstimatorColumns {
		r.columns[r.count].name, r.columns[r.count].identity = column, identity
		r.count++
	}
	return r.known.sizedValue(identity, length, value)
}

// sharedValueKey is the identity of a list or map, a value rows and builders
// can share without copying it: its backing storage and length. false for
// any other value, and for an empty one (no storage).
func sharedValueKey(value interface{}) (sizedValueKey, bool) {
	var identity unsafe.Pointer
	var length int
	switch v := value.(type) {
	case []interface{}:
		identity, length = unsafe.Pointer(unsafe.SliceData(v)), len(v)
	case []string:
		identity, length = unsafe.Pointer(unsafe.SliceData(v)), len(v)
	case []int64:
		identity, length = unsafe.Pointer(unsafe.SliceData(v)), len(v)
	case []float64:
		identity, length = unsafe.Pointer(unsafe.SliceData(v)), len(v)
	case []float32:
		identity, length = unsafe.Pointer(unsafe.SliceData(v)), len(v)
	case map[string]interface{}:
		if len(v) > 0 {
			identity, length = reflect.ValueOf(v).UnsafePointer(), len(v)
		}
	}
	return sizedValueKey{data: identity, length: length}, identity != nil
}

// estimateHeldBytes is estimateValueBytes of a value a builder keeps, where
// a list or map in seen, at any depth, counts as its slot: one the builder
// already counted, or one its input holds. What it counts is added to seen.
// A list of maps that all hold the same list counts that list once.
func estimateHeldBytes(value interface{}, seen map[sizedValueKey]struct{}) int64 {
	observeSizing()
	return estimateHeldBytesAt(value, seen, 0)
}

func estimateHeldBytesAt(value interface{}, seen map[sizedValueKey]struct{}, depth int) int64 {
	if depth > maxEstimateDepth {
		return unknownValueBytes
	}
	key, shared := sharedValueKey(value)
	if !shared {
		return estimateValueBytesAt(value, depth)
	}
	if _, counted := seen[key]; counted {
		return valueSlotBytes + sliceHeaderBytes
	}
	seen[key] = struct{}{}
	switch v := value.(type) {
	case []interface{}:
		total := valueSlotBytes + sliceHeaderBytes + int64(cap(v)-len(v))*valueSlotBytes
		for _, element := range v {
			total += estimateHeldBytesAt(element, seen, depth+1)
		}
		return total
	case map[string]interface{}:
		total := int64(valueSlotBytes + mapHeaderBytes)
		for name, element := range v {
			total += mapEntryBytes + stringHeaderBytes + int64(len(name)) + estimateHeldBytesAt(element, seen, depth+1)
		}
		return total
	default:
		return estimateValueBytesAt(value, depth)
	}
}

// estimateResultRowBytes is the estimated size of a result row; lists
// known's collect() counted are sized from what it counted.
func estimateResultRowBytes(row []interface{}, known *transactionMemoryTracker) int64 {
	observeSizing()
	total := int64(pipelineRowExtraBytes + sliceHeaderBytes)
	for _, value := range row {
		total += known.valueBytes(value)
	}
	return total
}

// estimatePathResultBytes is the estimated size of a matched path.
func estimatePathResultBytes(nodes, relationships int) int64 {
	observeSizing()
	return pathReferenceBytes + 2*sliceHeaderBytes + int64(nodes+relationships)*8
}

// aggregateStateBytes is the estimated size of one aggregation's running
// state in a group.
const aggregateStateBytes = 160

// estimateObserver, when set (tests), is called on every estimateValueBytes:
// a value walked.
var estimateObserver func()

// sizingObserver, when set (tests), is called on every sizing of a value or
// row (observeSizing): with no limit configured, nothing may size one.
var sizingObserver func()

func observeSizing() {
	if sizingObserver != nil {
		sizingObserver()
	}
}
