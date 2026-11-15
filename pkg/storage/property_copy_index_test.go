package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// callerCopyIndexBacking returns a slice whose array is heap allocated. The
// identity controls below compare one address across several statements, and an
// array allocated in the test's own frame may be reclaimed or moved once the
// compiler sees no further pointer use of it, which would invalidate an address
// observed earlier. Returning the slice from a non-inlined call makes the array
// escape, so every observed address stays the address of the same array.
//
//go:noinline
func callerCopyIndexBacking[T any](length int) []T {
	return make([]T, length)
}

// The compact identity index stores complete records in append-only blocks and
// keeps only compact handles in the hash table, so growing the table re-hashes
// handles rather than the records. A walk that outgrows many blocks must keep
// every recorded reference findable across every re-hash, must answer each
// reference with its own copy, and must leave references it never recorded
// absent. This control therefore walks the table through its whole growth
// sequence instead of stopping at the first spill.
func TestCallerCopyIndexGrowsAcrossBlocksWithoutLookupLoss(t *testing.T) {
	const entries = 1200 // spans nine blocks: 8+16+32+64+128+256+256+256+256
	backing := callerCopyIndexBacking[any](entries + 1)
	address := uintptr(reflect.ValueOf(backing).UnsafePointer())
	sliceType := reflect.TypeFor[[]any]()

	// One backing array, one address: only the slice length separates these
	// references, and the never-inserted length at the end must stay absent.
	keys := make([]callerCopyReference, entries)
	for i := range keys {
		keys[i] = callerCopyReference{typ: sliceType, pointer: address, length: i + 1}
	}

	index := newCallerCopyIndex()
	require.Equal(t, 16, len(index.slots), "a fresh index starts from the smallest table")

	grows := 0
	for i := range keys {
		before := len(index.slots)
		index.insert(keys[i], reflect.ValueOf(i))
		if len(index.slots) == before {
			continue
		}
		grows++
		// A re-hash must carry every record already placed, not just the newest.
		for j := 0; j <= i; j++ {
			copied, ok := index.lookup(keys[j])
			require.True(t, ok, "lookup lost entry %d when growing to %d slots", j, len(index.slots))
			require.Equal(t, int64(j), copied.Int(), "lookup returned another reference's copy")
		}
	}

	require.GreaterOrEqual(t, grows, 4, "the control must exercise repeated growth")
	require.GreaterOrEqual(t, len(index.slots), 2048, "the table must grow well past its initial size")
	require.LessOrEqual(t, index.count*4, len(index.slots)*3, "growth must keep the load-factor bound")
	require.Equal(t, entries, index.count)
	require.GreaterOrEqual(t, len(index.blocks), 8, "the control must span many blocks")

	// Handles address block positions, so each block may use at most 256
	// positions: the stored handle must never overflow its 8-bit offset, and
	// every recorded reference must be addressable exactly once.
	placed := 0
	for blockIndex, block := range index.blocks {
		require.LessOrEqual(t, cap(block), 256, "handle offsets must stay inside one block")
		for position := range block {
			handle := uint64(blockIndex)<<8 | uint64(position)
			handle++
			entry := index.entry(handle)
			require.Equal(t, keys[placed], entry.key)
			require.Equal(t, int64(placed), entry.copy.Int())
			placed++
		}
	}
	require.Equal(t, entries, placed, "every record must be addressable exactly once")

	handles := make(map[uint64]struct{}, entries)
	for _, handle := range index.slots {
		if handle == 0 {
			continue
		}
		_, duplicate := handles[handle]
		require.False(t, duplicate, "one handle was placed twice")
		handles[handle] = struct{}{}
	}
	require.Len(t, handles, entries)

	// No lookup is lost after the final re-hash, and a reference sharing the
	// same type and address with a different length is not answered for another.
	for i, key := range keys {
		copied, ok := index.lookup(key)
		require.True(t, ok)
		require.Equal(t, int64(i), copied.Int())
	}
	_, ok := index.lookup(callerCopyReference{typ: sliceType, pointer: address, length: entries + 1})
	require.False(t, ok, "an unrecorded reference must stay absent")
}

// A reference is identified by type, address and length together. Slices over
// one array share the address, so dropping either the length or the concrete
// type from the identity would answer one reference with another one's copy.
func TestCallerCopyIndexKeepsSameAddressSlicesAndTypesDistinct(t *testing.T) {
	type rowA []any
	type rowB []any

	backing := callerCopyIndexBacking[any](2)
	address := uintptr(reflect.ValueOf(backing).UnsafePointer())
	anySlice := reflect.TypeFor[[]any]()
	for _, sameAddress := range []any{backing, backing[:1], rowA(backing), rowB(backing)} {
		require.Equal(t, address, uintptr(reflect.ValueOf(sameAddress).UnsafePointer()),
			"the control must share one address")
	}

	short := callerCopyReference{typ: anySlice, pointer: address, length: 1}
	long := callerCopyReference{typ: anySlice, pointer: address, length: 2}
	typedA := callerCopyReference{typ: reflect.TypeFor[rowA](), pointer: address, length: 2}
	typedB := callerCopyReference{typ: reflect.TypeFor[rowB](), pointer: address, length: 2}

	index := newCallerCopyIndex()
	index.insert(short, reflect.ValueOf("short"))
	index.insert(long, reflect.ValueOf("long"))
	index.insert(typedA, reflect.ValueOf("row-a"))
	index.insert(typedB, reflect.ValueOf("row-b"))
	require.Equal(t, 4, index.count)

	for _, recorded := range []struct {
		key   callerCopyReference
		value string
	}{{short, "short"}, {long, "long"}, {typedA, "row-a"}, {typedB, "row-b"}} {
		copied, ok := index.lookup(recorded.key)
		require.True(t, ok)
		require.Equal(t, recorded.value, copied.String())
	}

	otherArray := callerCopyIndexBacking[any](2)
	otherAddress := uintptr(reflect.ValueOf(otherArray).UnsafePointer())
	require.NotEqual(t, address, otherAddress)
	for _, absent := range []callerCopyReference{
		{typ: anySlice, pointer: address, length: 3},
		{typ: anySlice, pointer: address, length: 0},
		{typ: anySlice, pointer: otherAddress, length: 2},
		{typ: reflect.TypeFor[[2]any](), pointer: address, length: 2},
	} {
		_, ok := index.lookup(absent)
		require.False(t, ok, "a reference that was never recorded must stay absent: %+v", absent)
	}
}

// A copy's scratch index is cleared before it is pooled, and a later copy reuses
// that cleared storage. A small index is pool-eligible, so this control uses one
// that stays under the existing reference cap. Clearing must drop the previous
// source references and destination images while keeping the block and table
// storage reusable, and the reused index must answer with the new pass only.
func TestCallerCopyIndexReusesClearedPooledEntriesWithoutRetention(t *testing.T) {
	require.Equal(t, 1024, callerCopyPooledReferences, "the pooling cap is the contract under test")
	const entries = 20
	require.LessOrEqual(t, entries, callerCopyPooledReferences, "the control must be pool-eligible")

	backing := callerCopyIndexBacking[any](entries)
	address := uintptr(reflect.ValueOf(backing).UnsafePointer())
	sliceType := reflect.TypeFor[[]any]()
	keys := make([]callerCopyReference, entries)
	for i := range keys {
		keys[i] = callerCopyReference{typ: sliceType, pointer: address, length: i + 1}
	}
	mapPointer := func(value map[string]any) uintptr {
		return uintptr(reflect.ValueOf(value).UnsafePointer())
	}

	// The first pass records one source reference and one destination image per
	// entry, standing in for a completed copy.
	sources := make([]map[string]any, entries)
	for i := range sources {
		sources[i] = map[string]any{"value": int64(i)}
	}
	index := newCallerCopyIndex()
	for i, key := range keys {
		index.insert(key, reflect.ValueOf(sources[i]))
	}
	require.Equal(t, entries, index.count)
	require.GreaterOrEqual(t, len(index.blocks), 2, "the control must span more than one block")

	slotsStorage := uintptr(reflect.ValueOf(index.slots).UnsafePointer())
	blockStorage := make([]uintptr, len(index.blocks))
	for i := range index.blocks {
		blockStorage[i] = uintptr(reflect.ValueOf(index.blocks[i]).UnsafePointer())
	}
	blockCount := len(index.blocks)

	index.clear()

	// Nothing from the previous copy may survive the clear: no slot, no source
	// reference, no destination image.
	require.Zero(t, index.count)
	require.Zero(t, index.current)
	for _, handle := range index.slots {
		require.Zero(t, handle)
	}
	for blockIndex, block := range index.blocks {
		require.Zero(t, len(block))
		for position, entry := range block[:cap(block)] {
			require.Equal(t, callerCopyReference{}, entry.key,
				"block %d position %d kept a source reference", blockIndex, position)
			require.False(t, entry.copy.IsValid(),
				"block %d position %d kept a destination image", blockIndex, position)
		}
	}
	for _, key := range keys {
		_, ok := index.lookup(key)
		require.False(t, ok, "a cleared index must not answer from the previous copy")
	}

	// Reusing the cleared index records the new pass in the same storage, and
	// every answer is this pass's destination, never the earlier one.
	reused := make([]map[string]any, entries)
	for i := range reused {
		reused[i] = map[string]any{"value": int64(100 + i)}
	}
	for i, key := range keys {
		index.insert(key, reflect.ValueOf(reused[i]))
	}
	require.Equal(t, entries, index.count)
	require.Equal(t, blockCount, len(index.blocks), "a cleared index keeps its blocks")
	require.Equal(t, slotsStorage, uintptr(reflect.ValueOf(index.slots).UnsafePointer()),
		"a cleared index keeps its table")
	for i := range index.blocks {
		require.Equal(t, blockStorage[i], uintptr(reflect.ValueOf(index.blocks[i]).UnsafePointer()),
			"block %d must reuse its storage", i)
	}
	for i, key := range keys {
		copied, ok := index.lookup(key)
		require.True(t, ok)
		destination := copied.Interface().(map[string]any)
		require.Equal(t, mapPointer(reused[i]), mapPointer(destination))
		require.NotEqual(t, mapPointer(sources[i]), mapPointer(destination))
	}
}

// A matrix wide enough to spill and re-hash must still carry the caller-copy
// contract across rows: one destination per shared element, a cycle that
// resolves inside the copy, and no part of the copy shared with the source.
func TestCallerCopyThroughIndexPreservesAliasesAndCyclesAcrossRows(t *testing.T) {
	const rows = 200
	shared := map[string]any{"value": int64(0)}
	matrix := make([][]any, rows)
	for i := range matrix {
		matrix[i] = []any{shared, int64(i)}
	}
	matrix[0] = append(matrix[0], matrix) // the first row links back to every row

	// The control is meaningful only while this shape really outgrows the inline
	// entries and re-hashes across many blocks, so inspect the walk itself.
	probe := callerCopyWalker{}
	_ = probe.value(matrix)
	require.NotNil(t, probe.seen, "the control must exercise the identity index")
	require.Equal(t, len(probe.inline), probe.count)
	require.GreaterOrEqual(t, len(probe.seen.blocks), 5, "the control must span many blocks")
	require.Greater(t, len(probe.seen.slots), 16, "the control must re-hash the handle table")

	copied := CopyValue(matrix, nil).([][]any)
	require.Len(t, copied, rows)
	require.NotEqual(t, uintptr(reflect.ValueOf(matrix).UnsafePointer()),
		uintptr(reflect.ValueOf(copied).UnsafePointer()))

	sourceShared := uintptr(reflect.ValueOf(shared).UnsafePointer())
	destination := copied[0][0].(map[string]any)
	destinationPointer := uintptr(reflect.ValueOf(destination).UnsafePointer())
	rowStorage := make(map[uintptr]struct{}, rows)
	for i, row := range copied {
		require.Len(t, row, len(matrix[i]))
		require.Equal(t, destinationPointer, uintptr(reflect.ValueOf(row[0]).UnsafePointer()),
			"row %d lost the shared destination", i)
		rowPointer := uintptr(reflect.ValueOf(row).UnsafePointer())
		_, duplicate := rowStorage[rowPointer]
		require.False(t, duplicate, "row %d reused another row's storage", i)
		rowStorage[rowPointer] = struct{}{}
	}
	require.NotEqual(t, sourceShared, destinationPointer, "the copy must not reuse the source image")

	cycle := copied[0][2].([][]any)
	require.Len(t, cycle, rows)
	require.Equal(t, uintptr(reflect.ValueOf(copied).UnsafePointer()),
		uintptr(reflect.ValueOf(cycle).UnsafePointer()), "the row cycle must resolve to the copied matrix")

	// Every route inside the copy reaches that one copy.
	destination["value"] = int64(7)
	require.Equal(t, int64(7), copied[rows-1][0].(map[string]any)["value"])
	require.Equal(t, int64(7), cycle[rows/2][0].(map[string]any)["value"])
	cycle[3][1] = int64(-3)
	require.Equal(t, int64(-3), copied[3][1], "the row cycle must reach the copied row")

	// Nothing in the copy is shared with the input.
	require.Equal(t, int64(0), shared["value"])
	require.Equal(t, int64(3), matrix[3][1])
}

// The same address can back slices of different lengths and slices of different
// concrete types. Both must stay distinct reference-bearing values while any
// element they really share keeps one destination image.
func TestCallerCopyKeepsSameAddressRowsAndTypesDistinct(t *testing.T) {
	type rowA []any
	type rowB []any

	shared := map[string]any{"value": int64(1)}
	backing := callerCopyIndexBacking[any](2)
	backing[0] = shared
	backing[1] = "tail"
	address := uintptr(reflect.ValueOf(backing).UnsafePointer())
	full := backing       // len 2 over the shared array
	prefix := backing[:1] // len 1 over the same array
	rowOfA := rowA(backing)
	rowOfB := rowB(backing)
	for _, slice := range []any{full, prefix, rowOfA, rowOfB} {
		require.Equal(t, address, uintptr(reflect.ValueOf(slice).UnsafePointer()),
			"the control must share one address")
	}

	// The decoys fill the inline entries, so these identity decisions are made by
	// the index rather than by the four inline records.
	root := []any{
		map[string]any{"decoy": 0}, map[string]any{"decoy": 1}, map[string]any{"decoy": 2},
		full, prefix, rowOfA, rowOfB,
	}
	probe := callerCopyWalker{}
	_ = probe.value(root)
	require.NotNil(t, probe.seen, "the control must exercise the identity index")
	require.Equal(t, len(probe.inline), probe.count)

	copied := CopyValue(root, nil).([]any)
	require.Len(t, copied, len(root))

	copiedFull := copied[3].([]any)
	copiedPrefix := copied[4].([]any)
	require.Len(t, copiedFull, 2)
	require.Len(t, copiedPrefix, 1, "a shorter slice over the same address is a distinct reference")
	require.NotEqual(t, uintptr(reflect.ValueOf(copiedFull).UnsafePointer()),
		uintptr(reflect.ValueOf(copiedPrefix).UnsafePointer()))

	// The element both slices really share still resolves to one destination.
	require.Equal(t, uintptr(reflect.ValueOf(copiedFull[0]).UnsafePointer()),
		uintptr(reflect.ValueOf(copiedPrefix[0]).UnsafePointer()))
	copiedPrefix[0].(map[string]any)["value"] = int64(2)
	require.Equal(t, int64(2), copiedFull[0].(map[string]any)["value"])
	require.Equal(t, int64(1), shared["value"], "the copies must not be shared with the source")

	require.IsType(t, rowA(nil), copied[5], "a distinct concrete type over the same address keeps its own copy")
	require.IsType(t, rowB(nil), copied[6])
	require.NotEqual(t, uintptr(reflect.ValueOf(copied[5]).UnsafePointer()),
		uintptr(reflect.ValueOf(copied[6]).UnsafePointer()))
	copied[5].(rowA)[1] = "changed-a"
	require.Equal(t, "tail", copied[6].(rowB)[1], "distinct concrete types over one address must not be conflated")
	require.Equal(t, "tail", backing[1], "the copies must not be shared with the source")
}

// Each copy owns its visited state and a small index is cleared and pooled after
// use, so a later copy of the same source reuses cleared storage. Nothing from
// the earlier copy may survive that reuse: a retained record would hand the
// second copy the first copy's destination and silently share two caller-owned
// graphs.
func TestCallerCopyReusingPooledIndexKeepsCopiesIndependent(t *testing.T) {
	shared := map[string]any{"value": int64(0)}
	rows := make([][]any, 12)
	for i := range rows {
		rows[i] = []any{shared, int64(i)}
	}
	rows[0] = append(rows[0], rows)

	probe := callerCopyWalker{}
	_ = probe.value(rows)
	require.NotNil(t, probe.seen, "the control must exercise the identity index")
	require.LessOrEqual(t, probe.seen.count, callerCopyPooledReferences, "the control must be pool-eligible")

	first := CopyValue(rows, nil).([][]any)
	second := CopyValue(rows, nil).([][]any)

	require.NotEqual(t, uintptr(reflect.ValueOf(first[0][0]).UnsafePointer()),
		uintptr(reflect.ValueOf(second[0][0]).UnsafePointer()),
		"a later copy must not inherit the earlier copy's destination image")
	require.NotEqual(t, uintptr(reflect.ValueOf(first[0][2]).UnsafePointer()),
		uintptr(reflect.ValueOf(second[0][2]).UnsafePointer()))

	second[0][0].(map[string]any)["value"] = int64(5)
	require.Equal(t, int64(0), first[0][0].(map[string]any)["value"],
		"the second copy must not write into the first")
	second[0][2].([][]any)[5][1] = int64(-5)
	require.Equal(t, int64(5), first[5][1], "the second copy's cycle must not reach the first copy")
	require.Equal(t, int64(5), rows[5][1])
	require.Equal(t, int64(0), shared["value"])
}
