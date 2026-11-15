package storage

import (
	"hash/maphash"
	"reflect"
)

// callerCopyIndex keeps complete identity records in append-only blocks. The
// hash table stores compact handles, so growing it rehashes handles instead of
// allocating and copying every reflected identity/value pair again. This scratch
// belongs to one copy operation; the existing 1024-reference pooling cap applies.
// No source reference or caller-owned image survives a pooled index's clear.
type callerCopyIndex struct {
	seed    maphash.Seed
	slots   []uint64
	blocks  [][]callerCopyTracked
	count   int
	current int
}

func newCallerCopyIndex() *callerCopyIndex {
	return &callerCopyIndex{seed: maphash.MakeSeed(), slots: make([]uint64, 16)}
}

func (index *callerCopyIndex) entry(handle uint64) *callerCopyTracked {
	position := handle - 1
	return &index.blocks[position>>8][position&255]
}

// slot is the one probe owner for lookup, insertion and rehashing.
func (index *callerCopyIndex) slot(key callerCopyReference) (uint64, uint64) {
	mask := uint64(len(index.slots) - 1)
	for position := maphash.Comparable(index.seed, key) & mask; ; position = (position + 1) & mask {
		handle := index.slots[position]
		if handle == 0 || index.entry(handle).key == key {
			return position, handle
		}
	}
}

func (index *callerCopyIndex) lookup(key callerCopyReference) (reflect.Value, bool) {
	_, handle := index.slot(key)
	if handle == 0 {
		return reflect.Value{}, false
	}
	return index.entry(handle).copy, true
}

func (index *callerCopyIndex) insert(key callerCopyReference, copied reflect.Value) {
	if (index.count+1)*4 > len(index.slots)*3 {
		index.grow()
	}
	position, previous := index.slot(key)
	if previous != 0 {
		index.entry(previous).copy = copied
		return
	}
	blockIndex := index.current
	if len(index.blocks) == 0 {
		index.blocks = append(index.blocks, make([]callerCopyTracked, 0, 8))
	} else if len(index.blocks[blockIndex]) == cap(index.blocks[blockIndex]) {
		blockIndex++
		if blockIndex == len(index.blocks) {
			capacity := min(256, cap(index.blocks[blockIndex-1])*2)
			index.blocks = append(index.blocks, make([]callerCopyTracked, 0, capacity))
		}
		index.current = blockIndex
	}
	block := index.blocks[blockIndex]
	// Each block uses at most 256 positions; handle zero denotes an empty slot.
	handle := uint64(blockIndex)<<8 | uint64(len(block))
	handle++
	index.blocks[blockIndex] = append(block, callerCopyTracked{key: key, copy: copied})
	index.count++
	index.slots[position] = handle
}

func (index *callerCopyIndex) grow() {
	old := index.slots
	index.slots = make([]uint64, len(old)*2)
	for _, handle := range old {
		if handle != 0 {
			position, _ := index.slot(index.entry(handle).key)
			index.slots[position] = handle
		}
	}
}

func (index *callerCopyIndex) clear() {
	clear(index.slots)
	for i := range index.blocks {
		clear(index.blocks[i])
		index.blocks[i] = index.blocks[i][:0]
	}
	index.current = 0
	index.count = 0
}
