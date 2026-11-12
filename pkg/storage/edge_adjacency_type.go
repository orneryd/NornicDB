package storage

import "encoding/binary"

// adjacentHeaderFixedWidth is the number of bytes a compact edge header (the
// value an adjacency entry stores, adjacencyValueFromRecord) spends before its
// type length: format, flags, start/end numIDs, created/updated nanos and
// confidence.
const adjacentHeaderFixedWidth = 1 + 1 + 8 + 8 + 8 + 8 + 8

// adjacencyEntryTypeBytes returns the relationship type bytes a compact header
// carries without decoding the rest of the header and without copying them.
// ok is false for a value that carries no readable type, such as an entry
// written before the values existed; the caller then reads the relationship
// record instead.
func adjacencyEntryTypeBytes(value []byte) (typeBytes []byte, ok bool) {
	if len(value) < adjacentHeaderFixedWidth || value[0] != edgeFormatCompactV2 {
		return nil, false
	}
	offset := adjacentHeaderFixedWidth
	typeLen, n := binary.Uvarint(value[offset:])
	if n <= 0 {
		return nil, false
	}
	offset += n
	if uint64(len(value)-offset) < typeLen {
		return nil, false
	}
	return value[offset : offset+int(typeLen)], true
}

// adjacencyEntryTypeMatches reports whether an adjacency entry value describes
// one of the requested relationship types. decided is false when the value
// carries no readable type, so the caller must read the relationship record to
// decide. An entry of another type is rejected from its value alone, before the
// relationship is hydrated or allocated.
func adjacencyEntryTypeMatches(value []byte, types []string) (matched, decided bool) {
	typeBytes, ok := adjacencyEntryTypeBytes(value)
	if !ok {
		return false, false
	}
	// Comparing string([]byte) to a string does not allocate: the compiler
	// compares the bytes directly.
	if len(types) == 1 {
		return string(typeBytes) == types[0], true
	}
	for _, candidate := range types {
		if string(typeBytes) == candidate {
			return true, true
		}
	}
	return false, true
}
