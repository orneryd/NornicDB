package cyphertext

// NeighborDirection indicates the direction to search for a non-whitespace byte.
type NeighborDirection int

const (
	Before    NeighborDirection = iota // Search backward from index-1
	AtOrAfter                          // Search forward from index
)

// NeighborByte returns a bounded byte view, or zero when no byte is present.
func NeighborByte(text string, index int, direction NeighborDirection) byte {
	at, found := NeighborIndex(text, index, direction)
	if !found || at < 0 || at >= len(text) {
		return 0
	}
	return text[at]
}

// NeighborIndex reports the raw non-whitespace cursor or actual scan terminal.
// Before starts at index-1; every other direction starts at index and moves forward.
// It retains byte stepping and does not clamp invalid offsets. found describes
// the whitespace decision; callers must apply their own bounds policy before
// dereferencing an invalid input cursor. NeighborByte provides the bounded view.
// For example, NeighborIndex("x ",2,Before) returns (0,true).
func NeighborIndex(text string, index int, direction NeighborDirection) (int, bool) {
	before := direction == Before
	step := 1
	if before {
		index--
		step = -1
	}
	for before && index >= 0 || !before && index < len(text) {
		if !IsWhitespaceAt(text, index) {
			return index, true
		}
		index += step
	}
	return index, false
}
