package cyphertext

// NeighborDirection indicates the direction to search for a non-whitespace byte.
type NeighborDirection int

const (
	Before    NeighborDirection = iota // Search backward from index-1
	AtOrAfter                          // Search forward from index
)

// NeighborByte returns the first non-whitespace byte in the given direction,
// or 0 if none found. Uses BYTE offsets exactly (no UTF8 cursor policy changes).
func NeighborByte(text string, index int, direction NeighborDirection) byte {
	step := 1
	if direction == Before {
		index--
		step = -1
	}

	for i := index; i >= 0 && i < len(text); i += step {
		if !IsWhitespaceAt(text, i) {
			return text[i]
		}
	}
	return 0
}
