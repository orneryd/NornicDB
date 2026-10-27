package cyphertext

import "testing"

func TestNeighborBytePreservesByteCursors(t *testing.T) {
	for _, tc := range []struct {
		text      string
		index     int
		direction NeighborDirection
		want      byte
	}{
		{"", 0, Before, 0}, {"  ", 1, AtOrAfter, 0}, {"x ", 2, Before, 'x'},
		{" x", 0, AtOrAfter, 'x'}, {"x", 0, Before, 0}, {"x", 1, AtOrAfter, 0},
		{".\u00a0:", 2, Before, '.'}, {".\u00a0:", 2, AtOrAfter, ':'},
	} {
		if got := NeighborByte(tc.text, tc.index, tc.direction); got != tc.want {
			t.Errorf("%q index=%d direction=%d got %q want %q", tc.text, tc.index, tc.direction, got, tc.want)
		}
	}
}
