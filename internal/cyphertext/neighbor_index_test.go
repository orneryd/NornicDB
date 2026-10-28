package cyphertext

import "testing"

func TestNeighborIndexTerminalContract(t *testing.T) {
	for _, tt := range []struct {
		s     string
		i     int
		d     NeighborDirection
		at    int
		found bool
	}{
		{" x ", 3, Before, 1, true}, {" x ", 0, AtOrAfter, 1, true}, {"  ", 2, Before, -1, false}, {"  ", 0, AtOrAfter, 2, false}, {" \x00 ", 0, AtOrAfter, 1, true}, {"", 0, Before, -1, false}, {"a", -1, AtOrAfter, -1, true}, {"a", 0, NeighborDirection(2), 0, true},
	} {
		at, found := NeighborIndex(tt.s, tt.i, tt.d)
		if at != tt.at || found != tt.found {
			t.Fatalf("%q %d %d: (%d,%t), want (%d,%t)", tt.s, tt.i, tt.d, at, found, tt.at, tt.found)
		}
	}
}
