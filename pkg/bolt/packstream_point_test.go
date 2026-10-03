package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// Points travel as Point2D (0x58) and Point3D (0x59) structures both ways
// (#817).
func TestPackStreamPointRoundTrip(t *testing.T) {
	for _, point := range []cypher.CypherPoint{
		{SRID: 7203, X: 1, Y: 2},
		{SRID: 4326, X: 2.5, Y: 1.5},
		{SRID: 9157, X: 1, Y: 2, Z: 3},
		{SRID: 4979, X: 2.5, Y: 1.5, Z: 10},
	} {
		encoded := encodePackStreamValue(point)
		require.Equal(t, encoded, encodePackStreamValueInto(nil, point))
		require.Equal(t, encoded, encodePackStreamValue(&point))
		decoded, consumed, err := decodePackStreamValue(encoded, 0)
		require.NoError(t, err)
		require.Equal(t, len(encoded), consumed)
		require.Equal(t, point, decoded)
	}
	require.Equal(t, []byte{0xC0}, encodePackStreamValue((*cypher.CypherPoint)(nil)))
	require.Equal(t, []byte{0xC0}, encodePackStreamValueInto(nil, (*cypher.CypherPoint)(nil)))

	// A structure with an unknown SRID stays a generic structure.
	unknown := append([]byte{0xB3, 0x58}, encodePackStreamValue(int64(1234))...)
	unknown = append(unknown, encodePackStreamValue(1.0)...)
	unknown = append(unknown, encodePackStreamValue(2.0)...)
	decoded, _, err := decodePackStreamValue(unknown, 0)
	require.NoError(t, err)
	require.Equal(t, "Point", decoded.(map[string]any)["_type"])
}
