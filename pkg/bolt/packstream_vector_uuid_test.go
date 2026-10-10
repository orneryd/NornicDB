package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// Bolt 5 has no VECTOR or UUID structure: both encoders send Neo4j's
// placeholder map, {reason: 'UNKNOWN_TYPE', originalType: …} (#907).
func TestPackStreamVectorAndUUIDPlaceholders(t *testing.T) {
	for _, value := range []any{
		cypher.CypherVector{Type: cypher.VectorInteger64, Ints: []int64{1, 2}},
		cypher.CypherUUID{},
	} {
		placeholder, ok := cypher.UnsupportedTypePlaceholder(value)
		require.True(t, ok)
		// A map's keys go out in Go's map order, so the bytes are compared
		// decoded.
		for _, encoded := range [][]byte{encodePackStreamValueIntoWithUTC(nil, value, false), encodePackStreamValue(value)} {
			decoded, _, err := decodePackStreamMap(encoded, 0)
			require.NoError(t, err)
			require.Equal(t, placeholder, decoded)
		}
	}
}
