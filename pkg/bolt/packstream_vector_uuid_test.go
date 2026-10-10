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
		require.Equal(t, encodePackStreamMapIntoWithUTC(nil, placeholder, false), encodePackStreamValueIntoWithUTC(nil, value, false))
		require.Equal(t, encodePackStreamMapIntoWithUTC(nil, placeholder, true), encodePackStreamValue(value))
	}
}
