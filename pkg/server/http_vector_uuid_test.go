package server

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// The HTTP API writes a UUID as its text and a VECTOR as Neo4j 2026.09
// describes it, not its coordinates (#907).
func TestTransactionHTTPVectorAndUUID(t *testing.T) {
	s := &Server{}
	value, meta := s.transactionHTTPValue(cypher.CypherVector{Type: cypher.VectorInteger64, Ints: []int64{1, 2}}, "neo4j")
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{"typeName":"Int64Vector","incomparableType":true,"sequenceValue":false}`, string(encoded))
	require.Equal(t, []interface{}{nil}, meta)
	value, meta = s.transactionHTTPValue(cypher.CypherVector{Type: cypher.VectorFloat32, Floats: []float64{1}}, "neo4j")
	encoded, err = json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{"typeName":"Float32Vector","incomparableType":true,"sequenceValue":false}`, string(encoded))
	value, meta = s.transactionHTTPValue(cypher.CypherUUID{}, "neo4j")
	require.Equal(t, "00000000-0000-0000-0000-000000000000", value)
	require.Equal(t, []interface{}{nil}, meta)
}
