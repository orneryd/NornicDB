package server

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// HTTP results write a point as Neo4j 5.26.30 does, keys in its order, with
// meta {"type":"point"} (#817).
func TestTransactionHTTPPointMatchesNeo4j(t *testing.T) {
	s := &Server{}
	value, meta := s.transactionHTTPValue(cypher.CypherPoint{SRID: 7203, X: 1, Y: 2}, "neo4j")
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"Point","coordinates":[1.0,2.0],"crs":{"srid":7203,"name":"cartesian","type":"link","properties":{"href":"https://spatialreference.org/ref/sr-org/7203/ogcwkt/","type":"ogcwkt"}}}`, string(encoded))
	require.Equal(t, `{"type":"Point","coordinates":[1.0,2.0],"crs":{"srid":7203,"name":"cartesian","type":"link","properties":{"href":"https://spatialreference.org/ref/sr-org/7203/ogcwkt/","type":"ogcwkt"}}}`, string(encoded))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "point"}}, meta)

	value, meta = s.transactionHTTPValue(&cypher.CypherPoint{SRID: 4979, X: 2.5, Y: 1.5, Z: 3}, "neo4j")
	encoded, err = json.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, `{"type":"Point","coordinates":[2.5,1.5,3.0],"crs":{"srid":4979,"name":"wgs-84-3d","type":"link","properties":{"href":"https://spatialreference.org/ref/epsg/4979/ogcwkt/","type":"ogcwkt"}}}`, string(encoded))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "point"}}, meta)

	value, meta = s.transactionHTTPValue((*cypher.CypherPoint)(nil), "neo4j")
	require.Nil(t, value)
	require.Equal(t, []interface{}{nil}, meta)
}
