package cypher

import (
	"context"
	"encoding/json"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

type review5276MessageCase struct {
	name, query string
	params      map[string]interface{}
}

func review5276MessageCases() []review5276MessageCase {
	return []review5276MessageCase{
		{"coll_sort_c25_integer", "CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": int64(1)}},
		{"coll_sort_c25_boolean", "CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": true}},
		{"coll_sort_c25_map", "CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": map[string]interface{}{"a": int64(1)}}},
		{"coll_sort_default", "RETURN coll.sort($l) AS v", map[string]interface{}{"l": int64(1)}},
		{"toupper_direct", "RETURN toUpper( $s ) AS v", map[string]interface{}{"s": int64(1)}},
		{"toupper_c25", "CYPHER 25 RETURN toUpper($s) AS v", map[string]interface{}{"s": int64(1)}},
		{"tail_direct", "RETURN tail($l) AS v", map[string]interface{}{"l": int64(1)}},
		{"toupper_alias", "WITH $s AS x RETURN toUpper(x) AS v", map[string]interface{}{"s": int64(1)}},
	}
}
func TestReviewer5276MessageObservation(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, c := range review5276MessageCases() {
		t.Run(c.name, func(t *testing.T) {
			_, err := e.Execute(context.Background(), c.query, c.params)
			require.Error(t, err)
			code, msg := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			b, _ := json.Marshal(map[string]interface{}{"case": c.name, "query": c.query, "code": code, "message": msg})
			t.Log("MESSAGE_NATIVE " + string(b))
		})
	}
}
func TestReviewer5276MessageReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	require.NotEmpty(t, uri)
	d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer d.Close(context.Background())
	v, err := neo4j.ExecuteQuery(context.Background(), d, "CALL dbms.components() YIELD versions RETURN versions[0] AS version", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	t.Logf("MESSAGE_REFERENCE_VERSION %v", v.Records[0].Values[0])
	for _, c := range review5276MessageCases() {
		t.Run(c.name, func(t *testing.T) {
			_, err := neo4j.ExecuteQuery(context.Background(), d, c.query, c.params, neo4j.EagerResultTransformer)
			require.Error(t, err)
			ne, ok := err.(*neo4j.Neo4jError)
			require.True(t, ok)
			b, _ := json.Marshal(map[string]interface{}{"case": c.name, "query": c.query, "code": ne.Code, "message": ne.Msg})
			t.Log("MESSAGE_REFERENCE " + string(b))
			if c.name == "toupper_direct" || c.name == "tail_direct" {
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", ne.Code)
				require.True(t, strings.Contains(ne.Msg, "Type mismatch for parameter"))
			}
		})
	}
}
func TestReviewer5276MessageOracleParity(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	require.NotEmpty(t, uri)
	d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer d.Close(context.Background())
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, c := range review5276MessageCases() {
		if c.name != "toupper_direct" && c.name != "tail_direct" {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			_, re := neo4j.ExecuteQuery(context.Background(), d, c.query, c.params, neo4j.EagerResultTransformer)
			require.Error(t, re)
			ne, ok := re.(*neo4j.Neo4jError)
			require.True(t, ok)
			_, err := e.Execute(context.Background(), c.query, c.params)
			require.Error(t, err)
			code, msg := nornicerrors.Neo4jStatus(err)
			require.Equal(t, ne.Code, code)
			referenceMessage := strings.SplitN(ne.Msg, " (line ", 2)[0]
			require.Contains(t, msg, referenceMessage, "public query=%s", c.query)
		})
	}
}
