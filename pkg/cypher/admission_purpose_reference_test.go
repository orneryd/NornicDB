package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestAdmissionPurposeReference(t *testing.T) {
	admissionPurposeReference(t, false)
}
func TestAdmissionPurposeCypher25Reference(t *testing.T) {
	admissionPurposeReference(t, true)
}
func admissionPurposeReference(t *testing.T, version25 bool) {
	var ref neo4j.DriverWithContext
	if version25 {
		uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
		require.NotEmpty(t, uri)
		driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
		ref = driver
		version, err := neo4j.ExecuteQuery(context.Background(), ref, "CALL dbms.components() YIELD versions RETURN versions[0] AS v", nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		require.Equal(t, "2025.12.1", version.Records[0].Values[0])
	} else {
		ref = fabricCallRowsReference(t)
		require.NotNil(t, ref)
	}
	cases := []struct {
		query  string
		params map[string]interface{}
	}{
		{"CALL db.index.fulltext.queryNodes($index, 'graph') YIELD node RETURN node", map[string]interface{}{"index": int64(1)}},
		{"WITH $index AS index CALL db.index.fulltext.queryNodes(index, 'graph') YIELD node RETURN node", map[string]interface{}{"index": int64(1)}},
		{"WITH 1 AS index CALL db.index.fulltext.queryNodes(index, 'graph') YIELD node RETURN node", nil},
		{"CALL db.index.vector.queryNodes($index, 1, [1.0]) YIELD node RETURN node", map[string]interface{}{"index": int64(1)}},
		{"CALL db.index.fulltext.queryNodes($cfg.index, 'graph') YIELD node RETURN node", map[string]interface{}{"cfg": map[string]interface{}{"index": int64(1)}}},
		{"WITH $m + 1 AS m RETURN m.a", nil},
		{"WITH {a:1} AS count RETURN count{.a} AS v", nil},
		{"WITH {a:1} AS collect RETURN collect{.a} AS v", nil},
		{"WITH {a:1} AS exists RETURN exists{.a} AS v", nil},
		{"WITH 2 AS distinct RETURN distinct{.a} AS v", nil},
		{"UNWIND [1,{k:1}] AS item RETURN [(item)-->(m)|m] AS v", nil},
		{"CREATE (n:PurposeGate) WITH n UNWIND [{id:1}] AS row SET n += row RETURN n.id AS v", nil},
	}
	if version25 {
		cases = []struct {
			query  string
			params map[string]interface{}
		}{
			{"CYPHER 25 RETURN toString([1,'a']) AS v", nil},
			{"CYPHER 25 RETURN toString([1]) AS v", nil},
			{"CYPHER 25 RETURN toString([[1,'a']]) AS v", nil},
		}
	}
	for index, tt := range cases {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			expected, re := neo4j.ExecuteQuery(context.Background(), ref, tt.query, tt.params, neo4j.EagerResultTransformer)
			actual, err := newFabricCallRowsExecutor(t).Execute(context.Background(), tt.query, tt.params)
			t.Logf("query=%s params=%v reference=%v actualError=%v actual=%v", tt.query, tt.params, re, err, actual)
			if version25 && index == 0 {
				// Existing NornicDB Cypher25 behaviour, also pinned by
				// TestCypher25ToStringWritesValues. Neo4j2025.12 predates it.
				ne, ok := re.(*neo4j.Neo4jError)
				require.True(t, ok)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", ne.Code)
				require.NoError(t, err)
				require.Equal(t, []string{"v"}, actual.Columns)
				require.Equal(t, [][]interface{}{{"[1, a]"}}, actual.Rows)
				return
			}
			if re != nil {
				ne, ok := re.(*neo4j.Neo4jError)
				require.True(t, ok)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, ne.Code, code)
			} else {
				require.NoError(t, err)
				require.Equal(t, expected.Keys, actual.Columns)
				require.Len(t, actual.Rows, len(expected.Records))
				for i, record := range expected.Records {
					wanted, e := json.Marshal(record.Values)
					require.NoError(t, e)
					got, e := json.Marshal(actual.Rows[i])
					require.NoError(t, e)
					require.JSONEq(t, string(wanted), string(got), tt.query)
				}
			}
		})
	}
}
