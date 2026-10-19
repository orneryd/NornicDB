package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalOuterSemantics(t *testing.T) {
	ctx := context.Background()
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	cases := []struct {
		name, query string
		rows        [][]interface{}
		unit        bool
	}{
		{"returning_empty", "WITH 1 AS x CALL { USE comp.one MATCH (n:ReviewXhighOuterMissing) RETURN n } RETURN x,n", nil, false},
		{"optional_empty", "WITH 1 AS x OPTIONAL CALL { USE comp.one MATCH (n:ReviewXhighOuterMissing) RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
		{"optional_partial", "UNWIND [1,2] AS x OPTIONAL CALL (x) { USE comp.one WITH x WHERE x=1 RETURN x AS y } RETURN x,y ORDER BY x", [][]interface{}{{int64(1), int64(1)}, {int64(2), nil}}, false},
		{"optional_union_empty", "WITH 1 AS x OPTIONAL CALL { USE comp.one MATCH (n:ReviewXhighOuterMissing) RETURN n UNION USE comp.one MATCH (n:ReviewXhighOuterMissing) RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
		{"nested_optional_empty", "WITH 1 AS x CALL { OPTIONAL CALL { USE comp.one MATCH (n:ReviewXhighOuterMissing) RETURN n } RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
		{"optional_match_control", "WITH 1 AS x CALL { USE comp.one OPTIONAL MATCH (n:ReviewXhighOuterMissing) RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
		{"unit_writes", "UNWIND [1,2] AS x CALL (x) { USE comp.one UNWIND [1,2] AS y CREATE (:ReviewXhighOuterUnit {x:x,y:y}) } RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(2)}}, true},
	}
	driver := fabricCallRowsReference(t)
	if driver != nil {
		t.Cleanup(func() {
			_, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:ReviewXhighOuterUnit) DETACH DELETE n", nil, neo4j.EagerResultTransformer)
			assert.NoError(t, err)
		})
		for _, tc := range cases {
			t.Run("reference/"+tc.name, func(t *testing.T) {
				query := strings.ReplaceAll(tc.query, "USE comp.one ", "")
				got, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err, query)
				require.Len(t, got.Records, len(tc.rows), query)
				for i, row := range got.Records {
					assert.Equal(t, tc.rows[i], row.Values, query)
				}
			})
		}
	}
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, composite := range []bool{false, true} {
				for _, tc := range cases {
					t.Run(fmt.Sprintf("normalize_%v/explicit_%v/composite_%v/%s", normalize, explicit, composite, tc.name), func(t *testing.T) {
						exec := newFabricCallRowsExecutor(t)
						if composite {
							exec = fabricCallRowsCompositeExecutor(t, exec)
						}
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
							if composite {
								_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
								require.True(t, ok, "real composite transaction required")
							}
						}
						query := tc.query
						if !composite {
							query = strings.ReplaceAll(query, "USE comp.one ", "")
						}
						got, err := exec.Execute(ctx, query, nil)
						if assert.NoError(t, err, query) {
							assert.Len(t, got.Rows, len(tc.rows), "query=%s rows=%v", query, got.Rows)
							if len(got.Rows) == len(tc.rows) && len(tc.rows) > 0 {
								assert.Equal(t, tc.rows, got.Rows, query)
							}
						}
						if tc.unit {
							countQuery := "MATCH (n:ReviewXhighOuterUnit) RETURN count(n) AS c"
							if composite {
								countQuery = "USE comp.one " + countQuery
							}
							count, err := exec.Execute(ctx, countQuery, nil)
							if assert.NoError(t, err, countQuery) {
								assert.Equal(t, [][]interface{}{{int64(4)}}, count.Rows, "writes must survive independently of returned outer rows")
							}
						}
					})
				}
			}
		}
	}
}
