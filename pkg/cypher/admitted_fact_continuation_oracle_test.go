package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// The source census found these independent routes through the same element
// fact owner. The actual reference decides both compile/runtime rejection and
// values, including an empty outer stream and an empty pattern result.
func TestAdmittedFactContinuationOracle(t *testing.T) {
	ref := fabricCallRowsReference(t)
	require.NotNil(t, ref)
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, shape := range []struct{ name, bindings, expression string }{
		{"pattern_reduce", "", "[zs IN reduce(a=[[x]], ignored IN [(n)-->(m)|1] | a) | [z IN zs | abs(z)]]"},
		{"alias_head", "WITH [[x]] AS lists ", "abs(head(head(lists)))"},
		{"alias_last", "WITH [[x]] AS lists ", "abs(last(last(lists)))"},
		{"alias_mixed", "WITH [[x]] AS lists ", "abs(head(last(lists)))"},
		{"map_direct", "", "[m IN [{k:1}] | labels(m)]"},
		{"map_alias", "WITH [{k:1}] AS lists ", "labels(head(lists))"},
		{"map_nested", "WITH [[{k:1}]] AS lists ", "labels(head(head(lists)))"},
		{"map_index", "WITH [[{k:1}]] AS lists ", "labels(lists[0][0])"},
		{"map_slice", "WITH [[{k:1}]] AS lists ", "labels(lists[0..1][0][0])"},
		{"map_reduce", "", "labels(head(reduce(a=[{k:1}], ignored IN [] | a)))"},
		{"pattern_projection", "", "[zs IN [(n)-->(m)|[x]] | [z IN zs | abs(z)]]"},
		{"pattern_function", "", "[(n)-->(m)|abs(x)]"},
		{"mixed_numeric", "", "[n IN [1,2.5] | toUpper(n)]"},
	} {
		for _, empty := range []bool{false, true} {
			for _, value := range []interface{}{true, int64(2), nil} {
				query := "WITH $p AS x " + shape.bindings
				if empty {
					query += "UNWIND $empty AS stop "
				}
				query += "RETURN " + shape.expression + " AS v"
				params := map[string]interface{}{"p": value, "empty": []interface{}{}}
				expected, referenceError := neo4j.ExecuteQuery(context.Background(), ref, query, params, neo4j.EagerResultTransformer)
				for _, norm := range []bool{false, true} {
					config.SetCypherQueryNormalizationEnabled(norm)
					for _, route := range []string{"native", "use"} {
						for _, transaction := range []bool{false, true} {
							t.Run(fmt.Sprintf("%s/empty_%v/value_%v/norm_%v/%s/tx_%v", shape.name, empty, value, norm, route, transaction), func(t *testing.T) {
								e := newFabricCallRowsExecutor(t)
								text := query
								ctx := context.Background()
								if route == "use" {
									e = fabricCallRowsCompositeExecutor(t, e)
									text = "USE comp.one " + text
								}
								if transaction {
									_, err := e.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									defer e.Execute(ctx, "ROLLBACK", nil)
								}
								actual, err := e.Execute(ctx, text, params)
								if referenceError != nil {
									require.Error(t, err, "query=%s params=%v reference=%v actual=%v", text, params, referenceError, actual)
									ne, ok := referenceError.(*neo4j.Neo4jError)
									require.True(t, ok)
									code, _ := nornicerrors.Neo4jStatus(err)
									require.Equal(t, ne.Code, code, "query=%s actual=%v reference=%v", text, err, referenceError)
								} else {
									require.NoError(t, err, text)
									require.Equal(t, expected.Keys, actual.Columns)
									require.Len(t, actual.Rows, len(expected.Records))
									for index, record := range expected.Records {
										a, e := json.Marshal(record.Values)
										require.NoError(t, e)
										b, e := json.Marshal(actual.Rows[index])
										require.NoError(t, e)
										require.JSONEq(t, string(a), string(b), "query=%s", text)
									}
								}
							})
						}
					}
				}
			}
		}
	}
}
