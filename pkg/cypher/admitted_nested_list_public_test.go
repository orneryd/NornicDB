package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Depth is generated structurally; no list-type spelling is enumerated by the
// implementation. Public values/errors are compared to the actual reference.
func TestAdmittedNestedListFramesPublic(t *testing.T) {
	ref := fabricCallRowsReference(t)
	require.NotNil(t, ref)
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for depth := 1; depth <= 3; depth++ {
		list := strings.Repeat("[", depth) + "x" + strings.Repeat("]", depth)
		var comprehension, reduction func(int, string) string
		comprehension = func(level int, source string) string {
			name := fmt.Sprintf("v%d", level)
			body := "abs(" + name + ")"
			if level+1 < depth {
				body = comprehension(level+1, name)
			}
			return "[" + name + " IN " + source + " | " + body + "]"
		}
		reduction = func(level int, source string) string {
			name, accumulator := fmt.Sprintf("v%d", level), fmt.Sprintf("a%d", level)
			body := "abs(" + name + ")"
			if level+1 < depth {
				body = reduction(level+1, name)
			}
			return "reduce(" + accumulator + "=0, " + name + " IN " + source + " | " + accumulator + " + " + body + ")"
		}
		index, slice, head, last := list, list, list, list
		for level := 0; level < depth; level++ {
			index += "[0]"
			slice += "[0..1][0]"
			head = "head(" + head + ")"
			last = "last(" + last + ")"
		}
		for _, shape := range []struct{ name, expression string }{
			{"comprehension", comprehension(0, list)},
			{"reduce", reduction(0, list)},
			{"index", "abs(" + index + ")"},
			{"slice", "abs(" + slice + ")"},
			{"head", "abs(" + head + ")"},
			{"last", "abs(" + last + ")"},
		} {
			for _, empty := range []bool{false, true} {
				query := "WITH $p AS x "
				if empty {
					query += "UNWIND $empty AS stop "
				}
				query += "RETURN " + shape.expression + " AS v"
				for _, value := range []interface{}{true, int64(2), nil} {
					params := map[string]interface{}{"p": value, "empty": []interface{}{}}
					expected, referenceError := neo4j.ExecuteQuery(context.Background(), ref, query, params, neo4j.EagerResultTransformer)
					for _, norm := range []bool{false, true} {
						config.SetCypherQueryNormalizationEnabled(norm)
						for _, route := range []string{"native", "use"} {
							for _, transaction := range []bool{false, true} {
								t.Run(fmt.Sprintf("depth_%d/%s/empty_%v/value_%v/norm_%v/%s/tx_%v", depth, shape.name, empty, value, norm, route, transaction), func(t *testing.T) {
									e := newFabricCallRowsExecutor(t)
									text := query
									if route == "use" {
										e = fabricCallRowsCompositeExecutor(t, e)
										text = "USE comp.one " + text
									}
									ctx := context.Background()
									if transaction {
										_, err := e.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer e.Execute(ctx, "ROLLBACK", nil)
									}
									actual, err := e.Execute(ctx, text, params)
									if referenceError != nil {
										require.Error(t, err, "query=%s result=%v reference=%v", text, actual, referenceError)
										ne, ok := referenceError.(*neo4j.Neo4jError)
										require.True(t, ok)
										code, _ := nornicerrors.Neo4jStatus(err)
										require.Equal(t, ne.Code, code, "query=%s actual=%v reference=%v", text, err, referenceError)
									} else {
										require.NoError(t, err, text)
										require.Equal(t, expected.Keys, actual.Columns)
										require.Len(t, actual.Rows, len(expected.Records))
										for index, record := range expected.Records {
											expectedJSON, marshalError := json.Marshal(record.Values)
											require.NoError(t, marshalError)
											actualJSON, marshalError := json.Marshal(actual.Rows[index])
											require.NoError(t, marshalError)
											require.JSONEq(t, string(expectedJSON), string(actualJSON), "query=%s", text)
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
}
