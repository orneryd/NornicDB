package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFabricLocalBindingKeywordContracts(t *testing.T) {
	prior := config.GetParserType()
	defer config.SetParserType(prior)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			for _, normalized := range []bool{true, false} {
				t.Run(fmt.Sprintf("normalization_%v", normalized), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(normalized)
					for _, tc := range []struct {
						name, body string
						value      interface{}
					}{
						{"path_create", "CREATE (s)-[:FabricCompRel]->(:FabricCompTarget {v:7}) RETURN [create = (s)-[:FabricCompRel]->(t) | length(create)] AS k", []interface{}{int64(1)}},
						{"path_match", "CREATE (s)-[:FabricCompRel]->(:FabricCompTarget {v:7}) RETURN [match = (s)-[:FabricCompRel]->(t) | length(match)] AS k", []interface{}{int64(1)}},
						{"path_merge", "CREATE (s)-[:FabricCompRel]->(:FabricCompTarget {v:7}) RETURN [merge = (s)-[:FabricCompRel]->(t) | length(merge)] AS k", []interface{}{int64(1)}},
						{"reduce_contains", "RETURN reduce(acc=0,contains IN [7] | acc+contains) AS k", int64(7)},
						{"reduce_in", "RETURN reduce(acc=0,in IN [7] | acc+in) AS k", int64(7)},
						{"reduce_is", "RETURN reduce(acc=0,is IN [7] | acc+is) AS k", int64(7)},
						{"reduce_not", "RETURN reduce(acc=0,not IN [7] | acc+not) AS k", int64(7)},
						{"foreach_not", "FOREACH (not IN [7] | CREATE (:FabricStructureForeach {v:not})) RETURN 7 AS k", int64(7)},
						{"map_false", "WITH {v:0} AS m,7 AS false RETURN m{false}.false AS k", int64(7)},
						{"map_null", "WITH {v:0} AS m,7 AS null RETURN m{null}.null AS k", int64(7)},
						{"map_true", "WITH {v:0} AS m,7 AS true RETURN m{true}.true AS k", int64(7)},
					} {
						t.Run(tc.name, func(t *testing.T) {
							base := newFabricCallRowsExecutor(t)
							fabricRunModes(t, base, func(t *testing.T, exec *StorageExecutor) {
								result, err := exec.Execute(t.Context(), "USE comp CALL { USE comp.one "+tc.body+" } RETURN k", nil)
								require.NoError(t, err, tc.body)
								require.Equal(t, []string{"k"}, result.Columns)
								require.Equal(t, [][]interface{}{{tc.value}}, result.Rows)
							})
						})
					}
				})
			}
		})
	}
}
