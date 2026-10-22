package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The exported Fabric query protocol accepts executable text. Its standalone
// calls must admit that text even when the caller carries another statement's
// program. Coordinator fragments are a separate source-preserving contract.
// Exercise the same source forms through ordinary public Execute as controls.
func TestRelationshipAccessProgramExecutableInterfaces(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "admission_interfaces")
	exec := NewStorageExecutor(store)
	for _, query := range []string{
		"CREATE (:Key {p:'x'})-[:T {p:'x'}]->()",
		"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	adapter := &cypherFabricExecutor{base: exec, autoCommit: true}
	var protocol fabric.CypherExecutor = adapter
	local := fabric.NewLocalFragmentExecutor(protocol, func(string) (storage.Engine, error) { return store, nil })
	for _, tc := range []struct {
		name, query string
		want        interface{}
		seek        bool
	}{
		{"scalar", "RETURN $x AS value", "x", false},
		{"equality", "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS value", "x", true},
		{"call", "CALL { MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS value } RETURN value", "x", true},
		{"union", "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS value UNION MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS value", "x", true},
		{"count", "RETURN COUNT { MATCH ()-[r:T]->() WHERE r.p=$x } AS value", int64(1), true},
		{"exists", "MATCH (a:Key) RETURN EXISTS { (a)-[r:T {p:$x}]->() } AS value", true, true},
		{"collect", "RETURN head(COLLECT { MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p }) AS value", "x", true},
		{"comprehension", "MATCH (a:Key) RETURN head([(a)-[r:T]->() WHERE r.p=$x | r.p]) AS value", "x", true},
		{"pattern", "MATCH (a:Key) WHERE (a)-[:T]->() RETURN true AS value", true, false},
		{"shortest", "MATCH (a:Key)-[:T]->(b) RETURN length(shortestPath((a)-[:T*]->(b))) AS value", int64(1), false},
		{"projection", "MATCH (a:Key) WITH [a] AS nodes RETURN nodes[0].p AS value", "x", false},
	} {
		for _, parent := range []string{"fresh", "other_statement"} {
			for _, entry := range []string{"execute", "query", "query_with_record", "local", "local_rows"} {
				t.Run(tc.name+"/"+parent+"/"+entry, func(t *testing.T) {
					ctx := context.Background()
					if parent == "other_statement" {
						var fixtureAdmission828 admittedRelationshipStatement
						ctx, fixtureAdmission828 = compileRelationshipStatement(ctx, "RETURN 0 AS unrelated", nil)
						_ = fixtureAdmission828
					}
					ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
					params := map[string]interface{}{"x": "x"}
					require.NotPanics(t, func() {
						var columns []string
						var rows [][]interface{}
						var err error
						switch entry {
						case "execute":
							result, runErr := exec.Execute(ctx, tc.query, params)
							require.NoError(t, runErr)
							columns, rows = result.Columns, result.Rows
						case "query":
							columns, rows, err = protocol.ExecuteQuery(ctx, "admission_interfaces", store, tc.query, params)
						case "query_with_record":
							columns, rows, err = protocol.ExecuteQueryWithRecord(ctx, "admission_interfaces", store, tc.query, params, nil)
						case "local":
							result, runErr := local.Execute(ctx, &fabric.LocationLocal{DBName: "admission_interfaces"}, tc.query, params)
							require.NoError(t, runErr)
							columns, rows = result.Columns, result.Rows
						case "local_rows":
							var iterator fabric.RowIterator
							columns, iterator, err = local.ExecuteRows(ctx, &fabric.LocationLocal{DBName: "admission_interfaces"}, tc.query, params)
							require.NoError(t, err)
							defer iterator.Close()
							for iterator.Next() {
								rows = append(rows, iterator.Row())
							}
							err = iterator.Err()
						}
						require.NoError(t, err)
						require.Equal(t, []string{"value"}, columns)
						require.Equal(t, [][]interface{}{{tc.want}}, rows)
						profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
						require.Empty(t, profile.unregistered, "a new executable query owns its own source sites")
						if tc.seek {
							seek := profile.seeks["r\x00rel_p"]
							require.NotNil(t, seek, "correct rows alone must not hide a missing source site")
							require.Positive(t, seek.lookups)
						}
					})
				})
			}
		}
	}
}
