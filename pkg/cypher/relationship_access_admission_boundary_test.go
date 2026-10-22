package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipAccessProgramFabricSourceAuthority(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "fabric_source")
	exec := NewStorageExecutor(store)
	for _, query := range []string{
		"CREATE ()-[:T {p:'x'}]->()",
		"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	const body = "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS p"
	const coordinator = "CALL { " + body + " } RETURN p"
	for _, registered := range []bool{true, false} {
		for _, ambient := range []bool{false, true} {
			for _, entry := range []string{"query", "record", "local"} {
				t.Run(entry+map[bool]string{true: "/registered", false: "/removed"}[registered]+map[bool]string{true: "/ambient", false: "/fresh"}[ambient], func(t *testing.T) {
					source := relationshipProgramFor(coordinator)
					if !registered {
						source = &relationshipAccessProgram{query: coordinator, byID: map[string]*relationshipAccessTemplate{}}
					}
					// Captured source belongs to the coordinator, independently of
					// whatever context a callback supplies to the public protocol.
					adapter := &cypherFabricExecutor{base: exec, autoCommit: true, source: source}
					var protocol fabric.CypherExecutor = adapter
					ctx := context.Background()
					if ambient {
						var fixtureAdmission827 admittedRelationshipStatement
						ctx, fixtureAdmission827 = compileRelationshipStatement(ctx, "RETURN 0 AS unrelated", nil)
						_ = fixtureAdmission827
					}
					ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
					params := map[string]interface{}{"x": "x"}
					var columns []string
					var rows [][]interface{}
					var err error
					switch entry {
					case "query":
						columns, rows, err = protocol.ExecuteQuery(ctx, "fabric_source", store, body, params)
					case "record":
						columns, rows, err = protocol.ExecuteQueryWithRecord(ctx, "fabric_source", store, body, params, nil)
					case "local":
						local := fabric.NewLocalFragmentExecutor(protocol, func(string) (storage.Engine, error) { return store, nil })
						result, runErr := local.Execute(ctx, &fabric.LocationLocal{DBName: "fabric_source"}, body, params)
						require.NoError(t, runErr)
						columns, rows = result.Columns, result.Rows
					}
					require.NoError(t, err)
					require.Equal(t, []string{"p"}, columns)
					require.Equal(t, [][]interface{}{{"x"}}, rows)
					profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
					if registered {
						require.Empty(t, profile.unregistered)
						require.NotNil(t, profile.seeks["r\x00rel_p"])
						require.Equal(t, int64(1), profile.seeks["r\x00rel_p"].lookups)
					} else {
						require.Empty(t, profile.seeks, "lowering cannot recreate a removed source site")
						require.NotEmpty(t, profile.unregistered)
					}
				})
			}
		}
	}
}

func TestRelationshipAccessProgramInlineRowSources(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_sources"))
	for _, query := range []string{
		"CREATE (:Key {p:'x'})-[:T {p:'x'}]->()",
		"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, query := range []string{
		"UNWIND $keys AS k MATCH ()-[r:T {p:k}]->() RETURN r.p AS p",
		"UNWIND $keys AS k MATCH (a:Key {p:k})-[:T]->() RETURN a.p AS p",
		"UNWIND $keys AS k MATCH (a:Key {p:k})-[r:T {p:k}]->() RETURN r.p AS p",
	} {
		for _, empty := range []bool{false, true} {
			keys := []interface{}{"x", "missing"}
			if empty {
				keys = nil
			}
			for _, mode := range []string{"", "PROFILE "} {
				result, err := exec.Execute(context.Background(), mode+query, map[string]interface{}{"keys": keys})
				require.NoError(t, err, mode+query)
				if empty {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
				}
			}
		}
	}
}
