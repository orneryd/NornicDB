package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/dynamiclabels"
	"github.com/stretchr/testify/require"
)

func TestDynamicLabelPublicExecute(t *testing.T) {
	for _, normalization := range []string{"true", "false"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization == "true")
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			for _, c := range dynamiclabels.Cases() {
				t.Run(c.Name, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_public"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, dynamiclabels.Fixture, nil)
					require.NoError(t, err)
					for range 2 {
						result, err := exec.Execute(ctx, c.Query, c.Params)
						if c.Code != "" {
							require.Error(t, err)
							requireStatusCode(t, err, c.Code)
							continue
						}
						require.NoError(t, err, c.Query)
						if len(c.Rows) == 0 {
							require.Empty(t, result.Rows, c.Query)
						} else {
							require.Equal(t, c.Rows, result.Rows, c.Query)
						}
					}
				})
			}
		})
	}
}

// Every function emitted by the label rewrite must be known to semantic
// validation and implemented by the evaluator. Test the emitted text rather
// than maintaining another list of internal function names in the test.
func TestDynamicLabelGeneratedFunctionContract(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range dynamiclabels.Cases() {
		if c.Code != "" {
			continue
		}
		query, _ := stripCypherPreamble(c.Query)
		rewritten, _, err := desugarLabelExpressions(query, c.Params, c.Cypher25)
		require.NoError(t, err, c.Query)
		require.NoError(t, forEachFunctionCall(rewritten, func(name, normalized string) error {
			if !strings.HasPrefix(normalized, "__nornic_") {
				return nil
			}
			seen[normalized] = true
			_, known := builtInCypherFunctions[normalized]
			require.True(t, known, "rewrite emitted an unknown function: %s", name)
			_, registered, _ := cypherfn.EvaluateFunction(name, nil, cypherfn.Context{})
			require.True(t, registered, "rewrite emitted an unregistered function: %s", name)
			return nil
		}))
	}
	require.Contains(t, seen, dynamicLabelTestFunction)
	require.Contains(t, seen, dynamicLabelPredicateFunction)
}

// Trust of generated text belongs to one dispatch call, not to the context:
// procedure-supplied statements must still cross the client boundary in PROFILE.
func TestDynamicLabelRoutedClientBoundary(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_boundary"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, dynamiclabels.Fixture, nil)
	require.NoError(t, err)
	for _, prefix := range []string{"", "PROFILE "} {
		_, err := exec.Execute(ctx, prefix+"MATCH (n:$any(['A','B'])) CALL apoc.cypher.run('RETURN __nornic_haslabels(null, [], false) AS v', {}) YIELD value RETURN value", nil)
		require.ErrorContains(t, err, "unknown function: __nornic_haslabels")
		_, err = exec.Execute(ctx, prefix+"RETURN __nornic_haslabels(null, [], false)", nil)
		require.ErrorContains(t, err, "unknown function: __nornic_haslabels")
	}
}

func TestDynamicLabelPublicDatabaseRouting(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase("dynamic_target"))
	require.NoError(t, manager.CreateCompositeDatabase("dynamic_composite", []multidb.ConstituentRef{
		{Alias: "data", DatabaseName: "dynamic_target", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "USE dynamic_target "+dynamiclabels.Fixture, nil)
	require.NoError(t, err)
	for _, query := range []string{
		"USE dynamic_target WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id ORDER BY id",
		"PROFILE USE dynamic_target WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id ORDER BY id",
		"USE dynamic_composite CALL { USE dynamic_composite.data WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id } RETURN id ORDER BY id",
		"PROFILE USE dynamic_composite CALL { USE dynamic_composite.data WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id } RETURN id ORDER BY id",
	} {
		t.Run(query, func(t *testing.T) {
			for _, c := range []struct {
				labels []any
				rows   [][]any
			}{
				{[]any{"A"}, [][]any{{int64(1)}, {int64(2)}}},
				{[]any{"B"}, [][]any{{int64(2)}, {int64(3)}}},
				{[]any{"A"}, [][]any{{int64(1)}, {int64(2)}}},
			} {
				result, err := exec.Execute(ctx, query, map[string]any{"labels": c.labels})
				require.NoError(t, err)
				require.Equal(t, c.rows, result.Rows)
			}
		})
	}
}

// A remote Fabric fragment must arrive as supported Cypher at the Neo4j
// evaluator; NornicDB's local lowering functions are not remote functions.
func TestDynamicLabelPublicRemoteFabric(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if uri == "" {
		t.Skip("Neo4j reference not configured")
	}
	ctx := context.Background()
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: uri, Database: "neo4j"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	_, _, err = remote.QueryCypher(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	_, _, err = remote.QueryCypher(ctx, dynamiclabels.Fixture, nil)
	require.NoError(t, err)
	manager, err := multidb.NewDatabaseManager(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), &multidb.Config{
		RemoteEngineFactory: func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateCompositeDatabase("dynamic_remote", []multidb.ConstituentRef{
		{Alias: "data", DatabaseName: "neo4j", URI: uri, Type: "remote", AccessMode: "read_write", AuthMode: "oidc_forwarding"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, form := range []string{"$(ls)", "$all(ls)", "$any(ls)"} {
		t.Run(form, func(t *testing.T) {
			want := [][]any{{"2"}}
			if form == "$any(ls)" {
				want = [][]any{{"1"}, {"2"}, {"3"}}
			}
			query := "USE dynamic_remote CALL { USE dynamic_remote.data WITH $labels AS ls MATCH (n:" + form + ") RETURN toString(n.id) AS id } RETURN id ORDER BY id"
			result, err := exec.Execute(ctx, query, map[string]any{"labels": []any{"A", "B"}})
			require.NoError(t, err)
			require.Equal(t, want, result.Rows)
		})
	}
}
