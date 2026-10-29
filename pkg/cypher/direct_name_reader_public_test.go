package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1486 lane (Codex). These consumers must read one decoded identity,
// including literal backticks, rather than stripping source delimiters.
func TestDirectNamePropertyReaderConsumers(t *testing.T) {
	for _, normalize := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization=%t", normalize), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalize)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			for _, name := range []string{"ordinary", " ", "  ", "\t", "a.b", "a`b", "`a`"} {
				t.Run(name, func(t *testing.T) {
					engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "direct_property")
					_, err := engine.CreateNode(&storage.Node{ID: "one", Labels: []string{"DirectProperty"}, Properties: map[string]any{name: int64(7)}})
					require.NoError(t, err)
					executor := NewStorageExecutor(engine)
					source := cyphertext.QuoteName(name)
					query := "MATCH (" + source + ":DirectProperty) RETURN exists(" + source + "." + source + ") AS present"
					result, err := executor.Execute(context.Background(), query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]any{{true}}, result.Rows, query)
				})
			}
		})
	}
}

func TestDirectNamePathParameterReader(t *testing.T) {
	executor, ctx := newPathSelectorExecutor(t)
	for _, name := range []string{"ordinary", "a`b", "`a`"} {
		t.Run(name, func(t *testing.T) {
			query := "MATCH p = SHORTEST $" + cyphertext.QuoteName(name) + " (a:SP {id:1})-->+(b:SP {id:3}) RETURN length(p) AS l ORDER BY l"
			result, err := executor.Execute(ctx, query, map[string]any{name: int64(2)})
			require.NoError(t, err, query)
			require.Equal(t, [][]any{{int64(1)}, {int64(2)}}, result.Rows, query)
		})
	}
}

func TestDirectNameProcedureRoleReader(t *testing.T) {
	for _, name := range []string{"ordinary", "a`b", "`a`"} {
		t.Run(name, func(t *testing.T) {
			system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
			ctx := context.Background()
			require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, name))
			executor := NewStorageExecutor(system)
			source := cyphertext.QuoteName(name)
			t.Run("GRANT", func(t *testing.T) {
				_, err := executor.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.direct ON DBMS TO "+source, nil)
				require.NoError(t, err)
				require.True(t, allowsBoosted(t, system, name, "db.direct"))
			})
			// The public store establishes the same grant independently, so
			// a GRANT reader failure cannot hide the SHOW reader assertion.
			require.NoError(t, auth.NewProcedurePrivilegeStore(system).Grant(ctx, name, "db.direct", true, false))
			t.Run("SHOW", func(t *testing.T) {
				result, err := executor.Execute(ctx, "SHOW ROLE "+source+" PRIVILEGES YIELD role RETURN role", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{name}}, result.Rows)
			})
		})
	}
}

func TestDirectNameWriteStretchPropertyIdentity(t *testing.T) {
	for _, name := range []string{"ordinary", "a`b", "`a`", "a:b"} {
		t.Run(name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "direct_stretch"))
			ctx := context.Background()
			source := cyphertext.QuoteName(name)
			_, err := executor.Execute(ctx, "CREATE (:DirectStretch {"+source+":0})", nil)
			require.NoError(t, err)
			query := "UNWIND [1,2] AS k MATCH (n:DirectStretch {" + source + ":0}) SET n." + source + "=k RETURN k ORDER BY k"
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(1)}, {int64(2)}}, result.Rows, query)
		})
	}
}

func TestDirectNameCorrelatedLookupStringValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "direct_lookup_values"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DirectLookupValue {k:'x',v:7}), (:DirectLookupValue {k:' x ',v:9})", nil)
	require.NoError(t, err)
	rows, err := exec.Execute(ctx, "UNWIND ['x',' x '] AS key CALL (key) { MATCH (n:DirectLookupValue) WHERE n.k=key RETURN n.v AS v } RETURN key,v", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]any{{"x", int64(7)}, {" x ", int64(9)}}, rows.Rows)
}
