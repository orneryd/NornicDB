package heimdall

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type watcherPolicyRouter struct {
	memoryDBRouter
	executor                *cypher.StorageExecutor
	calls                   int
	lastContext             context.Context
	lastDatabase, lastQuery string
	lastParams              map[string]interface{}
}

func (r *watcherPolicyRouter) Query(ctx context.Context, database, query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	r.calls++
	r.lastContext = ctx
	r.lastDatabase = database
	r.lastQuery = query
	r.lastParams = params
	if err := cypher.AuthorizeQuery(ctx, query); err != nil {
		return nil, err
	}
	result, err := r.executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = map[string]interface{}{}
		for j, column := range result.Columns {
			rows[i][column] = row[j]
		}
	}
	return rows, nil
}

func TestWatcherPublicQueryUsesGrammarPolicy(t *testing.T) {
	for _, writer := range []bool{false, true} {
		for _, tc := range []struct {
			name, query string
			allow       bool
		}{
			{"ordinary", "RETURN $value AS marker", true},
			{"confusable identifier", "WITH 7 AS REVOKE RETURN REVOKE AS marker", true},
			{"write word data", "RETURN 'CREATE // literal' AS marker", true},
			{"quoted identifier", "WITH 7 AS `DELETE` RETURN `DELETE` AS marker", true},
			{"known read procedure", "CALL db.labels()", true},
			{"admin metadata", "SHOW USERS", false},
			{"explained then admin metadata", "EXPLAIN CREATE (:WatcherPolicyProbe); SHOW USERS", false},
			{"unknown procedure", "CALL watcher_unknown.procedure()", false},
			{"dynamic procedure", "CALL apoc.cypher.run('CREATE (:WatcherPolicyProbe)', {})", false},
			{"explained write", "EXPLAIN CREATE (:WatcherPolicyProbe)", true},
			{"profile write", "PROFILE CREATE (:WatcherPolicyProbe)", false},
			{"explained then write", "EXPLAIN CREATE (:WatcherPolicyProbe); CREATE (:WatcherPolicyProbe)", false},
			{"read then write", "RETURN 1; CREATE (:WatcherPolicyProbe)", false},
			{"ordinary write", "CREATE (:WatcherPolicyProbe)", false},
			{"comment-looking string hides write", "WITH 'https://host' AS value CREATE (:WatcherPolicyProbe) RETURN value", false},
			{"block comment write", "/* safe */ CREATE (:WatcherPolicyProbe)", false},
			{"malformed", "RETURN (", false},
			{"prefix without body", "EXPLAIN", false},
			{"read then empty prefix", "RETURN 1; EXPLAIN", false},
		} {
			t.Run(fmt.Sprintf("%s/writer=%v", tc.name, writer), func(t *testing.T) {
				store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				router := &watcherPolicyRouter{executor: cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "default"))}
				p := &WatcherPlugin{}
				require.NoError(t, p.Initialize(heimdall.SubsystemContext{Config: heimdall.Config{Model: "test-model"}}))
				roles := []string{"watcher-reader"}
				if writer {
					roles = []string{"watcher-writer"}
				}
				ctx := auth.WithRequestPrincipalRoles(t.Context(), roles)
				ctx = cypher.WithDatabasePermissionResolver(ctx, "default", func(_, permission string) bool { return permission == "read" || writer && permission == "write" })
				action := p.Actions()["query"]
				require.False(t, action.External, "watcher query remains internal")
				result, err := action.Handler(heimdall.ActionContext{Context: ctx, Database: router, Params: map[string]interface{}{"database": "default", "cypher": tc.query, "params": map[string]interface{}{"value": "ordinary-control"}}})
				require.NoError(t, err)
				wantSuccess := tc.allow
				if tc.name == "explained write" && !writer {
					wantSuccess = false
				}
				assert.Equal(t, wantSuccess, result.Success)
				if tc.allow {
					require.Equal(t, 1, router.calls)
					require.Same(t, ctx, router.lastContext)
					require.Equal(t, roles, auth.RequestPrincipalRolesFromContext(router.lastContext))
					require.Equal(t, "default", router.lastDatabase)
					require.Equal(t, tc.query, router.lastQuery)
					require.Equal(t, map[string]interface{}{"value": "ordinary-control"}, router.lastParams)
					if result.Success {
						rows := result.Data["rows"].([]map[string]interface{})
						switch tc.name {
						case "ordinary":
							require.Equal(t, "ordinary-control", rows[0]["marker"])
						case "confusable identifier", "quoted identifier":
							require.Equal(t, int64(7), rows[0]["marker"])
						case "write word data":
							require.Equal(t, "CREATE // literal", rows[0]["marker"])
						}
					}
				} else {
					assert.Zero(t, router.calls, "policy must reject before general execution")
				}
				count, err := router.executor.Execute(t.Context(), "MATCH (n:WatcherPolicyProbe) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, int64(0), count.Rows[0][0], "rejected writes must not mutate graph")
			})
		}
	}

}

func TestWatcherPolicyFixtureHasActualWriteAuthority(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "default"))
	ctx := cypher.WithDatabasePermissionResolver(t.Context(), "default", func(_, permission string) bool { return permission == "read" || permission == "write" })
	_, err := executor.Execute(ctx, "CREATE (:WatcherPrincipalControl)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "WITH 'https://host' AS value CREATE (:WatcherPolicyProbe) RETURN value", nil)
	require.NoError(t, err)
}
