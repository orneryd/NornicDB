package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nestedUseRouterExecutor is administrationReviewExecutor with a second user
// database, "other", so a nested USE can name a database that is neither the
// session's nor the system database.
func nestedUseRouterExecutor(t *testing.T, database string) (*StorageExecutor, map[string]storage.Engine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	stores := map[string]storage.Engine{}
	for _, name := range []string{"system", "nornic", "other"} {
		stores[name] = storage.NewNamespacedEngine(base, name)
	}
	manager := administrationReviewManager{stores: stores}
	exec := NewStorageExecutor(stores[database])
	exec.SetDatabaseManager(manager)
	return exec, stores
}

// A statement a procedure runs (apoc.cypher.run and its relatives) that
// starts with USE runs through the one database router, executeOnDatabase:
// the system database's statement policy and the one-database-per-
// transaction rule apply to it as they do to a top-level USE, and it runs
// on the database it names, never on the caller's. A read of another user
// database outside a transaction stays a NornicDB extension
// (TestUseTargetAuthorizationOnEveryRoute).
func TestNestedUseRunsThroughDatabaseRouter(t *testing.T) {
	cases := []struct {
		name, session, target string
		tx                    bool
		code                  string // expected status code, "" for success
	}{
		{"user tx into system", "nornic", "system", true, "Neo.ClientError.Statement.SemanticError"},
		{"user auto-commit into system", "nornic", "system", false, "Neo.ClientError.Statement.SemanticError"},
		{"user tx into other user database", "nornic", "other", true, "Neo.ClientError.Statement.AccessMode"},
		// The statement a procedure or subquery runs belongs to the caller's
		// transaction, an implicit one here, so it can't write a second
		// database either.
		{"user auto-commit into other user database", "nornic", "other", false, "Neo.ClientError.Statement.AccessMode"},
		{"user tx into own database", "nornic", "nornic", true, ""},
		{"user auto-commit into own database", "nornic", "nornic", false, ""},
		{"system tx into user database", "system", "nornic", true, "error"},
		{"system auto-commit into user database", "system", "nornic", false, "error"},
	}
	routes := map[string]string{
		"apoc.cypher.run":     "CALL apoc.cypher.run('USE %s CREATE (:NestedUse) RETURN 1 AS one', {}) YIELD value RETURN value",
		"apoc.cypher.doitall": "CALL apoc.cypher.doitall('USE %s CREATE (:NestedUse) RETURN 1 AS one', {}) YIELD value RETURN value",
		"CALL subquery":       "CALL { USE %s CREATE (:NestedUse) } RETURN 1 AS one",
	}
	for route, statement := range routes {
		for _, c := range cases {
			t.Run(route+"/"+c.name, func(t *testing.T) {
				ctx := context.Background()
				exec, stores := nestedUseRouterExecutor(t, c.session)
				if c.tx {
					_, err := exec.handleBegin()
					require.NoError(t, err)
				}
				_, err := exec.Execute(ctx, fmt.Sprintf(statement, c.target), nil)
				if c.code == "error" {
					require.Error(t, err)
				} else if c.code != "" {
					requireStatusCode(t, err, c.code)
				} else {
					require.NoError(t, err)
				}
				if c.tx {
					_, commitErr := exec.handleCommit(t.Context())
					if c.code != "" {
						require.Error(t, commitErr, "a rejected statement fails its transaction")
					} else {
						require.NoError(t, commitErr)
					}
				}
				for name, store := range stores {
					nodes, err := store.GetNodesByLabel("NestedUse")
					require.NoError(t, err)
					want := 0
					if c.code == "" && name == c.target {
						want = 1
					}
					require.Len(t, nodes, want, "database %s", name)
				}
			})
		}
	}
}
