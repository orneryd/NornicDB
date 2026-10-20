package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNativeCommandExecutionKeepsGraphPlanPrecedence(t *testing.T) {
	for _, name := range []string{"index", "constraint", "database", "alias"} {
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "command_keywords"))
			query := "CREATE " + name + " = (:NativeCommandPrecedence)-[:PRECEDENCE]->(:NativeCommandPrecedence) RETURN length(" + name + ") AS n"
			contract, err := readStatementExecutionContract(query)
			require.NoError(t, err)
			require.True(t, contract.writes)
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}

func TestNativeExecutionDomainRejectsGraphOnSystem(t *testing.T) {
	for _, query := range []string{
		"CREATE ROLE new",
		"CREATE USER new SET PASSWORD 'test-password'",
		"CREATE (:NativeCommandDomain)",
		"MERGE (:NativeCommandDomain)",
		"MATCH (n) RETURN n",
		"UNWIND [1] AS x CREATE (:NativeCommandDomain {x:x}) RETURN x",
		"CREATE index = (:NativeCommandDomain)-[:DOMAIN]->(:NativeCommandDomain) RETURN index",
		"CREATE database = (:NativeCommandDomain)-[:DOMAIN]->(:NativeCommandDomain) RETURN database",
		"CREATE alias = (:NativeCommandDomain)-[:DOMAIN]->(:NativeCommandDomain) RETURN alias",
	} {
		for _, route := range []string{"public", "internal"} {
			for _, explicit := range []bool{false, true} {
				t.Run(query+"/"+route+"/"+map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
					ctx := context.Background()
					exec, system := administrationReviewExecutor(t, "system")
					if explicit {
						_, err := exec.handleBegin()
						require.NoError(t, err)
					}
					var err error
					if route == "public" {
						_, err = exec.Execute(ctx, query, nil)
					} else {
						_, err = exec.executeInternal(ctx, query, nil)
					}
					requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.Error(t, err, "a rejected statement fails its transaction")
					}
					nodes, err := system.AllNodes()
					require.NoError(t, err)
					require.Empty(t, nodes, "rejection has no stored effects")
				})
			}
		}
	}
}
