package cypher

// Unhandled statements are the terminal chokepoint of the converged router:
// a statement that passes syntax validation but matches no handler is
// rejected with the proper Neo4j-class error (syntax error) in every route
// and transaction mode, with no observable effects and no alternate
// execution (#4 in the convergence plan: error-first, no fallback).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnhandledStatementsReturnClassifiedSyntaxErrors(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	cases := []struct {
		stmt   string
		errMsg string
	}{
		// Valid keyword that no handler claims: the router terminal.
		{stmt: "SHOW WHATEVER", errMsg: "unsupported query type: SHOW"},
		// Invalid start keyword: the syntax-start terminal.
		{stmt: "WHATEVER 1", errMsg: "query must start with a valid clause"},
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()

					_, err := exec.Execute(ctx, "CREATE (:T)", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}

					res, err := exec.Execute(ctx, tc.stmt, nil)
					require.Error(t, err)
					require.Nil(t, res, "unhandled statement must not return rows")
					require.Contains(t, err.Error(), tc.errMsg)

					var classified interface{ BoltErrorCode() string }
					require.ErrorAs(t, err, &classified, "terminal error must carry a Neo4j Bolt classification")
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())

					if mode == "explicit transaction" {
						// The failed statement marks the transaction failed
						// (#683): COMMIT rolls it back and reports it.
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.Error(t, err)
						require.Contains(t, err.Error(), "TransactionMarkedAsFailed")
					}

					// The rejected statement leaves the graph unchanged.
					count, err := exec.storage.NodeCount()
					require.NoError(t, err)
					require.Equal(t, int64(1), count, "unhandled statement must have no observable effects")
				})
			}
		}
	}
}
