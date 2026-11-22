package cypher

import (
	"regexp"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-83 / D16 reproduction and classification. The reproduction is accepted
// on a user database by both parsers and both transaction modes:
//
//	SHOW TRANSACTIONS YIELD transactionId WHERE false
//	  CALL { RETURN 1 AS c } RETURN transactionId
//
// The composition mechanism is deliberate for Cypher 25: CypherParser.g4's
// showTail allows a regularQuery after YIELD ("Cypher 25 composition: any
// query over the yielded columns"), and executor_show.go's applyShowTail names
// WITH, UNWIND, MATCH, CALL and writes. The system database deliberately
// rejects those clauses (docs/compliance/rbac.md and TestSystemShowTailError),
// while the CHANGELOG records only the RETURN form.
//
// The canonical composition fixture (show_composition_test.go) exercises the
// continuation under CYPHER 25 and attributes the answers to Neo4j 2026.09
// (#907). The issue's statement has no preamble, so each accepted case here
// runs as a default statement, CYPHER 5 and CYPHER 25; all three currently
// take the same path. The D16 reference rejects CALL. Whether the wider
// default/Cypher 5 scope is a supported extension or accidental
// over-acceptance is the pending NOR-83 policy decision. These tests pin
// current behavior and error classes; they do not assert a new product parity
// rule.
//
// The issue places the corresponding TERMINATE form in population G. That
// population is not present in the D16 attachment recovered from Linear, so
// the TERMINATE cases use the grammar-symmetric form with a nonexistent,
// test-owned id: no host or server transaction is terminated.
const (
	nor83AdminContinuationDatabase = "nor83_admin_continuation"
	nor83AbsentTransactionID       = "nor83-terminal-absent-transaction-1"
)

func newNOR83AdminContinuationExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), nor83AdminContinuationDatabase))
}

// TestNOR83TransactionListingContinuations runs the accepted controls, the
// D16 reproductions and the WITH negative reference control through the
// public Execute path. Each parser runs each transaction mode; the row-bearing
// cases read a transaction owned by this fixture and prove the continuation
// executes, not merely parses.
func TestNOR83TransactionListingContinuations(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newNOR83AdminContinuationExecutor(t)
		forEachTraversalTransactionMode(t, exec, func(t *testing.T) {
			ownedID := regexp.MustCompile(`^` + regexp.QuoteMeta(nor83AdminContinuationDatabase) + `-transaction-[0-9]+$`)
			for _, tc := range []struct {
				name    string
				query   string
				columns []string
				check   func(*testing.T, *ExecuteResult)
			}{
				{
					name:    "show_yield_where_return_control",
					query:   `SHOW TRANSACTIONS YIELD transactionId, currentQuery, status WHERE false RETURN transactionId, currentQuery, status`,
					columns: []string{"transactionId", "currentQuery", "status"},
					check:   func(t *testing.T, result *ExecuteResult) { require.Empty(t, result.Rows) },
				},
				{
					name:    "show_call_reproduction",
					query:   `SHOW TRANSACTIONS YIELD transactionId WHERE false CALL { RETURN 1 AS c } RETURN transactionId`,
					columns: []string{"transactionId"},
					check:   func(t *testing.T, result *ExecuteResult) { require.Empty(t, result.Rows) },
				},
				{
					name: "show_call_continuation_executes",
					query: `SHOW TRANSACTIONS YIELD transactionId, database WHERE database = '` + nor83AdminContinuationDatabase +
						`' CALL { RETURN 1 AS c } RETURN transactionId, c`,
					columns: []string{"transactionId", "c"},
					check: func(t *testing.T, result *ExecuteResult) {
						require.Len(t, result.Rows, 1)
						require.Regexp(t, ownedID, result.Rows[0][0])
						require.Equal(t, int64(1), result.Rows[0][1])
					},
				},
				{
					name:    "show_with_negative_reference_control",
					query:   `SHOW TRANSACTIONS YIELD transactionId WHERE false WITH transactionId RETURN transactionId`,
					columns: []string{"transactionId"},
					check:   func(t *testing.T, result *ExecuteResult) { require.Empty(t, result.Rows) },
				},
				{
					name: "show_with_continuation_executes",
					query: `SHOW TRANSACTIONS YIELD transactionId, database WHERE database = '` + nor83AdminContinuationDatabase +
						`' WITH transactionId, database RETURN transactionId`,
					columns: []string{"transactionId"},
					check: func(t *testing.T, result *ExecuteResult) {
						require.Len(t, result.Rows, 1)
						require.Regexp(t, ownedID, result.Rows[0][0])
					},
				},
				{
					name: "terminate_yield_where_return_control",
					query: `TERMINATE TRANSACTIONS '` + nor83AbsentTransactionID +
						`' YIELD transactionId WHERE transactionId = '` + nor83AbsentTransactionID + `' RETURN transactionId`,
					columns: []string{"transactionId"},
					check: func(t *testing.T, result *ExecuteResult) {
						require.Equal(t, [][]interface{}{{nor83AbsentTransactionID}}, result.Rows)
					},
				},
				{
					name: "terminate_call_reproduction",
					query: `TERMINATE TRANSACTIONS '` + nor83AbsentTransactionID +
						`' YIELD transactionId CALL { RETURN 1 AS c } RETURN transactionId, c`,
					columns: []string{"transactionId", "c"},
					check: func(t *testing.T, result *ExecuteResult) {
						require.Equal(t, [][]interface{}{{nor83AbsentTransactionID, int64(1)}}, result.Rows)
					},
				},
				{
					name: "terminate_with_negative_reference_control",
					query: `TERMINATE TRANSACTIONS '` + nor83AbsentTransactionID +
						`' YIELD transactionId WITH transactionId RETURN transactionId`,
					columns: []string{"transactionId"},
					check: func(t *testing.T, result *ExecuteResult) {
						require.Equal(t, [][]interface{}{{nor83AbsentTransactionID}}, result.Rows)
					},
				},
			} {
				for _, version := range []struct {
					name   string
					prefix string
				}{
					{name: "default_statement"},
					{name: "cypher5", prefix: "CYPHER 5 "},
					{name: "cypher25", prefix: "CYPHER 25 "},
				} {
					t.Run(tc.name+"/"+version.name, func(t *testing.T) {
						result, err := exec.Execute(t.Context(), version.prefix+tc.query, nil)
						require.NoError(t, err)
						require.NotNil(t, result)
						require.Equal(t, tc.columns, result.Columns)
						tc.check(t, result)
					})
				}
			}
		})
	})
}

// TestNOR83TransactionListingRejectsInvalidTails pins the existing error class
// for malformed transactional-administration tails. Each case runs in a fresh
// executor and, for the explicit mode, a fresh transaction: a failed statement
// marks its explicit transaction failed, so one failure per transaction keeps
// the returned error the statement's own SyntaxError rather than a later
// poisoned-transaction wrapper. ROLLBACK owns cleanup for the expected
// failure; the test never commits it.
func TestNOR83TransactionListingRejectsInvalidTails(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			query string
		}{
			{
				name:  "show_yield_star_then_continuation",
				query: `SHOW TRANSACTIONS YIELD * WHERE false CALL { RETURN 1 AS c } RETURN transactionId`,
			},
			{
				name:  "show_where_after_yield_items",
				query: `SHOW TRANSACTIONS YIELD transactionId WHERE false WHERE false RETURN transactionId`,
			},
			{
				name:  "terminate_where_without_yield",
				query: `TERMINATE TRANSACTIONS '` + nor83AbsentTransactionID + `' WHERE transactionId = 'x'`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, explicit := range []bool{false, true} {
					name := "auto_commit"
					if explicit {
						name = "explicit_transaction"
					}
					t.Run(name, func(t *testing.T) {
						exec := newNOR83AdminContinuationExecutor(t)
						ctx := t.Context()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() {
								if exec.txContext != nil && exec.txContext.active {
									_, _ = exec.Execute(t.Context(), "ROLLBACK", nil)
								}
							})
						}
						result, err := exec.Execute(ctx, tc.query, nil)
						require.Error(t, err)
						require.Nil(t, result, "a refused administration command returns no graph data")
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
						if explicit {
							_, err = exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}
					})
				}
			})
		}
	})
}
