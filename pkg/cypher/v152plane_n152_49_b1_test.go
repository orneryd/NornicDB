package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-146. Neo4j 5.26 rejects a statement whose last clause is a CALL
// subquery that returns columns: "Query cannot conclude with CALL" is a
// statement-admission SyntaxError, so the statement never runs. NornicDB
// answers with the execution-contract diagnostic "Execution result columns do
// not match the statement schema" (Neo.DatabaseError.Statement.ExecutionFailed)
// from the result-schema check that runs after execution. The rejection and
// the controls below hold on both parsers and in auto-commit and an explicit
// transaction; the final-RETURN form keeps returning rows 1, 2, 3 and a
// trailing unit CALL subquery stays valid.
const (
	// nor146Witness is the ticket's exact reproduction.
	nor146Witness = "UNWIND [0] AS m CALL { UNWIND range(1,3) AS x RETURN x }"
	// nor146WitnessFinalReturn is the same query with Neo4j's final RETURN.
	nor146WitnessFinalReturn = nor146Witness + " RETURN x"
	// nor146WitnessRuntimeError returns columns too, so it is the same
	// admission rule, but its body would fail at run time (1 / 0) if the
	// statement were executed: a diagnostic from the body proves execution
	// started, an admission diagnostic proves it did not.
	nor146WitnessRuntimeError = "UNWIND [0] AS m CALL { UNWIND [0] AS x RETURN 1 / x AS y }"
	// nor146WitnessCreateBefore writes before the rejected CALL subquery.
	nor146WitnessCreateBefore = "UNWIND [0] AS m CREATE (n:Nor146Admission {m: m}) WITH m CALL { UNWIND range(1,3) AS x RETURN x }"
	// nor146UnitTrailingCall is Neo4j's valid trailing subquery form: the
	// body returns no columns, so the CALL concludes the statement.
	nor146UnitTrailingCall = "UNWIND [0] AS m CALL { WITH m CREATE (n:Nor146Unit {m: m}) }"
)

// nor146ConclusionMessage is Neo4j's statement-admission diagnostic for a
// query that ends in a returning CALL subquery.
var nor146ConclusionMessage = localization.CypherMatchingQueryCannotConclude("CALL").Fallback

// nor146MismatchMessage is the execution-contract diagnostic the ticket
// reports instead.
var nor146MismatchMessage = localization.CypherResultMetadataMismatch().Fallback

// nor146RequireAdmissionSyntaxError asserts the statement-admission
// diagnostic, as Bolt and HTTP classify it, and that it is not the
// post-execution result-schema mismatch the ticket reproduces.
func nor146RequireAdmissionSyntaxError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	require.Equal(t, nor146ConclusionMessage, message)
	require.NotContains(t, err.Error(), nor146MismatchMessage)
}

// nor146Mode is one public Execute statement context: an auto-commit
// statement, or a statement of an explicit transaction.
type nor146Mode struct {
	name  string
	begin bool
}

var nor146Modes = []nor146Mode{{name: "auto"}, {name: "tx", begin: true}}

// nor146Parsers are the two parsers the ticket requires, selected as the
// statement admission and routing owners read them.
var nor146Parsers = []string{config.ParserTypeNornic, config.ParserTypeANTLR}

// nor146Executor is a fresh executor over a fresh small store. In the tx mode
// it holds an open explicit transaction, rolled back when the test ends.
func nor146Executor(t *testing.T, mode nor146Mode) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor146"))
	if !mode.begin {
		return exec
	}
	_, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
	return exec
}

// TestNor146TrailingReturningCallExecute reproduces NOR-146 on the public
// Execute entry point for both parsers, in auto-commit and in an explicit
// transaction, and keeps the ticket's controls on the same matrix.
func TestNor146TrailingReturningCallExecute(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range nor146Parsers {
		config.SetParserType(parser)
		for _, mode := range nor146Modes {
			t.Run(parser+"/"+mode.name, func(t *testing.T) {
				t.Run("witness-rejected", func(t *testing.T) {
					exec := nor146Executor(t, mode)
					result, err := exec.Execute(context.Background(), nor146Witness, nil)
					require.Nil(t, result)
					nor146RequireAdmissionSyntaxError(t, err)
				})
				t.Run("rejected-before-execution", func(t *testing.T) {
					// The body would divide by zero at run time, so a body
					// diagnostic here would prove the statement had run.
					exec := nor146Executor(t, mode)
					_, err := exec.Execute(context.Background(), nor146WitnessRuntimeError, nil)
					nor146RequireAdmissionSyntaxError(t, err)
				})
				t.Run("final-return-rows", func(t *testing.T) {
					exec := nor146Executor(t, mode)
					result, err := exec.Execute(context.Background(), nor146WitnessFinalReturn, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"x"}, result.Columns)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
				})
				t.Run("unit-call-accepted", func(t *testing.T) {
					exec := nor146Executor(t, mode)
					_, err := exec.Execute(context.Background(), nor146UnitTrailingCall, nil)
					require.NoError(t, err)
					counted, err := exec.Execute(context.Background(), "MATCH (n:Nor146Unit) RETURN count(n) AS c", nil)
					require.NoError(t, err)
					require.Equal(t, []string{"c"}, counted.Columns)
					require.Equal(t, [][]interface{}{{int64(1)}}, counted.Rows)
				})
				t.Run("write-first-has-no-effect", func(t *testing.T) {
					exec := nor146Executor(t, mode)
					ctx := context.Background()
					_, err := exec.Execute(ctx, nor146WitnessCreateBefore, nil)
					if mode.begin {
						// A failed statement ends the transaction's work;
						// ROLLBACK is how its client ends it (#683), and the
						// effect check then reads the store, not the failed
						// transaction.
						_, _ = exec.Execute(ctx, "ROLLBACK", nil)
					}
					counted, countErr := exec.Execute(ctx, "MATCH (n:Nor146Admission) RETURN count(n) AS c", nil)
					require.NoError(t, countErr)
					require.Equal(t, [][]interface{}{{int64(0)}}, counted.Rows, "reported outcome: %v", err)
					nor146RequireAdmissionSyntaxError(t, err)
				})
			})
		}
	}
}

// TestNor146ResultSchemaMismatchStaysDistinct keeps the diagnostic the ticket
// reports as its own answer for the case it owns: a statement whose executed
// result columns genuinely differ from its established schema.
func TestNor146ResultSchemaMismatchStaysDistinct(t *testing.T) {
	err := resultmetadata.KnownColumns([]string{"x"}).Verify([]string{"y"})
	require.Error(t, err)
	var mismatch *localization.LocalizedError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "Neo.DatabaseError.Statement.ExecutionFailed", mismatch.Code)
	require.Equal(t, nor146MismatchMessage, err.Error())
	require.NotEqual(t, nor146ConclusionMessage, err.Error())
}
