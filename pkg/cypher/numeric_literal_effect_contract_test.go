package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A numeric literal may group its digits with underscores (1.0_0, 1_000,
// 1e1_0). Execution removes those underscores once, before the statement is
// parsed, so the statement it admits is the plain-digit form. The effect
// contract is the one owner of "does this statement write", and it reads the
// same statement execution admits: a CREATE whose only difference is that
// spelling must keep its write effect, its transaction and its write
// requirement rather than being classified as a read.
func TestStatementContractReadsGroupedNumericLiterals(t *testing.T) {
	for _, tc := range []struct {
		query string
		// reads is whether the statement reads the graph as well as writing
		// (MATCH, MERGE), so it needs read and write rather than write alone.
		reads bool
	}{
		{query: "CREATE (:Audit {amount:1.0_0})"},
		{query: "CREATE (:Audit {amount:1_000})"},
		{query: "CREATE (:Audit {amount:1.0e1_0})"},
		{query: "CREATE (:Audit {amount:0x1_F})"},
		{query: "MERGE (:Audit {amount:1_0.5})", reads: true},
		{query: "MATCH (n:Audit) SET n.amount = 2_0", reads: true},
		{query: "CREATE (:Audit {xs: [1_0, 2_0], m: {a: 3_0}})"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			contract, err := readStatementExecutionContract(t.Context(), tc.query)
			require.NoError(t, err)
			require.Equal(t, statementEffectWrite, contract.effect())
			require.True(t, contract.writes)
			require.True(t, contract.transaction)

			requirements := QueryPermissionRequirements(t.Context(), tc.query, RawSource)
			require.True(t, requirements.Write, "the statement writes, whatever its literal spelling")
			require.Equal(t, tc.reads, requirements.Read, "a graph-free write needs write alone")
		})
	}
	// The same literals in a read statement stay a read: normalization is not
	// a write hint.
	for _, query := range []string{"RETURN 1_000 AS n", "RETURN 1.0_0 AS n", "MATCH (n:Audit) RETURN n.amount AS a", "UNWIND [1_0, 2_0] AS x RETURN x"} {
		t.Run(query, func(t *testing.T) {
			contract, err := readStatementExecutionContract(t.Context(), query)
			require.NoError(t, err)
			require.Equal(t, statementEffectRead, contract.effect())
			requirements := QueryPermissionRequirements(t.Context(), query, RawSource)
			require.False(t, requirements.Write)
			require.True(t, requirements.Read)
		})
	}
}

// The admission boundary a principal meets before execution must agree with
// the classification: the CREATE needs write, not read.
func TestGroupedNumericLiteralAdmissionNeedsWrite(t *testing.T) {
	const query = "CREATE (:Audit {amount:1.0_0})"
	grant := func(allowed ...string) context.Context {
		return WithPermissionChecker(t.Context(), func(permission string) bool {
			for _, name := range allowed {
				if permission == name {
					return true
				}
			}
			return false
		})
	}
	require.NoError(t, AuthorizeQuery(grant("write"), query))
	denied := AuthorizeQuery(grant("read"), query)
	require.Error(t, denied)
	code, message := nornicerrors.Neo4jStatus(denied)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.Contains(t, message, "write")
}

// The public path must agree with the classification on either parser
// frontend: a principal holding write alone runs the CREATE, the grouped
// literal reaches storage as its plain value, and a read grant alone neither
// runs it nor reads a grouped literal differently.
func TestGroupedNumericLiteralCreateRunsAsAdmittedWrite(t *testing.T) {
	queries := []string{"CREATE (:Audit {amount:1.0_0})"}
	for _, setting := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(setting.name, func(t *testing.T) {
			t.Cleanup(setting.setup())
			for _, query := range queries {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "audit_amounts")
				exec := NewStorageExecutor(store)
				writeOnly := WithDatabasePermissionResolver(t.Context(), "audit_amounts", func(_, permission string) bool {
					return permission == "write"
				})
				_, err := exec.Execute(writeOnly, query, nil)
				require.NoError(t, err, query)

				nodes, err := store.GetNodesByLabel(t.Context(), "Audit")
				require.NoError(t, err)
				require.Len(t, nodes, 1, query)
				require.Equal(t, 1.0, nodes[0].Properties["amount"], query)

				// A read grant alone must not run it.
				readOnly := WithDatabasePermissionResolver(t.Context(), "audit_amounts", func(_, permission string) bool {
					return permission == "read"
				})
				_, err = exec.Execute(readOnly, query, nil)
				require.Error(t, err, query)
				nodes, err = store.GetNodesByLabel(t.Context(), "Audit")
				require.NoError(t, err)
				require.Len(t, nodes, 1, "the refused statement wrote nothing")

				// An ordinary read with a grouped literal stays a read: the
				// granted permissions above never widened what a read statement
				// may run, and the literal still reaches the row as its value.
				result, err := exec.Execute(readOnly, "MATCH (n:Audit) RETURN n.amount AS amount", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{1.0}}, result.Rows)
				result, err = exec.Execute(readOnly, "RETURN 1.0_0 AS amount", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{1.0}}, result.Rows)
			}
		})
	}
}

// A literal that breaks the digit-grouping rule keeps its text and is
// rejected: the effect contract reports the syntax error instead of silently
// classifying the unreadable statement as a read, and execution writes
// nothing.
func TestGroupedNumericLiteralInvalidStatementKeepsItsError(t *testing.T) {
	const query = "CREATE (:Audit {amount:1__0})"
	_, err := readStatementExecutionContract(t.Context(), query)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)

	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "audit_invalid")
	exec := NewStorageExecutor(store)
	_, err = exec.Execute(t.Context(), query, nil)
	require.Error(t, err)
	code, _ = nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	nodes, err := store.GetNodesByLabel(t.Context(), "Audit")
	require.NoError(t, err)
	require.Empty(t, nodes)
}

// The effect reader preserves cancellation; authorization returns that original
// cancellation rather than converting it to a privilege failure.
func TestGroupedNumericLiteralClassificationKeepsCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := readStatementExecutionContract(canceled, "CREATE (:Audit {amount:1.0_0})")
	require.ErrorIs(t, err, context.Canceled)

	// AuthorizeQuery reports the cancellation itself, not a permission verdict.
	grantAll := WithPermissionChecker(canceled, func(string) bool { return true })
	require.Equal(t, context.Canceled, AuthorizeQuery(grantAll, "CREATE (:Audit {amount:1.0_0})"))
}

// The classification is the same whatever NORNICDB_CYPHER_QUERY_NORMALIZATION
// says: numeric literal normalization is not gated by it, so neither is the
// verdict it decides.
func TestGroupedNumericLiteralClassificationIgnoresNormalizationSetting(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, enabled := range []bool{true, false} {
		config.SetCypherQueryNormalizationEnabled(enabled)
		requirements := QueryPermissionRequirements(t.Context(), "CREATE (:Audit {amount:1.0_0})", RawSource)
		require.True(t, requirements.Write, "enabled=%v", enabled)
		require.False(t, requirements.Read, "enabled=%v", enabled)
	}
}

func TestCanceledStatementContractDoesNotNormalizeLiterals(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	query := "RETURN [" + strings.Repeat("1.0_0,", 4096) + "1.0_0]"
	allocations := testing.AllocsPerRun(10, func() {
		_, err := readStatementExecutionContract(ctx, query)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected original cancellation, got %v", err)
		}
	})
	require.Zero(t, allocations, "an already canceled contract read must not allocate a canonical copy of its input")
}
