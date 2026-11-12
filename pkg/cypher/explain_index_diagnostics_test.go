package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAnnotateIndexDiagnostics_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_diag_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_person_name_diag IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX idx_movie_title_diag IF NOT EXISTS FOR (n:Movie) ON (n.title)", nil)
	require.NoError(t, err)

	// :Person(name) is indexed, so the WHERE equality is applicable and the
	// seek is planned. The diagnostics report the index as available only
	// because that planning succeeded.
	pattern, err := exec.parseNodePattern(ctx, "(n:Person)")
	require.NoError(t, err)
	planned, err := exec.explainPlansNodeIndexSeek(ctx, pattern, "n.name = 'Ada'")
	require.NoError(t, err)
	require.True(t, planned)

	args := map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE n.name = 'Ada'", planned)
	require.Equal(t, "available", args["indexStatus"])
	avail, ok := args["availableIndexes"].([]string)
	require.True(t, ok)
	require.NotEmpty(t, avail)

	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "UnknownLabel", "MATCH (n:UnknownLabel) RETURN n", false)
	require.Equal(t, "no_index_for_label", args["indexStatus"])

	// A wrapping function keeps the index from serving the predicate, so the
	// label's index is reported as present but not applicable.
	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE coalesce(n.name, '') = '' RETURN n", false)
	require.Equal(t, "no_applicable_index", args["indexStatus"])
	require.Equal(t, "function_wrapping (coalesce)", args["indexRejectionRisk"])

	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE toLower(n.name) = 'ada' RETURN n", false)
	require.Equal(t, "no_applicable_index", args["indexStatus"])
	require.Equal(t, "function_wrapping (toLower/toUpper)", args["indexRejectionRisk"])
}

func TestAnalyzeNodeScan_OperatorShapeBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "analyze_scan_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_person_email_diag IF NOT EXISTS FOR (n:Person) ON (n.email)", nil)
	require.NoError(t, err)

	op, err := exec.analyzeNodeScan(ctx, "MATCH (n:Person {email: 'a@b.com'}) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "NodeIndexSeek", op.OperatorType)
	require.Equal(t, "Person", op.Arguments["label"])
	require.Equal(t, "available", op.Arguments["indexStatus"])

	// The index on :Person(email) cannot serve an :Person(age) predicate.
	op, err = exec.analyzeNodeScan(ctx, "MATCH (n:Person {age: 30}) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "NodeByLabelScan", op.OperatorType)
	require.Equal(t, "Person", op.Arguments["label"])
	require.Equal(t, "no_applicable_index", op.Arguments["indexStatus"])

	op, err = exec.analyzeNodeScan(ctx, "MATCH (n:Person) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "NodeByLabelScan", op.OperatorType)
	require.Equal(t, "Person", op.Arguments["label"])

	op, err = exec.analyzeNodeScan(ctx, "MATCH (n) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "AllNodesScan", op.OperatorType)
	require.Empty(t, op.Arguments)
}
