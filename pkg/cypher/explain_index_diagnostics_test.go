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

	args := map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE n.name = 'Ada'")
	require.Equal(t, "available", args["indexStatus"])
	avail, ok := args["availableIndexes"].([]string)
	require.True(t, ok)
	require.NotEmpty(t, avail)

	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "UnknownLabel", "MATCH (n:UnknownLabel) RETURN n")
	require.Equal(t, "no_index_for_label", args["indexStatus"])

	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE coalesce(n.name, '') = '' RETURN n")
	require.Equal(t, "function_wrapping (coalesce)", args["indexRejectionRisk"])

	args = map[string]interface{}{}
	exec.annotateIndexDiagnostics(args, "Person", "MATCH (n:Person) WHERE toLower(n.name) = 'ada' RETURN n")
	require.Equal(t, "function_wrapping (toLower/toUpper)", args["indexRejectionRisk"])
}

func TestAnalyzeNodeScan_OperatorShapeBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "analyze_scan_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_person_email_diag IF NOT EXISTS FOR (n:Person) ON (n.email)", nil)
	require.NoError(t, err)

	op := exec.analyzeNodeScan("MATCH (n:Person {email: 'a@b.com'}) RETURN n")
	require.Equal(t, "NodeIndexSeek", op.OperatorType)
	require.Equal(t, "Person", op.Arguments["label"])
	require.Equal(t, "available", op.Arguments["indexStatus"])

	op = exec.analyzeNodeScan("MATCH (n:Person) RETURN n")
	require.Equal(t, "NodeByLabelScan", op.OperatorType)
	require.Equal(t, "Person", op.Arguments["label"])

	op = exec.analyzeNodeScan("MATCH (n) RETURN n")
	require.Equal(t, "AllNodesScan", op.OperatorType)
	require.Empty(t, op.Arguments)
}
