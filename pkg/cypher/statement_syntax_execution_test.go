// Author: Codex R17 implementation agent, 26 October 2026.
package cypher

import (
	"context"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetainedSyntaxExecutionReaders(t *testing.T) {
	for _, sample := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"WITH 1 AS v RETURN coalesce(v, 2) AS answer", [][]interface{}{{int64(1)}}},
		{"UNWIND [] AS row WITH CASE WHEN true THEN 1 ELSE true END AS v RETURN toUpper(coalesce(v,null)) AS answer", [][]interface{}{}},
		{"WITH 1 AS v CALL (v) { WITH v RETURN coalesce(v, 2) AS answer } RETURN answer", [][]interface{}{{int64(1)}}},
		{"CALL () { OPTIONAL MATCH (n:RetainedSyntaxMissing) WITH collect(n) AS ns RETURN size(ns) AS answer } RETURN answer", [][]interface{}{{int64(0)}}},
		{"CALL () { CALL () { RETURN 1 AS v } RETURN coalesce(v, 2) AS answer } RETURN answer", [][]interface{}{{int64(1)}}},
		{"CALL () { RETURN 1 AS v UNION ALL RETURN 2 AS v } RETURN v AS answer", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"/* é */ WITH 'عربي' AS v CALL (v) { RETURN /* 名 */ v AS answer } RETURN answer", [][]interface{}{{"عربي"}}},
	} {
		t.Run(sample.query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_syntax"))
			result, err := executor.Execute(context.Background(), sample.query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"answer"}, result.Columns)
			require.Equal(t, sample.rows, result.Rows)
		})
	}
}

func TestRetainedInspectionScopeAdmission(t *testing.T) {
	for _, source := range []string{
		"WITH 2718 AS retainedAdmission RETURN retainedAdmission AS answer",
		"RETURN 2718 AS answer UNION ALL RETURN 3141 AS answer",
	} {
		t.Run(source, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_admission"))
			ctx := cypherantlr.WithStatementSyntax(context.Background(), source)
			inspection, err := cypherantlr.InspectSyntax(ctx, source, false, cypherantlr.AcceptedSource)
			require.NoError(t, err)
			require.Equal(t, cypherantlr.CypherParserRULE_script, inspection.Root().Rule())
			admission, err := executor.admitBoundStatement(ctx, source)
			require.NoError(t, err)
			require.True(t, admission.graph, "a retained query body must bind its graph result contract")
			require.False(t, admission.command)
			require.NotNil(t, admission.program)
			require.NoError(t, admission.verifyColumns([]string{"answer"}))
			require.Error(t, admission.verifyColumns([]string{"wrong"}))
			current, err := cypherantlr.QuerySyntax(ctx, source, false, cypherantlr.AcceptedSource)
			require.NoError(t, err)
			require.Equal(t, inspection.Origin(), current.Origin(), "scope admission must consume the original inspection product")
			require.Equal(t, cypherantlr.CypherParserRULE_script, current.Root().Rule())
		})
	}
}

func TestRetainedSyntaxProtocolInspectionEntry(t *testing.T) {
	source := "WITH 271828 AS protocolEntry CALL (protocolEntry) { RETURN protocolEntry AS value } RETURN value"
	permissions := QueryPermissionRequirements(context.Background(), source, AcceptedSource)
	require.True(t, permissions.Read)
	require.False(t, permissions.Write)
	inspection, err := cypherantlr.InspectSyntax(context.Background(), source, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, cypherantlr.CypherParserRULE_script, inspection.Root().Rule(), "permission inspection must carry canonical whole-statement meaning into execution")
	query, err := cypherantlr.QuerySyntax(context.Background(), source, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, inspection.Origin(), query.Origin())
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_protocol"))
	for attempt := 0; attempt < 2; attempt++ {
		result, err := executor.Execute(context.Background(), source, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"value"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(271828)}}, result.Rows)
	}
}
