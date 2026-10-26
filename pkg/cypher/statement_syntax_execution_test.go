// Author: Codex R17 implementation agent, 26 October 2026.
package cypher

import (
	"context"
	"testing"

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
