package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamedSingleNodePathPreservesWholeNodeProperties(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema string
	}{
		{name: "unindexed"},
		{name: "range index", schema: "CREATE INDEX named_path_doc_id FOR (n:NamedPathDoc) ON (n.id)"},
		{name: "unique key", schema: "CREATE CONSTRAINT named_path_doc_id_unique FOR (n:NamedPathDoc) REQUIRE n.id IS UNIQUE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &recordingLabelScanEngine{MemoryEngine: newTestMemoryEngine(t)}
			exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))
			if tc.schema != "" {
				_, err := exec.Execute(ctx, tc.schema, nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "CREATE (:NamedPathDoc {id: 1, keep: 'present'})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH p=(n:NamedPathDoc) RETURN [x IN nodes(p) | properties(x)] AS props", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, []interface{}{map[string]interface{}{"id": int64(1), "keep": "present"}}, result.Rows[0][0])
			result, err = exec.Execute(ctx, "MATCH p=(n:NamedPathDoc) CALL (p) { RETURN nodes(p)[0] AS x } RETURN x.keep AS kept", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"present"}}, result.Rows)

			_, err = exec.Execute(ctx, "MATCH p=(n:NamedPathDoc) CALL (p) { WITH nodes(p)[0] AS x RETURN x } SET x.added = 2 RETURN x.id AS id", nil)
			require.NoError(t, err)
			result, err = exec.Execute(ctx, "MATCH (n:NamedPathDoc) RETURN properties(n) AS props", nil)
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{"id": int64(1), "keep": "present", "added": int64(2)}, result.Rows[0][0])
		})
	}
}
