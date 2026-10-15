package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func materializedPathAccessFixture() PathResult {
	return PathResult{
		Nodes: []*storage.Node{
			{ID: "a", Labels: []string{"MaterializedPathAccess"}},
			{ID: "b", Labels: []string{"MaterializedPathAccess"}},
		},
		Relationships: []*storage.Edge{{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_PATH_ACCESS"}},
		Length:        1,
	}
}

func TestMaterializedPathUsesTypedCarrier(t *testing.T) {
	path := materializedPathAccessFixture()
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	t.Cleanup(func() { _ = exec.storage.Close() })

	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"value", path},
		{"pointer", &path},
		{"path map", exec.pathToMap(path)},
		{"nested path map", map[string]interface{}{"payload": []interface{}{exec.pathToMap(path)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &recordingAccessRecorder{}
			require.True(t, materializedValue(recorder, tc.value))
			require.Equal(t, []string{"a", "b", "e"}, recorder.ids)
		})
	}

	var nilPath *PathResult
	require.False(t, materializedValue(nil, nilPath))
	for _, value := range []interface{}{
		map[string]interface{}{"_pathResult": map[string]interface{}{"nodes": []interface{}{}, "relationships": []interface{}{}}},
		[]interface{}{int64(1), map[string]interface{}{"elementId": "ordinary"}},
	} {
		recorder := &recordingAccessRecorder{}
		require.False(t, materializedValue(recorder, value), "%#v", value)
		require.Empty(t, recorder.ids, "%#v", value)
	}
}

func TestMaterializedPathAccessAcrossTransactions(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "auto-commit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			inner := &accessCountingEngine{MemoryEngine: storage.NewMemoryEngine()}
			t.Cleanup(func() { _ = inner.Close() })
			store := storage.NewNamespacedEngine(inner, "materialized")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "a"}})
			require.NoError(t, err)
			_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "b"}})
			require.NoError(t, err)
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_PATH_ACCESS"}))

			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"MATCH p=(:MaterializedPathAccess {id:'a'})-[:MATERIALIZED_PATH_ACCESS]->(:MaterializedPathAccess {id:'b'}) RETURN p",
				"MATCH p=(:MaterializedPathAccess {id:'a'})-[:MATERIALIZED_PATH_ACCESS]->(:MaterializedPathAccess {id:'b'}) RETURN {payload:[p]} AS value",
			} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Len(t, result.Rows, 1, query)
				require.Equal(t, []string{"materialized:a", "materialized:b", "materialized:e"}, inner.take(), query)
			}
			if explicit {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
		})
	}
}

type discardMaterializedAccessRecorder struct{}

func (discardMaterializedAccessRecorder) RecordMaterializedAccess(string) {}

func BenchmarkMaterializedValuePath(b *testing.B) {
	path := materializedPathAccessFixture()
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	b.Cleanup(func() { _ = exec.storage.Close() })
	recorder := discardMaterializedAccessRecorder{}
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"path map", exec.pathToMap(path)},
		{"node control", path.Nodes[0]},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !materializedValue(recorder, tc.value) {
					b.Fatal("path was not recognized")
				}
			}
		})
	}
}
