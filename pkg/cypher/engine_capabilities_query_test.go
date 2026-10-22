package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEngineDecoratorsPreserveProjectedMatch(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err = tenant.CreateNode(&storage.Node{ID: "record", Labels: []string{"NamespaceRecord"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	other := storage.NewNamespacedEngine(base, "other")
	_, err = other.CreateNode(&storage.Node{ID: "record", Labels: []string{"NamespaceRecord"}, Properties: map[string]interface{}{"id": int64(2)}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&storage.Node{ID: "target", Labels: []string{"Target"}, Properties: map[string]interface{}{"id": int64(9)}})
	require.NoError(t, err)
	require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "link", StartNode: "record", EndNode: "target", Type: "LINK"}))
	for name, engine := range map[string]storage.Engine{
		"namespace":  tenant,
		"traced":     storage.NewTracedEngine(tenant),
		"wal-traced": storage.NewWALEngine(storage.NewTracedEngine(tenant), nil),
		"traced-wal": storage.NewTracedEngine(storage.NewWALEngine(tenant, nil)),
	} {
		t.Run(name, func(t *testing.T) {
			exec := cypher.NewStorageExecutor(engine)
			for _, query := range []struct {
				text string
				want int64
			}{
				{"MATCH (n:NamespaceRecord) RETURN n.id ORDER BY n.id", 1},
				{"MATCH (n:NamespaceRecord {id:1}) RETURN n.id", 1},
				{"MATCH (n:NamespaceRecord) WHERE n.id=1 RETURN count(n)", 1},
				{"MATCH (n:NamespaceRecord) RETURN n.id LIMIT 1", 1},
				{"MATCH (n:NamespaceRecord)-[:LINK]->(m:Target) RETURN m.id", 9},
			} {
				result, err := exec.Execute(context.Background(), query.text, nil)
				require.NoError(t, err, query.text)
				require.Equal(t, [][]interface{}{{query.want}}, result.Rows, query.text)
			}
		})
	}
}

func BenchmarkEngineDecoratorMatch(b *testing.B) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err = tenant.CreateNode(&storage.Node{ID: "record", Labels: []string{"NamespaceRecord"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(b, err)
	for name, engine := range map[string]storage.Engine{
		"namespace":  tenant,
		"traced":     storage.NewTracedEngine(tenant),
		"wal-traced": storage.NewWALEngine(storage.NewTracedEngine(tenant), nil),
	} {
		b.Run(name, func(b *testing.B) {
			exec := cypher.NewStorageExecutor(engine)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(ctx, "MATCH (n:NamespaceRecord) RETURN n LIMIT 1", map[string]interface{}{"nonce": i})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
