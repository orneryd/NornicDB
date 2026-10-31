package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every ingress of an acknowledged body publishes the image a read from
// storage decodes (writtenNodeImage, writtenEdgeImage): what the entity
// caches and the write-behind overlay serve right after the write equals what
// an uncached read decodes afterwards, and a cache fill keeps it as decoded.
// Values whose Go form the codec changes are where a writer's own body would
// differ: generic lists, mixed numbers, narrow widths, zoned times, nested
// maps.
func TestEveryIngressPublishesStoredImage(t *testing.T) {
	zone := time.FixedZone("plus-one", 3600)
	values := map[string]any{
		"strings": []any{"stored", "x"}, "mixed": []any{int64(1), 2.5}, "go ints": []any{1, 2},
		"int": 7, "int32": int32(7), "float32": float32(0.5), "time": time.Date(2024, 1, 2, 3, 4, 5, 6, zone),
		"nested": map[string]any{"x": []any{int64(1), int64(2)}, "y": []any{"a"}}, "plain": "x", "count": int64(3),
	}
	properties := func(round int) map[string]any {
		out := make(map[string]any, len(values)+1)
		for k, v := range values {
			out[k] = v
		}
		out["round"] = int64(round)
		return out
	}
	type route struct {
		name  string
		write func(t *testing.T, engine *BadgerEngine, round int)
	}
	inTx := func(t *testing.T, engine *BadgerEngine, implicit bool, apply func(*BadgerTransaction) error) {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.SetImplicit(implicit))
		require.NoError(t, apply(tx))
		require.NoError(t, tx.Commit())
	}
	node := func(round int) *Node {
		return &Node{ID: "test:n", Labels: []string{"N"}, Properties: properties(round)}
	}
	edge := func(round int) *Edge {
		return &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: properties(round)}
	}
	routes := []route{
		{"direct", func(t *testing.T, engine *BadgerEngine, round int) {
			if round == 0 {
				_, err := engine.CreateNode(node(round))
				require.NoError(t, err)
				require.NoError(t, engine.CreateEdge(edge(round)))
				return
			}
			require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: node(round)}))
			require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: edge(round)}))
		}},
		{"bulk", func(t *testing.T, engine *BadgerEngine, round int) {
			if round == 0 {
				require.NoError(t, engine.BulkCreateNodes([]*Node{node(round)}))
				require.NoError(t, engine.BulkCreateEdges([]*Edge{edge(round)}))
				return
			}
			require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: node(round)}))
			require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: edge(round)}))
		}},
		{"explicit transaction", func(t *testing.T, engine *BadgerEngine, round int) {
			inTx(t, engine, false, func(tx *BadgerTransaction) error {
				if round == 0 {
					if _, err := tx.CreateNode(node(round)); err != nil {
						return err
					}
					return tx.CreateEdge(edge(round))
				}
				if err := tx.UpdateGraphNode(NodeUpdateRequest{After: node(round), Policy: ClientNodeEdits}); err != nil {
					return err
				}
				return tx.UpdateEdge(EdgeUpdateRequest{After: edge(round)})
			})
		}},
		{"implicit transaction", func(t *testing.T, engine *BadgerEngine, round int) {
			inTx(t, engine, true, func(tx *BadgerTransaction) error {
				if round == 0 {
					if _, err := tx.CreateNode(node(round)); err != nil {
						return err
					}
					return tx.CreateEdge(edge(round))
				}
				if err := tx.UpdateGraphNode(NodeUpdateRequest{After: node(round), Policy: ClientNodeEdits}); err != nil {
					return err
				}
				return tx.UpdateEdge(EdgeUpdateRequest{After: edge(round)})
			})
		}},
	}
	for _, buffered := range []bool{false, true} {
		for _, r := range routes {
			t.Run(fmt.Sprintf("%s/write-behind=%v", r.name, buffered), func(t *testing.T) {
				engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
					DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
				}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				for _, endpoint := range []NodeID{"test:a", "test:b"} {
					_, err := engine.CreateNode(&Node{ID: endpoint, Labels: []string{"E"}})
					require.NoError(t, err)
				}
				type image struct {
					stored, visible *Node
					storedEdge      *Edge
					visibleEdge     *Edge
				}
				read := func() image {
					t.Helper()
					var out image
					out.stored, err = engine.GetStoredNode("test:n")
					require.NoError(t, err)
					out.visible, err = engine.GetNode("test:n")
					require.NoError(t, err)
					out.storedEdge, err = engine.GetStoredEdge("test:e")
					require.NoError(t, err)
					out.visibleEdge, err = engine.GetEdge("test:e")
					require.NoError(t, err)
					out.stored, out.visible = publicNodeImage(out.stored), publicNodeImage(out.visible)
					out.storedEdge, out.visibleEdge = comparableStoredEdge(out.storedEdge), comparableStoredEdge(out.visibleEdge)
					return out
				}
				for round := 0; round < 2; round++ {
					r.write(t, engine, round)
					published := read()
					require.NoError(t, engine.FlushWriteBehind(nil))
					engine.invalidateCachesAfterRestore()
					decoded := read()
					require.Equal(t, decoded, published, "round %d: published body differs from the stored one", round)
					require.Equal(t, decoded, read(), "round %d: a cache fill changed the body", round)
				}
			})
		}
	}
}
