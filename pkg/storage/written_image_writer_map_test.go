package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An ingress publishes its own body with the stored values (writtenNodeImage,
// writtenEdgeImage): a property map the writer holds is replaced in that body,
// never modified, so a map the writer shares between writes keeps its values.
func TestWrittenImageNeverModifiesWriterMaps(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("write-behind=%v/explicit=%v", buffered, explicit), func(t *testing.T) {
				engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				shared := map[string]any{"n": 7, "u": uint16(8), "s": "x"}
				write := func(apply func(*BadgerTransaction) error, direct func() error) {
					t.Helper()
					if !explicit {
						require.NoError(t, direct())
						return
					}
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, apply(tx))
					require.NoError(t, tx.Commit())
				}
				for _, id := range []NodeID{"test:a", "test:b"} {
					node := &Node{ID: id, Labels: []string{"N"}, Properties: shared}
					write(func(tx *BadgerTransaction) error { _, err := tx.CreateNode(node); return err },
						func() error { _, err := engine.CreateNode(node); return err })
				}
				edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: shared}
				write(func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) }, func() error { return engine.CreateEdge(edge) })
				update := &Node{ID: "test:a", Labels: []string{"N"}, Properties: shared}
				write(func(tx *BadgerTransaction) error { return tx.UpdateNode(update) }, func() error { return engine.UpdateNode(update) })
				require.Equal(t, map[string]any{"n": 7, "u": uint16(8), "s": "x"}, shared)
				stored, err := engine.GetNode("test:a")
				require.NoError(t, err)
				require.Equal(t, map[string]any{"n": int64(7), "u": int64(8), "s": "x"}, stored.Properties)
				storedEdge, err := engine.GetEdge("test:r")
				require.NoError(t, err)
				require.Equal(t, map[string]any{"n": int64(7), "u": int64(8), "s": "x"}, storedEdge.Properties)
			})
		}
	}
}
