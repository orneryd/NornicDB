package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The written image differs from the writer's node only in stored property
// values and codec timestamps: a body the owner decodes keeps the runtime
// embedding identity (attempt and reader provenance) that a body it serves
// undecoded keeps, as storage's node copies do, so an update's notification
// carries the writer's embedding attempt either way.
func TestWrittenImageKeepsRuntimeEmbeddingIdentity(t *testing.T) {
	for _, value := range []any{"x", []any{"a", "b"}} {
		for _, buffered := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%T/write-behind=%v/explicit=%v", value, buffered, explicit), func(t *testing.T) {
					engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
					require.NoError(t, err)
					t.Cleanup(func() { _ = engine.Close() })
					_, err = engine.CreateNode(&Node{ID: "test:n", Labels: []string{"N"}, Properties: map[string]any{"v": "old"}})
					require.NoError(t, err)
					updated := make(chan *Node, 4)
					engine.OnNodeUpdated(func(node *Node) { updated <- node })
					writer, err := engine.GetNode("test:n")
					require.NoError(t, err)
					attempt := NewEmbeddingAttempt(1, 1, writer.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(*Node) {})
					writer.SetEmbeddingAttempt(attempt)
					writer.Properties = map[string]any{"v": value}
					if explicit {
						tx, err := engine.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.UpdateGraphNode(writer, ClientNodeEdits))
						require.NoError(t, tx.Commit())
					} else {
						require.NoError(t, engine.UpdateNode(writer))
					}
					select {
					case node := <-updated:
						require.Same(t, attempt, node.EmbeddingAttempt())
						require.Same(t, engine, node.runtimeState().embeddingOwner)
					case <-time.After(10 * time.Second):
						t.Fatal("no update notification")
					}
				})
			}
		}
	}
}
