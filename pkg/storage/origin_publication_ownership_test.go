package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Public transaction/flush/read operations exercise the real handover and
// publication owners. Manual drain establishes the ordering; retaining the
// overlay pointer only observes the immutable body that normal readers copy.
func TestBufferedOriginPublicationKeepsPublishedBodyImmutable(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	require.NoError(t, engine.SubscribeEvents(EventHandlers{EntityCommitted: func(EntityCommitEvent) {}}))
	const id = NodeID("test:origin-owned")
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"value": "first"}}); err != nil {
			return err
		}
		// Replacement inside the same CREATE frame supplies a fresh image,
		// rather than carrying transient metadata back from a preceding read.
		return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"value": "second"}}, Policy: ClientNodeEdits})
	})
	engine.writeBehind.mu.Lock()
	body, found := engine.writeBehind.nodeLocked(id)
	before := copyNode(body)
	engine.writeBehind.mu.Unlock()
	require.True(t, found)
	require.NotNil(t, body)
	origin, bound := body.EmbeddingWorkOrigin()
	require.True(t, bound, "the handover binds provenance before publishing the body")
	visible, err := engine.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, "second", visible.Properties["value"])
	visibleOrigin, visibleBound := visible.EmbeddingWorkOrigin()
	require.True(t, visibleBound)
	require.Equal(t, origin, visibleOrigin)
	require.NoError(t, engine.FlushWriteBehind(nil))
	require.Equal(t, before, body, "replay/notification capture cannot annotate a body already lent to readers")
	stored, err := engine.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, "second", stored.Properties["value"])
}

// A staging/notification frame owns its Operation value, not automatically its
// pointees. Admission has no authority to change a borrowed entity image.
func TestOriginEnvelopeBindingPreservesBorrowedBodies(t *testing.T) {
	node := &Node{ID: "test:borrowed", Properties: map[string]any{"value": "unchanged"}}
	before := *node
	operation := Operation{Type: OpUpdateNode, Node: node}
	operation.bindVectorWorkOrigin(CaptureVectorWorkOrigin())
	require.True(t, operation.vectorOriginBound)
	require.Equal(t, before, *node)
}

func TestOriginDecoratorForwardsPublisherFacts(t *testing.T) {
	for _, namespace := range []string{"", "owned"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			engine := newTestEngine(t)
			var original Engine = engine
			if namespace != "" {
				original = NewNamespacedEngine(original, namespace)
			}
			// A value no owner can capture for itself.
			origin := CaptureVectorWorkOrigin() + 1<<40
			admitted, err := EngineWithVectorWorkOrigin(original, origin)
			require.NoError(t, err)
			events := make(chan EntityCommitEvent, 1)
			require.NoError(t, SubscribeEvents(admitted, EventHandlers{EntityCommitted: func(event EntityCommitEvent) { events <- event }}))
			node := &Node{ID: "test:forwarded", Labels: []string{"Doc"}, Properties: map[string]any{"value": "one"}}
			_, err = admitted.CreateNode(node)
			require.NoError(t, err)
			select {
			case event := <-events:
				require.Equal(t, origin, event.VectorWorkOrigin)
				require.Equal(t, NodeID("test:forwarded"), event.Operation.Node.ID)
				require.Equal(t, "one", event.Operation.Node.Properties["value"])
			case <-time.After(5 * time.Second):
				t.Fatal("forwarded publisher did not deliver its committed event")
			}
			require.False(t, node.runtimeState().entityOriginBound, "admission metadata belongs to the detached argument")
		})
	}
}
