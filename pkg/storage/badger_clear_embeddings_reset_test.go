package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// clearResetUpdates records the node-update publications one reset produces.
type clearResetUpdates struct {
	mu  sync.Mutex
	ids map[NodeID]int
}

func (c *clearResetUpdates) record(node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = make(map[NodeID]int)
	}
	c.ids[node.ID]++
}

func (c *clearResetUpdates) snapshot() map[NodeID]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	observed := make(map[NodeID]int, len(c.ids))
	for id, count := range c.ids {
		observed[id] = count
	}
	return observed
}

// newClearResetEngine is the fixture every reset case shares: an in-memory
// BadgerEngine with the pending-embedding index enabled, because the marker the
// cases assert is the queue entry the embed worker consumes.
func newClearResetEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine := newTestEngine(t)
	engine.SetEmbeddingsEnabled(true)
	return engine
}

func createEmbeddedNode(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &Node{
		ID:              id,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "content"},
		ChunkEmbeddings: [][]float32{{0.25, 0.5}},
	})
	require.NoError(t, err, id)
}

func requireStoredVectorCount(t *testing.T, engine *BadgerEngine, id NodeID, want int) {
	t.Helper()
	stored, err := engine.GetStoredNode(t.Context(), id)
	require.NoError(t, err, id)
	require.Len(t, stored.ChunkEmbeddings, want, "%s vector state", id)
}

// requirePendingReset reads the durable pending marker of one node, so a reset
// cannot pass by leaving a namespace-wide queue that merely looks right.
func requirePendingReset(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	require.Equal(t, 1, engine.CountPendingEmbeddingIDs(context.Background(), func(candidate []byte) bool {
		return NodeID(candidate) == id
	}), "%s is queued for regeneration", id)
}

// A successful reset reports exactly the nodes whose vectors it removed, clears
// the stored body but nothing else, and queues each cleared node through the
// canonical node update owner rather than a namespace-wide index refresh.
func TestClearAllEmbeddingsForPrefixReportsExactCountAndQueuesRegeneration(t *testing.T) {
	engine := newClearResetEngine(t)
	updates := &clearResetUpdates{}
	engine.OnNodeUpdated(updates.record)

	createEmbeddedNode(t, engine, "test:alpha")
	createEmbeddedNode(t, engine, "test:beta")
	_, err := engine.CreateNode(t.Context(), &Node{
		ID:         "test:unembedded",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"text": "content"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, engine.PendingEmbeddingsCount(), "only the node without vectors waits for one")

	beforeAlpha, err := engine.GetStoredNode(t.Context(), "test:alpha")
	require.NoError(t, err)
	beforeAlpha = CopyNode(beforeAlpha)

	cleared, err := engine.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 2, cleared, "the count is exactly the nodes whose vectors were removed")

	for _, id := range []NodeID{"test:alpha", "test:beta"} {
		requireStoredVectorCount(t, engine, id, 0)
		requirePendingReset(t, engine, id)
	}
	require.Equal(t, 3, engine.PendingEmbeddingsCount(), "the cleared nodes and the existing candidate are the whole queue")

	// The reset is an update, not a rewrite: what a read returns besides the
	// vectors is the image the node already held.
	afterAlpha, err := engine.GetStoredNode(t.Context(), "test:alpha")
	require.NoError(t, err)
	require.Equal(t, beforeAlpha.Labels, afterAlpha.Labels)
	require.Equal(t, beforeAlpha.Properties, afterAlpha.Properties)
	require.Equal(t, beforeAlpha.CreatedAt, afterAlpha.CreatedAt)

	// Each cleared node publishes its own update through that owner, and the
	// reset publishes nothing for a node it did not clear.
	require.Eventually(t, func() bool {
		return len(updates.snapshot()) == 2
	}, 2*time.Second, 5*time.Millisecond, "each cleared node publishes an update, saw %v", updates.snapshot())
	observed := updates.snapshot()
	require.Equal(t, 1, observed["test:alpha"])
	require.Equal(t, 1, observed["test:beta"])
}

// A body the scan cannot decode stops the reset before it writes anything, so a
// corrupt record is reported instead of the namespace reset claiming success.
func TestClearAllEmbeddingsForPrefixRejectsCorruptPayloadWithoutClearing(t *testing.T) {
	engine := newClearResetEngine(t)
	for _, id := range []NodeID{"test:healthy-a", "test:healthy-b"} {
		createEmbeddedNode(t, engine, id)
	}
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(nodeKey("test:corrupt"), []byte("not-a-node-body"))
	}))

	cleared, err := engine.ClearAllEmbeddingsForPrefix("test:")
	require.ErrorIs(t, err, ErrInvalidData)
	require.ErrorContains(t, err, "test:corrupt")
	require.ErrorContains(t, err, "unexpected format byte")
	require.Zero(t, cleared, "the scan fails before the reset writes anything")

	for _, id := range []NodeID{"test:healthy-a", "test:healthy-b"} {
		requireStoredVectorCount(t, engine, id, 1)
	}
	require.Zero(t, engine.PendingEmbeddingsCount(), "a failed scan queues nothing")
}

// A reset that cannot write a node reports the nodes it did clear: partial
// progress is returned with the error, never as a clean success.
func TestClearAllEmbeddingsForPrefixReturnsPartialCountWhenALaterTargetFails(t *testing.T) {
	engine := newClearResetEngine(t)
	// The healthy node sorts first, so the reset clears it before it reaches the
	// record below. That record's key carries no namespace, which the canonical
	// node update rejects with the namespace-required error: a target failure the
	// reset cannot pass over.
	createEmbeddedNode(t, engine, "tenant:aaa")
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		unwritable := &Node{ID: "zz-unwritable", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.5}}}
		encoded, _, err := engine.encodeNodeInTxn(txn, "tenant", unwritable)
		if err != nil {
			return err
		}
		return txn.Set(nodeKey("zz-unwritable"), encoded)
	}))

	cleared, err := engine.ClearAllEmbeddingsForPrefix("")
	require.Error(t, err)
	require.Equal(t, 1, cleared, "the returned count is what the reset actually cleared")
	require.ErrorContains(t, err, "zz-unwritable")
	require.ErrorContains(t, err, "after clearing 1 of 2 nodes")

	requireStoredVectorCount(t, engine, "tenant:aaa", 0)
	requirePendingReset(t, engine, "tenant:aaa")
	requireStoredVectorCount(t, engine, "zz-unwritable", 1)
}

// The prefix scopes the reset: another namespace keeps its vectors and gains no
// queue entry, because the reset no longer refreshes the whole index.
func TestClearAllEmbeddingsForPrefixKeepsOtherNamespacesIntact(t *testing.T) {
	engine := newClearResetEngine(t)
	createEmbeddedNode(t, engine, "tenant_a:doc")
	createEmbeddedNode(t, engine, "tenant_b:doc")
	require.Zero(t, engine.PendingEmbeddingsCount())
	// Neither an unreadable foreign body nor an unscoped malformed key
	// belongs to this namespace; acquisition itself must be scoped.
	require.NoError(t, engine.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey("tenant_b:corrupt"), []byte("not-a-node-body")); err != nil {
			return err
		}
		return txn.Set([]byte{prefixNode}, []byte("invalid"))
	}))

	cleared, err := engine.ClearAllEmbeddingsForPrefix("tenant_a:")
	require.NoError(t, err)
	require.Equal(t, 1, cleared)

	requireStoredVectorCount(t, engine, "tenant_a:doc", 0)
	requirePendingReset(t, engine, "tenant_a:doc")
	requireStoredVectorCount(t, engine, "tenant_b:doc", 1)
	require.Equal(t, 1, engine.PendingEmbeddingsCount(), "only the cleared namespace is queued")
}

// Hidden stored nodes keep no embedding of the replaced model, even though no
// visible read returns them, and the reset leaves them hidden.
func TestClearAllEmbeddingsForPrefixClearsHiddenStoredNodes(t *testing.T) {
	engine := newClearResetEngine(t)
	createEmbeddedNode(t, engine, "test:hidden")
	createEmbeddedNode(t, engine, "test:visible")
	require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:hidden", true))
	_, err := engine.GetNode(t.Context(), "test:hidden")
	require.ErrorIs(t, err, ErrNotFound, "the hidden node is not part of a visible read")

	cleared, err := engine.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 2, cleared, "hidden nodes are cleared too")

	requireStoredVectorCount(t, engine, "test:hidden", 0)
	requireStoredVectorCount(t, engine, "test:visible", 0)
	requirePendingReset(t, engine, "test:hidden")
	requirePendingReset(t, engine, "test:visible")

	_, err = engine.GetNode(t.Context(), "test:hidden")
	require.ErrorIs(t, err, ErrNotFound, "the reset removes vectors, not the suppression")
	mustReadNode(t, engine.GetNode, "test:visible")
}
