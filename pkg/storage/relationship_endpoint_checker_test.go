package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every layer of the server's storage stack answers whether a relationship's
// end node is visible the way GetNode would, including staged async writes,
// and declines to answer while decay filtering can hide a node.
func TestRelationshipEndpointVisibleAcrossStack(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	walBacking, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	wal := NewWALEngine(badger, walBacking)
	async := NewAsyncEngine(wal, &AsyncEngineConfig{FlushInterval: time.Hour})
	namespaced := NewNamespacedEngine(async, "ns")

	_, err = namespaced.CreateNode(&Node{ID: "flushed", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&Node{ID: "deleted", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	require.NoError(t, namespaced.DeleteNode("deleted"))
	_, err = namespaced.CreateNode(&Node{ID: "staged", Labels: []string{"Doc"}})
	require.NoError(t, err)

	check := func(checker RelationshipEndpointChecker, id NodeID, visible bool) {
		t.Helper()
		got, answered := checker.RelationshipEndpointVisible(id)
		require.True(t, answered, id)
		require.Equal(t, visible, got, id)
	}
	check(namespaced, "flushed", true)
	check(namespaced, "staged", true)
	check(namespaced, "deleted", false)
	check(namespaced, "missing", false)
	check(async, "ns:staged", true)
	check(wal, "ns:flushed", true)
	check(badger, "ns:flushed", true)
	check(badger, "ns:missing", false)

	_, answered := badger.RelationshipEndpointVisible("")
	require.False(t, answered)
	badger.SetDecayEnabled(true)
	_, answered = namespaced.RelationshipEndpointVisible("flushed")
	require.False(t, answered)

	// Layers above an engine that can't answer don't answer either.
	plain := NewNamespacedEngine(nonCheckingEngine{Engine: badger}, "ns")
	_, answered = plain.RelationshipEndpointVisible("flushed")
	require.False(t, answered)
	_, answered = NewWALEngine(nonCheckingEngine{Engine: badger}, walBacking).RelationshipEndpointVisible("ns:flushed")
	require.False(t, answered)
	plainAsync := NewAsyncEngine(nonCheckingEngine{Engine: badger}, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer plainAsync.Close()
	_, answered = plainAsync.RelationshipEndpointVisible("ns:flushed")
	require.False(t, answered)
}

// nonCheckingEngine hides RelationshipEndpointChecker from the engine it wraps.
type nonCheckingEngine struct{ Engine }
