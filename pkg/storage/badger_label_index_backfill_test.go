package storage

// Tests for the label-index startup backfill that closes the v1.1.1 /
// v1.1.2 bug where MATCH (n:Label) returned 0 for restored / pre-
// existing nodes after a server restart. Reproduce the on-disk shape
// the bug requires: node bodies present, label-index entries (prefix
// 0x03) absent. Reopen and confirm the backfill rebuilds them so
// MATCH (n:Label) sees every pre-existing node again.

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wipeLabelIndexAndMarker removes every prefix-0x03 entry and clears
// the rebuild marker, simulating a store that came over from a binary
// that didn't maintain the index, or a partial file copy.
func wipeLabelIndexAndMarker(t *testing.T, eng *BadgerEngine) {
	t.Helper()
	require.NoError(t, eng.db.DropPrefix([]byte{prefixLabelIndex}))
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(labelIndexReadyKey)
	}))
}

// waitForLabelIndexReady spins until the backfill marker is written
// or the deadline passes. Returns whether the marker landed.
func waitForLabelIndexReady(t *testing.T, eng *BadgerEngine, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready, err := eng.labelIndexReady()
		require.NoError(t, err)
		if ready {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestLabelIndexBackfill_RebuildsForPreexistingNodesAfterReopen — the
// reported bug, exactly. Create a store with 250 :Concept nodes,
// scrub the label-index range plus the marker (the on-disk shape of
// a v1.1.0-or-older store), close, and reopen. The backfill must
// rebuild every entry so MATCH (n:Concept) returns 250 again.
func TestLabelIndexBackfill_RebuildsForPreexistingNodesAfterReopen(t *testing.T) {
	dir := t.TempDir()

	// Stage 1: populate, then simulate "old binary wrote bodies without index".
	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	const n = 250
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &Node{
			ID:     NodeID(fmt.Sprintf("test:concept-%05d", i)),
			Labels: []string{"Concept"},
		}
	}
	require.NoError(t, engine1.BulkCreateNodes(nodes))
	wipeLabelIndexAndMarker(t, engine1)
	// Sanity: in-session GetNodesByLabel now returns 0 (index wiped).
	gone, err := engine1.GetNodesByLabel("Concept")
	require.NoError(t, err)
	require.Len(t, gone, 0, "label index must be empty after wipe (preconditions)")
	require.NoError(t, engine1.Close())

	// Stage 2: reopen — backfill must rebuild the index.
	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	require.True(t, waitForLabelIndexReady(t, engine2, 5*time.Second),
		"label-index backfill marker must be set within 5s after reopen")

	got, err := engine2.GetNodesByLabel("Concept")
	require.NoError(t, err)
	assert.Len(t, got, n,
		"after backfill, MATCH (n:Concept) must surface every pre-existing node; got %d/%d", len(got), n)
}

// TestLabelIndexBackfill_HandlesMultiLabelNodesCorrectly — the bug
// report cited `MATCH (c:Aprodan:Academic:Concept)` returning 0 vs.
// the 227 truth. Pin: every label on every node is restored, so each
// label is independently queryable after rebuild.
func TestLabelIndexBackfill_HandlesMultiLabelNodesCorrectly(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := engine1.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("test:multi-%03d", i)),
			Labels: []string{"Aprodan", "Academic", "Concept"},
		})
		require.NoError(t, err)
	}
	wipeLabelIndexAndMarker(t, engine1)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	require.True(t, waitForLabelIndexReady(t, engine2, 5*time.Second))

	for _, label := range []string{"Aprodan", "Academic", "Concept"} {
		got, err := engine2.GetNodesByLabel(label)
		require.NoError(t, err)
		assert.Len(t, got, 50,
			"after rebuild, label %q must surface all 50 multi-labelled nodes; got %d", label, len(got))
	}
}

// TestLabelIndexBackfill_SkipsEmptyStores — pin: an empty store
// completes the backfill synchronously (no goroutine, marker set
// immediately). Otherwise every fresh test/dev install pays a
// pointless background goroutine spin-up.
func TestLabelIndexBackfill_SkipsEmptyStores(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	ready, err := engine.labelIndexReady()
	require.NoError(t, err)
	assert.True(t, ready,
		"empty stores must mark the label-index backfill ready synchronously, not via the background goroutine")
}

// TestLabelIndexBackfill_IsIdempotentAcrossRestarts — once the marker
// lands, subsequent opens must NOT re-rebuild (the marker is the
// gate). Pin: rebuilding on every open would burn O(N) work
// indefinitely.
func TestLabelIndexBackfill_IsIdempotentAcrossRestarts(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := engine1.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("test:idem-%02d", i)),
			Labels: []string{"L"},
		})
		require.NoError(t, err)
	}
	require.NoError(t, engine1.Close())

	// Reopen: marker should already be set (every CreateNode in stage 1
	// went through the live write path which writes both body and
	// index; ensureLabelIndex runs on first open ever and short-circuits
	// because it found the marker missing AND the index already
	// consistent — actually wait: the marker isn't written until the
	// first open's ensureLabelIndex runs. Pin that subsequent opens
	// see the marker and don't redo work).
	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	require.True(t, waitForLabelIndexReady(t, engine2, 5*time.Second))
	require.NoError(t, engine2.Close())

	// Open a third time — marker is set, nothing should happen.
	engine3, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine3.Close() })

	// Third-open backfill goroutine must NOT run because the marker
	// is set. Probe: backfillDone is nil (no goroutine launched).
	engine3.labelIndexBackfillMu.Lock()
	stillIdle := engine3.labelIndexBackfillDone == nil
	engine3.labelIndexBackfillMu.Unlock()
	assert.True(t, stillIdle,
		"backfill must not relaunch on subsequent opens once the marker is set")

	// And queries still return the right rows.
	got, err := engine3.GetNodesByLabel("L")
	require.NoError(t, err)
	assert.Len(t, got, 10)
}

// TestLabelIndexBackfill_BackfillCancelledOnClose — the long-running
// goroutine must respect Close() so a fast-shutdown doesn't leak it
// past test cleanup. Pin: stopLabelIndexBackfill cancels and waits.
func TestLabelIndexBackfill_BackfillCancelledOnClose(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	const n = 5_000
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &Node{
			ID:     NodeID(fmt.Sprintf("test:bgshutdown-%05d", i)),
			Labels: []string{"BG"},
		}
	}
	require.NoError(t, engine1.BulkCreateNodes(nodes))
	wipeLabelIndexAndMarker(t, engine1)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	// Close immediately — without waiting for marker. stopLabelIndexBackfill
	// must cancel the goroutine and wait for it to exit.
	start := time.Now()
	require.NoError(t, engine2.Close())
	elapsed := time.Since(start)
	// 5s is generous; the cancel should propagate to the rebuild loop
	// at the next batch boundary.
	assert.Less(t, elapsed, 10*time.Second,
		"Close must not block indefinitely on the backfill goroutine; took %v", elapsed)
}

// TestLabelIndexBackfill_RebuildIsFreshNotMerged — if the on-disk
// index has stale entries from older numIDs (e.g. from a previous
// dict that recycled IDs), the rebuild MUST drop the prefix first
// and re-emit, not merge. Pin by writing a bogus prefix-0x03 entry
// for a label nobody owns; the rebuild must remove it.
func TestLabelIndexBackfill_RebuildIsFreshNotMerged(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:fresh-1", Labels: []string{"Real"}})
	require.NoError(t, err)

	// Plant a fake stale index entry. Use a numID nobody allocated
	// (max uint64 - 1) to ensure it can't accidentally point to a
	// real node.
	require.NoError(t, engine1.withUpdate(func(txn *badger.Txn) error {
		stale := labelIndexKey("ghost", 1<<63)
		return txn.Set(stale, []byte{})
	}))
	wipeMarkerOnly(t, engine1) // keep the planted entry, only wipe the marker
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	require.True(t, waitForLabelIndexReady(t, engine2, 5*time.Second))

	// "ghost" label must return 0 — the rebuild dropped the prefix.
	ghosts, err := engine2.GetNodesByLabel("ghost")
	require.NoError(t, err)
	assert.Empty(t, ghosts, "stale label-index entry must NOT survive the rebuild")

	// Real label still resolves.
	real, err := engine2.GetNodesByLabel("Real")
	require.NoError(t, err)
	assert.Len(t, real, 1)
}

// wipeMarkerOnly deletes the rebuild marker without touching the
// index range. Used to force the backfill to rerun against an
// existing (possibly stale) on-disk index.
func wipeMarkerOnly(t *testing.T, eng *BadgerEngine) {
	t.Helper()
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(labelIndexReadyKey)
	}))
}

// TestLabelIndexBackfill_NamespacedNodesRebuildIntoCorrectScope — the
// production stack writes node IDs with a database prefix
// ("nornic:foo-1"). The rebuild must preserve this so namespace-
// scoped reads still work correctly post-rebuild.
func TestLabelIndexBackfill_ReadyMarkerRequiresCurrentScheme(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, testCase := range []struct {
		name  string
		value []byte
		ready bool
	}{
		{name: "missing"},
		{name: "legacy", value: []byte{1}},
		{name: "empty", value: []byte{}},
		{name: "other_scheme", value: []byte{2}},
		{name: "current", value: []byte{3}, ready: true},
		{name: "malformed", value: []byte{3, 0}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				if testCase.value == nil {
					return txn.Delete(labelIndexReadyKey)
				}
				return txn.Set(labelIndexReadyKey, testCase.value)
			}))
			ready, err := engine.labelIndexReady()
			require.NoError(t, err)
			require.Equal(t, testCase.ready, ready)
		})
	}
}

func TestLabelIndexBackfill_LegacyMarkerRebuildsCurrentScheme(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, namespace := range []string{"alpha", "beta"} {
		store := NewNamespacedEngine(engine, namespace)
		_, err := store.CreateNode(&Node{ID: "existing", Labels: []string{"Label", "Second"}, Properties: map[string]interface{}{"id": namespace}})
		require.NoError(t, err)
	}
	wipeLabelIndexAndMarker(t, engine)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, []byte{1})
	}))
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	reopened.labelIndexBackfillMu.Lock()
	done := reopened.labelIndexBackfillDone
	reopened.labelIndexBackfillMu.Unlock()
	require.NotNil(t, done, "a legacy ready marker must schedule the existing backfill")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy label-index backfill did not finish")
	}
	require.NoError(t, reopened.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(labelIndexReadyKey)
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			assert.Equal(t, []byte{3}, value)
			return nil
		})
	}))
	for _, namespace := range []string{"alpha", "beta"} {
		store := NewNamespacedEngine(reopened, namespace)
		for _, label := range []string{"Label", "Second"} {
			nodes, err := store.GetNodesByLabel(label)
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, NodeID("existing"), nodes[0].ID)
			require.Equal(t, namespace, nodes[0].Properties["id"])
		}
	}
	require.NoError(t, reopened.Close())
	again, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = again.Close() })
	again.labelIndexBackfillMu.Lock()
	nextBackfill := again.labelIndexBackfillDone
	again.labelIndexBackfillMu.Unlock()
	require.Nil(t, nextBackfill, "current marker must prevent repeated backfill")
}

func TestLabelIndexBackfill_EmptyStoreMarksCurrentScheme(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_%t", legacy), func(t *testing.T) {
			dir := t.TempDir()
			engine, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				if legacy {
					return txn.Set(labelIndexReadyKey, []byte{1})
				}
				return txn.Delete(labelIndexReadyKey)
			}))
			require.NoError(t, engine.Close())
			reopened, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			reopened.labelIndexBackfillMu.Lock()
			done := reopened.labelIndexBackfillDone
			reopened.labelIndexBackfillMu.Unlock()
			require.Nil(t, done)
			require.NoError(t, reopened.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(labelIndexReadyKey)
				if err != nil {
					return err
				}
				return item.Value(func(value []byte) error {
					assert.Equal(t, []byte{3}, value)
					return nil
				})
			}))
		})
	}
}

func TestLabelIndexBackfill_NamespacedNodesRebuildIntoCorrectScope(t *testing.T) {
	dir := t.TempDir()

	inner1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	ns1 := NewNamespacedEngine(inner1, "nornic")

	for i := 0; i < 30; i++ {
		_, err := ns1.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("ns-rebuild-%02d", i)),
			Labels: []string{"Memory"},
		})
		require.NoError(t, err)
	}
	wipeLabelIndexAndMarker(t, inner1)
	require.NoError(t, inner1.Close())

	inner2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner2.Close() })
	ns2 := NewNamespacedEngine(inner2, "nornic")

	require.True(t, waitForLabelIndexReady(t, inner2, 5*time.Second))

	got, err := ns2.GetNodesByLabel("Memory")
	require.NoError(t, err)
	assert.Len(t, got, 30,
		"NamespacedEngine read after backfill must surface every namespace-scoped node; got %d/30", len(got))
}
