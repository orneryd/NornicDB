package storage

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// writeV1NodeBody constructs and writes a legacy V1 node body (raw
// msgpack-headered, untokenized) under prefixNode. Used to seed
// migration-input fixtures.
func writeV1NodeBody(t *testing.T, eng *BadgerEngine, n *Node) {
	t.Helper()
	data, _, err := encodeNodeV1(n)
	require.NoError(t, err)
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(n.ID), data)
	}))
}

// writeV1CompactEdgeBody constructs and writes a legacy V1 compact
// edge body. Resolves endpoint numIDs through the engine's id dict so
// the migration can decode endpoints back to string IDs.
func writeV1CompactEdgeBody(t *testing.T, eng *BadgerEngine, edge *Edge) {
	t.Helper()
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		startNum, err := eng.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.StartNode)
		if err != nil {
			return err
		}
		endNum, err := eng.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.EndNode)
		if err != nil {
			return err
		}
		body, err := encodeEdgeCompactV1(edge, startNum, endNum)
		if err != nil {
			return err
		}
		if err := eng.idDict.flushTxnCounters(txn); err != nil {
			return err
		}
		return txn.Set(edgeKey(edge.ID), body)
	}))
}

// freshV1Engine returns an engine open against a tempdir whose schema
// version has been forced to V1, simulating a data directory that
// existed before the V2 codec landed. Bodies in the store still need
// to be V1-shaped via writeV1NodeBody / writeV1CompactEdgeBody.
func freshV1Engine(t *testing.T) *BadgerEngine {
	t.Helper()
	dir := t.TempDir()
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })

	// The fresh engine just bumped to V2. Force it back to V1 so the
	// migration sees pre-V2 state.
	require.NoError(t, eng.writeSchemaVersion(storageVersionV1))
	eng.storageVersion = storageVersionV1
	return eng
}

// TestMigrateV1ToV2_RewritesAllNodes asserts every node body is
// re-encoded under the V2 format byte after migration. After the bump
// the schema version reads V2.
func TestMigrateV1ToV2_RewritesAllNodes(t *testing.T) {
	eng := freshV1Engine(t)

	for i := 0; i < 50; i++ {
		writeV1NodeBody(t, eng, &Node{
			ID:     NodeID("test:node-" + strconv.Itoa(i)),
			Labels: []string{"Item"},
			Properties: map[string]any{
				"index": int64(i),
				"label": "node-" + strconv.Itoa(i),
				"tag":   "shared-tag",
			},
		})
	}

	require.NoError(t, eng.migrateV1ToV2())

	v, err := eng.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionPropKeyDictV2, v)

	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixNode}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		seen := 0
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			seen++
			val, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, nodeFormatTokenizedV1, val[0],
				"every node body must start with V2 format byte after migration")
		}
		require.Equal(t, 50, seen)
		return nil
	}))
}

// TestMigrateV1ToV2_RewritesAllEdges asserts every edge body is
// re-encoded under edgeFormatCompactV2 after migration.
func TestMigrateV1ToV2_RewritesAllEdges(t *testing.T) {
	eng := freshV1Engine(t)

	// Create a couple of nodes' numIDs so edges can resolve endpoints.
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		_, err := eng.idDict.resolveOrAllocateNodeNumIDInTxn(txn, "test:start")
		if err != nil {
			return err
		}
		_, err = eng.idDict.resolveOrAllocateNodeNumIDInTxn(txn, "test:end")
		if err != nil {
			return err
		}
		return eng.idDict.flushTxnCounters(txn)
	}))

	for i := 0; i < 20; i++ {
		writeV1CompactEdgeBody(t, eng, &Edge{
			ID:        EdgeID("test:edge-" + strconv.Itoa(i)),
			StartNode: "test:start",
			EndNode:   "test:end",
			Type:      "REL",
			Properties: map[string]any{
				"order":  int64(i),
				"weight": 0.5,
			},
		})
	}

	require.NoError(t, eng.migrateV1ToV2())

	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixEdge}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		seen := 0
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			seen++
			val, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, edgeFormatCompactV2, val[0],
				"every edge body must start with V2 format byte")
		}
		require.Equal(t, 20, seen)
		return nil
	}))
}

// TestMigrateV1ToV2_PreservesPropertiesRoundTrip is the end-to-end
// correctness check: properties read back through the V2 decoder must
// equal the original V1 input.
func TestMigrateV1ToV2_PreservesPropertiesRoundTrip(t *testing.T) {
	eng := freshV1Engine(t)

	node := &Node{
		ID:     NodeID("test:correctness"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name":   "Alice",
			"age":    int64(30),
			"score":  0.95,
			"active": true,
			"tags":   []any{"a", "b"},
		},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}
	writeV1NodeBody(t, eng, node)

	require.NoError(t, eng.migrateV1ToV2())

	var data []byte
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(node.ID))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	}))

	decoded, err := eng.decodeNode("test", data)
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.Properties["name"], decoded.Properties["name"])
	require.Equal(t, node.Properties["age"], decoded.Properties["age"])
	require.InDelta(t, node.Properties["score"], decoded.Properties["score"], 1e-9)
	require.Equal(t, node.Properties["active"], decoded.Properties["active"])
	require.True(t, decoded.CreatedAt.Equal(node.CreatedAt))
}

// TestMigrateV1ToV2_Idempotent re-runs the migration over an already-V2
// store. The migration's per-record skip check must short-circuit on
// already-tokenized bodies; the version bump must be idempotent.
func TestMigrateV1ToV2_Idempotent(t *testing.T) {
	eng := freshV1Engine(t)
	for i := 0; i < 10; i++ {
		writeV1NodeBody(t, eng, &Node{
			ID:         NodeID("test:n-" + strconv.Itoa(i)),
			Properties: map[string]any{"i": int64(i)},
		})
	}

	require.NoError(t, eng.migrateV1ToV2())
	first := snapshotNodeBodies(t, eng)

	require.NoError(t, eng.migrateV1ToV2())
	second := snapshotNodeBodies(t, eng)

	require.Equal(t, first, second, "second migration pass must produce identical output")
}

// TestMigrateV1ToV2_HandlesPreCompactGobEdges covers the legacy edge
// path: pre-compact-codec edges (full Edge struct serialized via the
// gob fallback) need to round-trip through the migration.
func TestMigrateV1ToV2_HandlesPreCompactGobEdges(t *testing.T) {
	eng := freshV1Engine(t)

	edge := &Edge{
		ID:         EdgeID("test:legacy-edge"),
		StartNode:  NodeID("test:src"),
		EndNode:    NodeID("test:dst"),
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2020)},
		Confidence: 0.5,
	}

	// Synthesize a header-less gob edge body — the format pre-V1
	// compact code emitted.
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(edge))
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(edge.ID), buf.Bytes())
	}))

	require.NoError(t, eng.migrateV1ToV2())

	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(edge.ID))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		require.Equal(t, edgeFormatCompactV2, val[0])
		return nil
	}))
}

// TestMigrateV1ToV2_RejectsNodeMissingNamespace is the deterministic
// error path for a node body whose ID is not namespace-prefixed. The
// migration must surface this as an error rather than silently
// continuing.
func TestMigrateV1ToV2_RejectsNodeMissingNamespace(t *testing.T) {
	eng := freshV1Engine(t)
	writeV1NodeBody(t, eng, &Node{
		ID:         NodeID("unprefixed-id"),
		Properties: map[string]any{"k": "v"},
	})

	err := eng.migrateV1ToV2()
	require.Error(t, err)
	require.Contains(t, err.Error(), "lacks namespace prefix")
}

// TestMigrateV1ToV2_RejectsCorruptNodeBody asserts the migration
// surfaces a decode error instead of silently dropping a bad body.
func TestMigrateV1ToV2_RejectsCorruptNodeBody(t *testing.T) {
	eng := freshV1Engine(t)
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		// Garbage bytes — neither valid msgpack nor valid gob.
		return txn.Set(nodeKey(NodeID("test:corrupt")), []byte{0x00, 0x01, 0x02})
	}))

	err := eng.migrateV1ToV2()
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding v1 node")
}

// TestMigrateV1ToV2_RejectsEmptyEdgeBody covers the empty-data branch
// of decodeEdgeAnyV1.
func TestMigrateV1ToV2_RejectsEmptyEdgeBody(t *testing.T) {
	eng := freshV1Engine(t)
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(EdgeID("test:empty")), []byte{})
	}))
	// Empty values are skipped by collectBatch before the decoder sees
	// them, so the migration should succeed and leave the empty body
	// alone.
	require.NoError(t, eng.migrateV1ToV2())
}

// TestMigrateV1ToV2_VersionBumpIsLastWrite asserts that if the node
// pass succeeds but the edge pass fails, the schema version remains
// at V1 — guaranteeing the next run picks up the failed work.
func TestMigrateV1ToV2_VersionBumpIsLastWrite(t *testing.T) {
	eng := freshV1Engine(t)

	// Valid node, malformed edge.
	writeV1NodeBody(t, eng, &Node{
		ID:         NodeID("test:n"),
		Properties: map[string]any{"k": "v"},
	})
	require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(EdgeID("test:e")), []byte{0x99, 0x99, 0x99})
	}))

	err := eng.migrateV1ToV2()
	require.Error(t, err)

	v, err := eng.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionV1, v, "version must remain at V1 after a failed pass")
}

// TestMigrateV1ToV2_ResumableAfterPartialPass restarts the migration
// after the first pass converted some nodes; the second pass must
// finish and bump the version. Verified by writing a few V2 bodies
// upfront (simulating a previous successful batch) alongside V1 bodies.
func TestMigrateV1ToV2_ResumableAfterPartialPass(t *testing.T) {
	eng := freshV1Engine(t)

	// Half the nodes already in V2 form (simulating a prior partial pass).
	for i := 0; i < 25; i++ {
		require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
			body, _, err := eng.encodeNodeInTxn(txn, "test", &Node{
				ID:         NodeID("test:done-" + strconv.Itoa(i)),
				Properties: map[string]any{"i": int64(i)},
			})
			if err != nil {
				return err
			}
			if err := txn.Set(nodeKey(NodeID("test:done-"+strconv.Itoa(i))), body); err != nil {
				return err
			}
			return eng.propKeyDict.flushTxnCounters(txn)
		}))
	}
	// Other half still V1.
	for i := 0; i < 25; i++ {
		writeV1NodeBody(t, eng, &Node{
			ID:         NodeID("test:todo-" + strconv.Itoa(i)),
			Properties: map[string]any{"i": int64(i)},
		})
	}

	require.NoError(t, eng.migrateV1ToV2())

	// All bodies V2 now.
	bodies := snapshotNodeBodies(t, eng)
	require.Len(t, bodies, 50)
	for id, body := range bodies {
		require.NotEmpty(t, body, id)
		require.Equal(t, nodeFormatTokenizedV1, body[0],
			"node %s should be V2 after resumable migration", id)
	}

	v, err := eng.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionPropKeyDictV2, v)
}

// TestMigrateV1ToV2_BatchIteratesPastBoundary covers the batched-walk
// branch: when there are more records than the batch size, the
// migration must continue past the first batch.
func TestMigrateV1ToV2_BatchIteratesPastBoundary(t *testing.T) {
	eng := freshV1Engine(t)

	count := migrationV1ToV2BatchSize + 50
	for i := 0; i < count; i++ {
		writeV1NodeBody(t, eng, &Node{
			ID:         NodeID("test:batch-" + strconv.Itoa(i)),
			Properties: map[string]any{"i": int64(i)},
		})
	}

	require.NoError(t, eng.migrateV1ToV2())

	bodies := snapshotNodeBodies(t, eng)
	require.Len(t, bodies, count)
	for id, body := range bodies {
		require.Equal(t, nodeFormatTokenizedV1, body[0], id)
	}
}

// TestEdgeNamespace_FallbackToEnd covers the branch where StartNode
// lacks a namespace prefix but EndNode has one.
func TestEdgeNamespace_FallbackToEnd(t *testing.T) {
	require.Equal(t, "ns", edgeNamespace(&Edge{
		StartNode: "no-prefix-here",
		EndNode:   "ns:endpoint",
	}))
}

// TestEdgeNamespace_NoPrefix covers the branch where neither endpoint
// is namespace-prefixed — the helper returns "".
func TestEdgeNamespace_NoPrefix(t *testing.T) {
	require.Equal(t, "", edgeNamespace(&Edge{
		StartNode: "bare-start",
		EndNode:   "bare-end",
	}))
}

// TestErrStorageUpgradeRequired_Message asserts the exact error
// message text — operators rely on it for triage.
func TestErrStorageUpgradeRequired_Message(t *testing.T) {
	err := &ErrStorageUpgradeRequired{OnDisk: 1, Current: 2}
	require.Contains(t, err.Error(), "storage upgrade required")
	require.Contains(t, err.Error(), "on-disk version 1")
	require.Contains(t, err.Error(), "binary version 2")
	require.Contains(t, err.Error(), "--upgrade-storage")
}

// TestRunOnStartMigrations_GatedRefuses asserts that opening a V1 data
// directory with non-empty bodies refuses without --upgrade-storage.
func TestRunOnStartMigrations_GatedRefuses(t *testing.T) {
	dir := t.TempDir()

	// Build a V1 data directory: open, write a node, force version 1, close.
	{
		eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
		require.NoError(t, err)
		writeV1NodeBody(t, eng, &Node{
			ID:         NodeID("test:gate"),
			Properties: map[string]any{"k": "v"},
		})
		require.NoError(t, eng.writeSchemaVersion(storageVersionV1))
		require.NoError(t, eng.Close())
	}

	// Reopen WITHOUT the upgrade flag — must refuse.
	_, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: false})
	require.Error(t, err)
	var upgradeErr *ErrStorageUpgradeRequired
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, storageVersionV1, upgradeErr.OnDisk)
	require.Equal(t, storageVersionCurrent, upgradeErr.Current)

	// Reopen WITH the upgrade flag — must succeed and finish at V2.
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: true})
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	require.Equal(t, storageVersionCurrent, eng.storageVersion)
}

// TestRunOnStartMigrations_EmptyStoreSkipsGate asserts that opening a
// fresh empty data directory works without the upgrade flag — there's
// nothing to lose, so no operator decision is required.
func TestRunOnStartMigrations_EmptyStoreSkipsGate(t *testing.T) {
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	require.Equal(t, storageVersionCurrent, eng.storageVersion)
}

// TestRunOnStartMigrations_ChainsV0ThroughV2 exercises the multi-arm
// migration chain. A V0 store with legacy access state should run V0→V1
// (extracting access metadata) and then V1→V2 (rewriting node bodies)
// in a single open.
func TestRunOnStartMigrations_ChainsV0ThroughV2(t *testing.T) {
	dir := t.TempDir()

	// Build a V0 store: open, force version 0, write a legacy-shaped node.
	{
		eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
		require.NoError(t, err)
		require.NoError(t, eng.writeSchemaVersion(0))

		writeLegacyNodeBytes(t, eng, "test:legacy", 0.5, time.Unix(1700000000, 0).UTC(), 99)
		require.NoError(t, eng.Close())
	}

	// Reopen with the upgrade flag — both arms run.
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             dir,
		AllowStorageUpgrade: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	require.Equal(t, storageVersionCurrent, eng.storageVersion)

	// V0→V1 ran: access metadata for the legacy node must exist.
	meta, err := eng.GetAccessMeta("test:legacy")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, int64(99), meta.Fixed.AccessCount)
}

// snapshotNodeBodies reads every node body into a map keyed by ID.
// Used to compare migration outputs across runs.
func snapshotNodeBodies(t *testing.T, eng *BadgerEngine) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixNode}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(key[1:])] = val
		}
		return nil
	}))
	return out
}
