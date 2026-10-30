package storage

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"

	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

// Build an actual V2 body, including legacy inline embedding payloads. The
// primary key and labels stay fixed while each excluded payload grows.

func labelMetadataAllocatedBytes(t *testing.T, read func() error) uint64 {
	t.Helper()
	require.NoError(t, read())
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const rounds = 12
	for i := 0; i < rounds; i++ {
		require.NoError(t, read())
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / rounds
}

func TestLabelMetadataPayloadIndependence(t *testing.T) {
	b, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer b.Close()
	_, err = b.writeLabelIndexBuildingMarker(b.labelIndex.markerGeneration.Load() + 1)
	require.NoError(t, err)
	var baseline map[string]uint64
	for _, size := range []int{0, 4096, 512 * 1024} {
		body := labelMetadataBody(t, b, size, false)
		costs := map[string]uint64{}
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			reads := map[string]func() error{
				"decoder": func() error {
					metadata, err := decodeNodeMetadata(body)
					if err == nil && !labelsContain(metadata.Labels, "Target") {
						return fmt.Errorf("missing decoder label")
					}
					return err
				},
				// Badger's physical body acquisition can allocate its raw value buffers;
				// compare real label reads against the same acquisition without decoding.
				"raw_body": func() error {
					item, err := txn.Get(nodeKey("test:metadata"))
					if err != nil {
						return err
					}
					return item.Value(func([]byte) error { return nil })
				},
				"point": func() error {
					found, err := b.nodeHasLabelInTxn(txn, "test:metadata", "Target")
					if err == nil && !found {
						return fmt.Errorf("missing point label")
					}
					return err
				},
				"candidates": func() error {
					r := b.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", storedRead, false)
					defer r.Close()
					if !r.Next() || r.NodeID() != "test:metadata" {
						return fmt.Errorf("missing candidate: %v", r.Err())
					}
					if r.Next() {
						return fmt.Errorf("extra candidate")
					}
					return r.Err()
				},
				"count_metadata": func() error {
					labels, err := decodeStoredNodeLabels(body)
					if err == nil && !labelsContain(labels, "Target") {
						return fmt.Errorf("missing count label")
					}
					return err
				},
				// Planted full-materialization alternative must violate the property.
				"full_negative_control": func() error { _, err := b.decodeNode("test", body); return err },
			}
			for name, read := range reads {
				costs[name] = labelMetadataAllocatedBytes(t, read)
			}
			return nil
		}))
		t.Logf("payload=%d bytes, allocated bytes/read=%v", size, costs)
		if size == 0 {
			baseline = costs
			continue
		}
		for _, name := range []string{"decoder", "count_metadata"} {
			require.LessOrEqual(t, costs[name], baseline[name]+32*1024, "%s copied excluded payload of size %d", name, size)
		}
		for _, name := range []string{"point", "candidates"} {
			require.LessOrEqual(t, costs[name], costs["raw_body"]+baseline[name]+32*1024, "%s allocated payload beyond physical body acquisition, size %d", name, size)
		}
		if size == 512*1024 {
			require.Greater(t, costs["full_negative_control"], baseline["full_negative_control"]+512*1024, "negative control must detect full decoding")
		}
	}
}

func TestLabelMetadataSkipsExcludedValues(t *testing.T) {
	b, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer b.Close()
	_, err = b.writeLabelIndexBuildingMarker(b.labelIndex.markerGeneration.Load() + 1)
	require.NoError(t, err)
	body := labelMetadataBody(t, b, 4096, true)
	_, err = b.decodeNode("test", body)
	require.Error(t, err, "negative control: a full decoder must try the excluded value")
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		found, err := b.nodeHasLabelInTxn(txn, "test:metadata", "Target")
		require.NoError(t, err)
		require.True(t, found)
		r := b.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", storedRead, false)
		defer r.Close()
		require.True(t, r.Next(), "stored discovery must include suppressed nodes without decoding excluded values: %v", r.Err())
		require.Equal(t, NodeID("test:metadata"), r.NodeID())
		return r.Err()
	}))
}

// Keep the fixture's framing independent of the production decoder.

func TestLabelCursorContextReachesPublicScan(t *testing.T) {
	for _, ready := range []bool{true, false} {
		t.Run(fmt.Sprintf("ready=%t", ready), func(t *testing.T) {
			b := createTestBadgerEngine(t)
			_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
			require.NoError(t, err)
			if !ready {
				_, err = b.writeLabelIndexBuildingMarker(b.labelIndex.markerGeneration.Load() + 1)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hook := func() { cancel() }
			b.labelIndex.afterAdmission.Store(&hook)
			defer b.labelIndex.afterAdmission.Store(nil)
			seen := 0
			err = b.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: "test:", Label: "Target", QueryNodes: true}, func(*Node) error { seen++; return nil })
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, seen)
		})
	}
}

func TestLabelCursorContextStopsScopedCollection(t *testing.T) {
	b := createTestBadgerEngine(t)
	// The first foreign entry makes the ready reader switch to its snapshot's
	// scoped dictionary. Cancellation at that boundary must stop collection.
	for _, id := range []NodeID{"foreign:a", "test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hook := func() { cancel() }
	b.labelIndex.beforeReverseLookup.Store(&hook)
	defer b.labelIndex.beforeReverseLookup.Store(nil)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		r := b.newLabelIndexReader(ctx, txn, "test:", "Target", "", storedRead, false)
		defer r.Close()
		require.False(t, r.Next())
		require.ErrorIs(t, r.Err(), context.Canceled)
		ready, ok := r.(*nodeIndexReader)
		require.True(t, ok)
		require.Empty(t, ready.candidates, "no dictionary candidate may be allocated after cancellation")
		return nil
	}))
}

func TestLabelMetadataSkipsEmbeddingDecoder(t *testing.T) {
	b, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer b.Close()
	_, err = b.writeLabelIndexBuildingMarker(b.labelIndex.markerGeneration.Load() + 1)
	require.NoError(t, err)
	metadata, err := encodeValue(map[string]any{"ID": "test:metadata", "Labels": []string{"Target"}, "EmbedMeta": map[string]any{"trap": msgpack.RawMessage{0xd4, 99, 0}}})
	require.NoError(t, err)
	// Valid empty tokenized properties; only the embedding metadata is a trap.
	body := append([]byte{nodeFormatTokenizedV1, 1, 0}, metadata...)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("test:metadata"), body) }))
	_, err = b.decodeNode("test", body)
	require.Error(t, err, "negative control must invoke the excluded embedding decoder")
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		found, err := b.nodeHasLabelInTxn(txn, "test:metadata", "Target")
		require.NoError(t, err)
		require.True(t, found)
		r := b.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", storedRead, false)
		defer r.Close()
		require.True(t, r.Next(), "%v", r.Err())
		return r.Err()
	}))
	labels, err := decodeStoredNodeLabels(body)
	require.NoError(t, err)
	require.Equal(t, []string{"Target"}, labels)
	// The label rebuild is also a metadata consumer, never a full-body reader.
	_, err = b.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
}

// Cancel from the actual cursor's context polls, after it has begun collecting
// candidates, without a timer or a scheduling race.
type labelCursorCancelContext struct {
	context.Context
	remaining int
	cancel    context.CancelFunc
}

func (c *labelCursorCancelContext) Err() error {
	if c.remaining > 0 {
		c.remaining--
		if c.remaining == 0 {
			c.cancel()
		}
	}
	return c.Context.Err()
}

func TestLabelCursorContextDuringScopedCollection(t *testing.T) {
	b := createTestBadgerEngine(t)
	for i := 0; i < 8; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:%d", i)), Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &labelCursorCancelContext{Context: parent, cancel: cancel}
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		cursor := b.newLabelIndexReader(ctx, txn, "test:", "Target", "", storedRead, false)
		defer cursor.Close()
		r, ok := cursor.(*nodeIndexReader)
		require.True(t, ok)
		ctx.remaining = 3
		r.useScopedCandidates()
		require.ErrorIs(t, r.Err(), context.Canceled)
		require.Len(t, r.candidates, 1, "collection must stop between dictionary entries")
		require.False(t, r.Next())
		return nil
	}))
}

// Exercise cancellation within work that used to run before the visitor.
type labelOwnerCancelContext struct {
	context.Context
	owner   string
	cancel  context.CancelFunc
	reached bool
}

func (c *labelOwnerCancelContext) Err() error {
	var pcs [64]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, c.owner) {
			c.reached = true
			c.cancel()
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestLabelContextOnePassTail(t *testing.T) {
	for _, physical := range []bool{false, true} {
		t.Run(fmt.Sprint(physical), func(t *testing.T) {
			b := createTestBadgerEngine(t)
			nodes := make([]*Node, labelScanPointLookups+8)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:%06d", i)), Labels: []string{"Target"}}
			}
			require.NoError(t, b.BulkCreateNodes(nodes))
			b.nodeCacheMu.Lock()
			b.nodeCache = map[NodeID]*Node{}
			b.nodeCacheMu.Unlock()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &labelOwnerCancelContext{Context: parent, owner: "readNodeRecordsInOnePass", cancel: cancel}
			seen := 0
			opts := StreamNodesOptions{Label: "Target", Prefix: "test:", QueryNodes: true}
			var err error
			if physical {
				tx, e := b.BeginTransaction()
				require.NoError(t, e)
				defer tx.Rollback()
				err = tx.StreamNodesWithOptions(ctx, opts, func(*Node) error { seen++; return nil })
			} else {
				err = b.StreamNodesWithOptions(ctx, opts, func(*Node) error { seen++; return nil })
			}
			require.True(t, ctx.reached, "must enter one-pass tail")
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, labelScanPointLookups, seen)
		})
	}
}
func TestLabelContextLogicalScopedPreparation(t *testing.T) {
	b := createTestBadgerEngine(t)
	for _, id := range []NodeID{"foreign:a", "test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	tx.mu.Lock()
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
		tx.snapshotTx = nil
	}
	tx.mu.Unlock()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &labelOwnerCancelContext{Context: parent, owner: "useScopedCandidates", cancel: cancel}
	seen := 0
	err = tx.StreamNodesWithOptions(ctx, StreamNodesOptions{QueryNodes: true, Prefix: "test:"}, func(*Node) error { seen++; return nil })
	require.True(t, ctx.reached)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, seen)
}

// The constructor population is extracted from every production storage file,
// including closures. New call sites require an explicit inventory decision.
func TestLabelContextCreationPopulation(t *testing.T) {
	expected := map[string]int{
		"GetFirstNodeByLabelInScopeWithContext": 1, "ForEachNodeIDByLabelInScopeWithContext": 1,
		"getNodesByLabelInScopeWithContext": 1, "streamNodesByLabelProjectedInScopeWithContext": 1,
		"streamNodesByLabelAtSnapshotWithViewContext": 1, "streamNodesByLabelFromPhysicalSnapshotAfterReadWithContext": 1,
		"scanForUniqueViolationInTxn": 1, "scanForNodeKeyViolationInTxn": 1, "scanForTemporalOverlapInTxn": 1,
		"countLabelIndexEntriesInNamespaceWithContext": 1, "countStoredLabelNodes": 1,
		"newLabelIndexReader": 1, "iterateNodesVisibleAtInScopeInTxn": 1,
	}
	inv, err := workcensus.Extract(workcensus.Config{Dir: "../..", Patterns: []string{"./pkg/storage"}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Packages: []string{"github.com/orneryd/nornicdb/pkg/storage"}, Selectors: []workcensus.Selector{{Kind: "constructor", Node: "call", Reachability: "all", Names: []string{"newLabelIndexReader", "newNodeIndexReader"}, Packages: []string{"github.com/orneryd/nornicdb/pkg/storage"}}}})
	require.NoError(t, err)
	actual := map[string]int{}
	for _, m := range inv.Members {
		owner := strings.Split(m.Owner, "$")[0]
		owner = owner[strings.LastIndex(owner, ".")+1:]
		actual[owner]++
		require.NotEmpty(t, m.Args, m.ID)
		if len(m.Args) > 0 {
			require.Equal(t, "ctx", m.Args[0], m.ID)
		}
	}
	require.Equal(t, expected, actual)
}

// Check the statement side as well as the constructor side. A new statement
// call into a context-free adapter must fail even if every constructor still
// has the same spelling and count.

func TestLabelContextOnePassStopsBeforeNextCallback(t *testing.T) {
	b := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		err := readNodeRecordsInOnePass(parent, txn, "test:", []NodeID{"test:a", "test:b"}, func(NodeID, *badger.Item) error { seen++; cancel(); return nil })
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, seen)
		return nil
	}))
}

func TestLabelContextCountRepair(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Set(labelCountKey("test", "Target"), []byte{1}) }))
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &labelOwnerCancelContext{Context: parent, owner: "newLabelIndexReader", cancel: cancel}
	_, err = b.NodeCountByLabelInNamespaceWithContext(ctx, "test", "Target")
	require.True(t, ctx.reached)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLabelContextBufferedPreparation(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{ID: "test:a", Labels: []string{"Target"}})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &labelOwnerCancelContext{Context: parent, owner: "visitEntityOverlayLocked", cancel: cancel}
	seen := 0
	err := b.StreamNodesWithOptions(ctx, StreamNodesOptions{Label: "Target", QueryNodes: true}, func(*Node) error { seen++; return nil })
	require.True(t, ctx.reached)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, seen)
}

func TestLabelContextNamespaceMaterialization(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &labelOwnerCancelContext{Context: parent, owner: "materializedValuesWithContext", cancel: cancel}
	_, err = NewNamespacedEngine(b, "test").GetNodesByLabelWithContext(ctx, "Target")
	require.True(t, ctx.reached)
	require.ErrorIs(t, err, context.Canceled)
}
