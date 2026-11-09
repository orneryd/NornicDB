package storage

// projectedNodeDecoder snapshot name ownership (NOR-214): the scan-scoped
// decoder resolves every stored property-key token through the scan's own
// Badger snapshot (readStoredBinding), memoized only within this decoder and
// namespace. A pinned transaction therefore keeps the names its bodies were
// written with, this writer's staged names stay visible to this writer, a
// buffered write publishes the same names once it commits, and the memo never
// carries a token's name from one namespace into another.

import (
	"context"
	"sort"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// projectedScan collects the properties a projected scan decodes per node ID.
func projectedScan(t *testing.T, stream func(opts StreamNodesOptions, visit func(*Node) error) error, opts StreamNodesOptions) map[NodeID]map[string]interface{} {
	t.Helper()
	got := make(map[NodeID]map[string]interface{})
	require.NoError(t, stream(opts, func(node *Node) error {
		got[node.ID] = node.Properties
		return nil
	}))
	return got
}

func projectedScanIDs(got map[NodeID]map[string]interface{}) []string {
	ids := make([]string, 0, len(got))
	for id := range got {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return ids
}

// A token's name is resolved in the namespace of the node body, so two
// namespaces that give the same token number different names decode to their
// own name. A memo that survived the namespace change would report the first
// namespace's name for the second namespace's body.
func TestProjectedNodeDecoderTokenNamesArePerNamespace(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "x"}},
		{ID: "b:1", Labels: []string{"P"}, Properties: map[string]interface{}{"kind": "y"}},
		{ID: "a:2", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "z", "other": "unprojected"}},
		{ID: "b:2", Labels: []string{"P"}, Properties: map[string]interface{}{"kind": "w"}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	got := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return engine.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"name", "kind"}})
	require.Equal(t, []string{"a:1", "a:2", "b:1", "b:2"}, projectedScanIDs(got))
	require.Equal(t, map[string]interface{}{"name": "x"}, got["a:1"])
	require.Equal(t, map[string]interface{}{"kind": "y"}, got["b:1"])
	require.Equal(t, map[string]interface{}{"name": "z"}, got["a:2"])
	require.Equal(t, map[string]interface{}{"kind": "w"}, got["b:2"])
}

// A pinned transaction reads the dictionary of the snapshot it started with: a
// DROP DATABASE of its namespace and a re-create that gives the token another
// name cannot rename the properties of the bodies the snapshot holds. This is
// the approved snapshot semantics TestPR1667CurrentReleaseSnapshotKeepsPropertiesAcrossDrop
// pins for the label-projected reader, asserted here for the generic projected
// scan of the same transaction.
func TestProjectedNodeDecoderSnapshotKeepsNamesAcrossNamespaceDrop(t *testing.T) {
	b := pr1667NewEngine(t)
	pr1667CreateTargetNode(t, b, "a:old", "original")
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("a"))

	pr1667DropPrefix(t, b, "a:")
	_, err = b.CreateNode(t.Context(), &Node{ID: "a:old", Labels: []string{pr1667RetirementLabel}, Properties: map[string]any{"different": "recreated"}})
	require.NoError(t, err)

	// The re-create took effect and gave the namespace a different property.
	require.Equal(t, map[string]interface{}{"different": "recreated"}, projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return b.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"different"}})["a:old"])

	got := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return tx.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"name", "different"}})
	require.Equal(t, []string{"a:old"}, projectedScanIDs(got))
	require.Equal(t, map[string]interface{}{"name": "original"}, got["a:old"],
		"a pinned snapshot must decode its body's token through the dictionary it started with")

	// A filter with no projection decodes every property through the same
	// snapshot, so the names it compares and returns are the snapshot's too.
	filtered := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return tx.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{PropertyFilter: func(props map[string]interface{}) bool { return props["name"] == "original" }})
	require.Equal(t, []string{"a:old"}, projectedScanIDs(filtered))
	require.Equal(t, map[string]interface{}{"name": "original"}, filtered["a:old"],
		"the filter and the row read the names of the snapshot's dictionary")
}

// A write stages a new property name in memory before the name is persisted,
// and marks it persisted only once the entity commit lands. The scanner bound
// to the writer's snapshot resolves that staged name for the writer, a
// concurrent read does not see the uncommitted node, and after the commit a
// fresh scan decodes the committed body to the same names.
func TestProjectedNodeDecoderStagedWriteKeepsNames(t *testing.T) {
	b := pr1667NewEngine(t)
	pr1667CreateTargetNode(t, b, "a:old", "original")
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("a"))
	_, err = tx.CreateNode(t.Context(), &Node{
		ID:         "a:new",
		Labels:     []string{pr1667RetirementLabel},
		Properties: map[string]any{"name": "second", "staged": "fresh"},
	})
	require.NoError(t, err)

	// The staged name is not persisted yet: this writer's own claim resolves it.
	staged, ok := b.propKeyDict.lookupID("a", "staged")
	require.True(t, ok)
	writer := newProjectedNodeDecoder(b, tx.badgerTx, StreamNodesOptions{Projection: []string{"name", "staged"}})
	writer.useNamespace([]byte("a:new"))
	resolved, err := writer.resolveToken(staged)
	require.NoError(t, err)
	require.Equal(t, "staged", resolved.name)
	require.True(t, resolved.wanted)

	// The transaction's scan sees both nodes' names; the uncommitted one comes
	// from the transaction's own pending state.
	inTxn := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return tx.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"name", "staged"}})
	require.Equal(t, []string{"a:new", "a:old"}, projectedScanIDs(inTxn))
	require.Equal(t, map[string]interface{}{"name": "original"}, inTxn["a:old"])
	require.Equal(t, map[string]interface{}{"name": "second", "staged": "fresh"}, inTxn["a:new"])

	// A concurrent scan does not borrow the staged name for a committed body.
	outside := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return b.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"name", "staged"}})
	require.Equal(t, []string{"a:old"}, projectedScanIDs(outside))
	require.Equal(t, map[string]interface{}{"name": "original"}, outside["a:old"])

	require.NoError(t, tx.Commit())
	committed := projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
		return b.StreamNodesWithOptions(context.Background(), opts, visit)
	}, StreamNodesOptions{Projection: []string{"name", "staged"}})
	require.Equal(t, []string{"a:new", "a:old"}, projectedScanIDs(committed))
	require.Equal(t, map[string]interface{}{"name": "second", "staged": "fresh"}, committed["a:new"])
}

// A write-behind buffer acknowledges a write whose property name is not yet
// persisted. The scan overlays the buffered node with its own names, and after
// the flush decodes the committed body to those same names, including the
// stored-bytes equality check.
func TestProjectedNodeDecoderWriteBehindBufferedNames(t *testing.T) {
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	_, err = b.CreateNode(t.Context(), &Node{ID: "a:1", Labels: []string{"P"}, Properties: map[string]any{"name": "committed"}})
	require.NoError(t, err)
	_, err = b.CreateNode(t.Context(), &Node{ID: "a:2", Labels: []string{"P"}, Properties: map[string]any{"fresh": "buffered"}})
	require.NoError(t, err)

	scan := func(opts StreamNodesOptions) map[NodeID]map[string]interface{} {
		return projectedScan(t, func(opts StreamNodesOptions, visit func(*Node) error) error {
			return b.StreamNodesWithOptions(context.Background(), opts, visit)
		}, opts)
	}
	buffered := scan(StreamNodesOptions{Projection: []string{"name", "fresh"}})
	require.Equal(t, []string{"a:1", "a:2"}, projectedScanIDs(buffered))
	require.Equal(t, map[string]interface{}{"name": "committed"}, buffered["a:1"])
	require.Equal(t, map[string]interface{}{"fresh": "buffered"}, buffered["a:2"])

	// A filter with no projection decodes every property: the write's node
	// arrives with its whole body and the other node is rejected by a property
	// the scan was not asked to project.
	whole := scan(StreamNodesOptions{PropertyFilter: func(props map[string]interface{}) bool {
		return props["fresh"] == "buffered"
	}})
	require.Equal(t, []string{"a:2"}, projectedScanIDs(whole))
	require.Equal(t, map[string]interface{}{"fresh": "buffered"}, whole["a:2"])
	require.Empty(t, scan(StreamNodesOptions{PropertyFilter: func(map[string]interface{}) bool { return false }}))

	require.NoError(t, b.FlushWriteBehind(context.Background()))
	committed := scan(StreamNodesOptions{Projection: []string{"name", "fresh"}})
	require.Equal(t, []string{"a:1", "a:2"}, projectedScanIDs(committed))
	require.Equal(t, map[string]interface{}{"name": "committed"}, committed["a:1"])
	require.Equal(t, map[string]interface{}{"fresh": "buffered"}, committed["a:2"])

	require.Equal(t, []string{"a:1", "a:2"}, projectedScanIDs(scan(StreamNodesOptions{
		Projection:           []string{"fresh"},
		PropertyStringEquals: map[string]string{"fresh": "buffered"},
	})), "the equality hint keeps the node without the property; the caller still tests it")

	// After the flush the same filter reads the committed body: the other node
	// is rejected before it is materialized.
	require.Equal(t, []string{"a:2"}, projectedScanIDs(scan(StreamNodesOptions{
		PropertyFilter: func(props map[string]interface{}) bool { return props["fresh"] == "buffered" },
	})))
	require.Equal(t, []string{"a:2"}, projectedScanIDs(scan(StreamNodesOptions{
		Projection:     []string{"fresh"},
		PropertyFilter: func(props map[string]interface{}) bool { return props["fresh"] == "buffered" },
	})), "the node without the property is rejected before it is materialized")
}

// A canceled statement ends the projected scan with the caller's context
// error, and a visitor's error ends it with that error.
func TestProjectedNodeDecoderCanceledScanAndVisitorError(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"a:1", "a:2", "a:3"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"name": string(id)}})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	scanned := 0
	err = engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Projection: []string{"name"}}, func(*Node) error {
		scanned++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, scanned, 3, "the scan stops at the canceled context")

	visitorErr := context.DeadlineExceeded
	err = engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{Projection: []string{"name"}}, func(*Node) error {
		return visitorErr
	})
	require.ErrorIs(t, err, visitorErr)

	// A filter without a projection stops the same way.
	ctx, cancel = context.WithCancel(context.Background())
	scanned = 0
	err = engine.StreamNodesWithOptions(ctx, StreamNodesOptions{PropertyFilter: func(map[string]interface{}) bool { return true }}, func(*Node) error {
		scanned++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, scanned, "the scan stops at the canceled context")
}
