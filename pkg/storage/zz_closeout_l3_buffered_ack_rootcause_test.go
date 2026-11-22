package storage

// Close-out lane L3: root-cause diagnostic for P-N15-145 (NOR-379, relationship
// cardinality) and P-N15-144 (NOR-378, relationship temporal) at buffered ACK.
// Test-side only.
//
// Proposed mechanism: a direct relationship create orders itself behind
// buffered commits only through holdsEntities with its endpoint nodes
// (createEdgeWithWriteContext, badger_edges.go:46; orderAfterBufferedCommits,
// buffered_write_order.go:307-318). holdsEntities consults the buffer's edge
// adjacency only for deleted or relabelled nodes (buffered_write_order.go:216-229),
// not for plain endpoints, so a pending relationship at the endpoint does not
// force a drain. The relationship constraint checks then read Badger only
// (checkEdgeCardinalityInTxn :686, checkEdgeTemporalInTxn :578, via
// validateEdgeConstraintsInTxn :405) and miss the pending relationship.
//
// The diagnostic logs, in the failing state (ack) and in the control state
// (synchronous): (1) the decisive branch value held = holdsEntities(endpoints);
// (2) what the buffer itself knows: the pending edge in the endpoint's
// adjacency overlay; (3) the owner's verdict when run directly on a Badger
// view. The bypass then applies a corrected ordering test-side (drain when an
// endpoint has buffered adjacency) and shows the probe gets the right answer.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func closeoutL3RCOverlay(t *testing.T, engine *BadgerEngine, node NodeID) int {
	t.Helper()
	w := engine.writeBehind
	require.NoError(t, w.mu.RLockContext(context.Background()))
	defer w.mu.RUnlock()
	n := 0
	for _, outgoing := range []bool{true, false} {
		live, touched, _, err := w.edgeAdjacencyOverlayLocked(context.Background(), node, outgoing)
		require.NoError(t, err)
		n += len(live) + len(touched)
	}
	return n
}

func closeoutL3RCOwner(t *testing.T, engine *BadgerEngine, probe *Edge) error {
	t.Helper()
	var ownerErr error
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		ownerErr = engine.validateEdgeConstraintsInTxn(context.Background(), txn, probe, engine.GetSchemaForNamespace("test"), "test", "")
		return nil
	}))
	return ownerErr
}

// closeoutL3RCBypass is the corrected ordering applied test-side: drain when an
// endpoint of the direct relationship write has buffered adjacency.
func closeoutL3RCBypass(t *testing.T, engine *BadgerEngine, endpoints ...NodeID) bool {
	t.Helper()
	for _, n := range endpoints {
		if closeoutL3RCOverlay(t, engine, n) > 0 {
			require.NoError(t, engine.FlushWriteBehind(context.Background()))
			return true
		}
	}
	return false
}

func TestCloseoutL3_RC_N15_145_CardinalityMechanism(t *testing.T) {
	for _, point := range []string{"synchronous", "ack"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			engine := closeoutL3Engine(t)
			for _, id := range []NodeID{"test:hub", "test:a"} {
				_, err := engine.CreateNode(ctx, &Node{ID: id})
				require.NoError(t, err, "PRECONDITION: node")
			}
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "card_one", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "T", Direction: "OUTGOING", MaxCount: 1}))
			closeoutL3Commit(t, engine, point, func(tx *BadgerTransaction) error {
				return tx.CreateEdge(ctx, &Edge{ID: "test:holder", StartNode: "test:hub", EndNode: "test:a", Type: "T"})
			})
			probe := &Edge{ID: "test:probe", StartNode: "test:hub", EndNode: "test:a", Type: "T"}
			held, err := engine.writeBehind.holdsEntities(ctx, writtenEntities{nodes: []NodeID{"test:hub", "test:a"}})
			require.NoError(t, err)
			overlay := closeoutL3RCOverlay(t, engine, "test:hub")
			ownerErr := closeoutL3RCOwner(t, engine, probe)
			t.Logf("CLOSEOUT_L3_RC card point=%s pending=%d holdsEntities(endpoints)=%v hubOverlayEdges=%d ownerOnBadger=%v", point, engine.writeBehind.PendingOps(), held, overlay, ownerErr)
			if point == "synchronous" {
				var v *ConstraintViolationError
				require.True(t, errors.As(ownerErr, &v), "control: on a committed relationship the owner must reject the probe")
				return
			}
			require.False(t, held, "MECHANISM: holdsEntities(endpoints) must be false (no drain) for the mechanism to hold")
			require.Positive(t, overlay, "MECHANISM: the buffer must hold the pending relationship in the hub's adjacency")
			require.NoError(t, ownerErr, "MECHANISM: the owner reading Badger alone must miss the pending relationship")
			drained := closeoutL3RCBypass(t, engine, "test:hub", "test:a")
			probeErr := engine.CreateEdge(ctx, probe)
			t.Logf("CLOSEOUT_L3_RC card bypass drained=%v probeErr=%v", drained, probeErr)
			var v *ConstraintViolationError
			require.True(t, drained, "BYPASS: the corrected ordering must drain")
			require.True(t, errors.As(probeErr, &v), "BYPASS: with the corrected ordering the probe must be rejected (got %v)", probeErr)
			require.Equal(t, ConstraintCardinality, v.Type)
		})
	}
}

func TestCloseoutL3_RC_N15_144_TemporalMechanism(t *testing.T) {
	for _, point := range []string{"synchronous", "ack"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			engine := closeoutL3Engine(t)
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "history", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"key", "from", "to"}}))
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(ctx, &Node{ID: id})
				require.NoError(t, err, "PRECONDITION: node")
			}
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			edge := func(id EdgeID, key string, offset time.Duration) *Edge {
				return &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]any{"key": key, "from": start.Add(offset), "to": start.Add(offset + time.Hour)}}
			}
			require.NoError(t, engine.CreateEdge(ctx, edge("test:holder", "before", 0)))
			closeoutL3Commit(t, engine, point, func(tx *BadgerTransaction) error {
				return tx.UpdateEdge(ctx, EdgeUpdateRequest{After: edge("test:holder", "changed", 2*time.Hour)})
			})
			probe := edge("test:overlap", "changed", 150*time.Minute)
			held, err := engine.writeBehind.holdsEntities(ctx, writtenEntities{nodes: []NodeID{"test:a", "test:b"}})
			require.NoError(t, err)
			overlay := closeoutL3RCOverlay(t, engine, "test:a")
			ownerErr := closeoutL3RCOwner(t, engine, probe)
			t.Logf("CLOSEOUT_L3_RC temporal point=%s pending=%d holdsEntities(endpoints)=%v aOverlayEdges=%d ownerOnBadger=%v", point, engine.writeBehind.PendingOps(), held, overlay, ownerErr)
			if point == "synchronous" {
				var v *ConstraintViolationError
				require.True(t, errors.As(ownerErr, &v), "control: on a committed update the owner must reject the overlap")
				return
			}
			require.False(t, held, "MECHANISM: holdsEntities(endpoints) must be false (no drain)")
			require.Positive(t, overlay, "MECHANISM: the buffer must hold the pending update in a's adjacency")
			require.NoError(t, ownerErr, "MECHANISM: the owner reading Badger alone must miss the pending interval")
			drained := closeoutL3RCBypass(t, engine, "test:a", "test:b")
			probeErr := engine.CreateEdge(ctx, probe)
			t.Logf("CLOSEOUT_L3_RC temporal bypass drained=%v probeErr=%v", drained, probeErr)
			var v *ConstraintViolationError
			require.True(t, drained, "BYPASS: the corrected ordering must drain")
			require.True(t, errors.As(probeErr, &v), "BYPASS: with the corrected ordering the overlap must be rejected (got %v)", probeErr)
			require.Equal(t, ConstraintTemporal, v.Type)
		})
	}
}
