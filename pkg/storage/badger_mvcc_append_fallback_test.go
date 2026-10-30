package storage

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"
)

func TestBadgerMVCC_AppendAndFallbackVisibility(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	v1 := MVCCVersion{CommitTimestamp: time.Unix(100, 0).UTC(), CommitSequence: 1}
	v2 := MVCCVersion{CommitTimestamp: time.Unix(200, 0).UTC(), CommitSequence: 2}

	// Invalid-data guards.
	if err := engine.AppendNodeVersion(nil, v1); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("AppendNodeVersion(nil) expected ErrInvalidData, got: %v", err)
	}
	if err := engine.AppendEdgeVersion(nil, v1); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("AppendEdgeVersion(nil) expected ErrInvalidData, got: %v", err)
	}

	nodeID := NodeID("test:mvcc-only-node")
	node := &Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	}
	if err := engine.AppendNodeVersion(node, v1); err != nil {
		t.Fatalf("AppendNodeVersion failed: %v", err)
	}

	// No primary-key node exists, so this exercises the MVCC-head/version fallback path.
	latestNode, err := engine.GetNodeLatestVisible(nodeID)
	if err != nil {
		t.Fatalf("GetNodeLatestVisible failed: %v", err)
	}
	if latestNode == nil || latestNode.ID != nodeID {
		t.Fatalf("unexpected latest node: %#v", latestNode)
	}

	nodeAtV1, err := engine.GetNodeVisibleAt(nodeID, v1)
	if err != nil {
		t.Fatalf("GetNodeVisibleAt(v1) failed: %v", err)
	}
	if nodeAtV1 == nil || nodeAtV1.ID != nodeID {
		t.Fatalf("unexpected node at v1: %#v", nodeAtV1)
	}

	if err := engine.AppendNodeTombstone(nodeID, v2); err != nil {
		t.Fatalf("AppendNodeTombstone failed: %v", err)
	}
	if _, err := engine.GetNodeVisibleAt(nodeID, v2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound at tombstone version, got: %v", err)
	}

	edgeID := EdgeID("test:mvcc-only-edge")
	edge := &Edge{
		ID:         edgeID,
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"weight": int64(7)},
	}
	if err := engine.AppendEdgeVersion(edge, v1); err != nil {
		t.Fatalf("AppendEdgeVersion failed: %v", err)
	}

	latestEdge, err := engine.GetEdgeLatestVisible(edgeID)
	if err != nil {
		t.Fatalf("GetEdgeLatestVisible failed: %v", err)
	}
	if latestEdge == nil || latestEdge.ID != edgeID {
		t.Fatalf("unexpected latest edge: %#v", latestEdge)
	}

	if err := engine.AppendEdgeTombstone(edgeID, v2); err != nil {
		t.Fatalf("AppendEdgeTombstone failed: %v", err)
	}
	if _, err := engine.GetEdgeVisibleAt(edgeID, v2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for tombstoned edge at v2, got: %v", err)
	}
}

func TestBadgerMVCC_RebuildHeads_NilContext(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	v1 := MVCCVersion{CommitTimestamp: time.Unix(300, 0).UTC(), CommitSequence: 3}
	if err := engine.AppendNodeVersion(&Node{
		ID:         "test:rebuild-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "bob"},
	}, v1); err != nil {
		t.Fatalf("AppendNodeVersion failed: %v", err)
	}

	// Nil context should default to Background and still rebuild successfully.
	if err := engine.RebuildMVCCHeads(nil); err != nil {
		t.Fatalf("RebuildMVCCHeads(nil) failed: %v", err)
	}

	got, err := engine.GetNodeLatestVisible("test:rebuild-node")
	if err != nil {
		t.Fatalf("GetNodeLatestVisible after rebuild failed: %v", err)
	}
	if got == nil || got.ID != "test:rebuild-node" {
		t.Fatalf("unexpected node after rebuild: %#v", got)
	}
}
