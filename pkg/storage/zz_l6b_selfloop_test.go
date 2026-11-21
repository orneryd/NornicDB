package storage

// Lane L6b (closeout): S04 L-H1-004, a buffered self-loop is missing from
// incoming adjacency reads.
//  1. indexEdgeAdjacency (commit_buffer.go:373-388) on a self-loop and on a
//     plain edge; then a test-side corrected copy whose seen key carries the
//     direction.
//  2. Engine level, write-behind on: incoming reads of the self-loop before the
//     generation drains, after 2 s without a drain, and after FlushWriteBehind;
//     and with a short drain interval (50 ms) read at once and after 2 s, to
//     decide whether later misses come from an undrained generation or from a
//     cached adjacency filled while the overlay lacked the loop.

import (
	"testing"
	"time"
)

type zzL6bDirKey struct {
	node     NodeID
	id       EdgeID
	incoming bool
}

func zzL6bIndexCorrected(e *Edge) (out, in map[NodeID][]EdgeID) {
	seen := map[zzL6bDirKey]bool{}
	out, in = map[NodeID][]EdgeID{}, map[NodeID][]EdgeID{}
	if k := (zzL6bDirKey{e.StartNode, e.ID, false}); !seen[k] {
		seen[k] = true
		out[e.StartNode] = append(out[e.StartNode], e.ID)
	}
	if k := (zzL6bDirKey{e.EndNode, e.ID, true}); !seen[k] {
		seen[k] = true
		in[e.EndNode] = append(in[e.EndNode], e.ID)
	}
	return out, in
}

func zzL6bIDs(edges []*Edge) []EdgeID {
	var ids []EdgeID
	for _, e := range edges {
		ids = append(ids, e.ID)
	}
	return ids
}

func TestL6bSelfLoopIncomingBuffered(t *testing.T) {
	for _, e := range []*Edge{
		{ID: "loop", StartNode: "a", EndNode: "a", Type: "LOOP"},
		{ID: "plain", StartNode: "b", EndNode: "a", Type: "R"},
	} {
		buf := newCommitBuffer()
		buf.indexEdgeAdjacency(e)
		cOut, cIn := zzL6bIndexCorrected(e)
		t.Logf("L6B_S04 indexEdgeAdjacency edge=%s start=%s end=%s -> outgoingByNode[%s]=%v incomingByNode[%s]=%v | corrected(direction in seen key) out=%v in=%v",
			e.ID, e.StartNode, e.EndNode, e.StartNode, buf.outgoingByNode[e.StartNode], e.EndNode, buf.incomingByNode[e.EndNode], cOut[e.StartNode], cIn[e.EndNode])
		if e.ID == "loop" && len(buf.incomingByNode["a"]) == 0 && len(cIn["a"]) == 1 {
			t.Errorf("CAUSE S04: the outgoing entry marks the shared seen key {a, loop}, so the incoming entry of the self-loop is skipped (commit_buffer.go:381-386)")
		}
	}

	read := func(t *testing.T, engine *BadgerEngine, dir AdjacentEdgeDirection) ([]EdgeID, error) {
		edges, err := engine.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: dir, Node: "test:a", Headers: true})
		return zzL6bIDs(edges), err
	}
	t.Run("manual-drain", func(t *testing.T) {
		engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"A"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.CreateEdge(t.Context(), &Edge{ID: "test:loop", StartNode: "test:a", EndNode: "test:a", Type: "LOOP"}); err != nil {
			t.Fatal(err)
		}
		in0, e0 := read(t, engine, AdjacentIncoming)
		out0, _ := read(t, engine, AdjacentOutgoing)
		time.Sleep(2 * time.Second)
		in2, e2 := read(t, engine, AdjacentIncoming)
		flushErr := engine.FlushWriteBehind(t.Context())
		in3, e3 := read(t, engine, AdjacentIncoming)
		t.Logf("L6B_S04 manual-drain incoming at-once=%v err=%v outgoing=%v | after 2s undrained incoming=%v err=%v | FlushWriteBehind err=%v -> incoming=%v err=%v", in0, e0, out0, in2, e2, flushErr, in3, e3)
	})
	t.Run("interval-50ms", func(t *testing.T) {
		engine := newWriteBehindTestEngine(t, 50*time.Millisecond)
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"A"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.CreateEdge(t.Context(), &Edge{ID: "test:loop", StartNode: "test:a", EndNode: "test:a", Type: "LOOP"}); err != nil {
			t.Fatal(err)
		}
		in0, e0 := read(t, engine, AdjacentIncoming)
		time.Sleep(2 * time.Second)
		in2, e2 := read(t, engine, AdjacentIncoming)
		engine.adjCacheInvalidate()
		in3, e3 := read(t, engine, AdjacentIncoming)
		t.Logf("L6B_S04 interval-50ms incoming at-once=%v err=%v | after 2s (drained by interval) incoming=%v err=%v | after adjCacheInvalidate incoming=%v err=%v", in0, e0, in2, e2, in3, e3)
	})
}
