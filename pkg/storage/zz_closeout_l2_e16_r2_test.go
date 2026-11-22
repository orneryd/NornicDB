//go:build closeout_l2_e16_r2

package storage

import (
	"context"
	"errors"
	"testing"
)

// E16 Reproduction 2 (storage API): an older transaction that holds the start
// node's write lock lists a relationship a peer committed after its snapshot
// (MatchEdgesBetween) but cannot read it by ID (GetEdge returns not found).
// Ported to the pin's API: BeginTransaction takes (ctx, OrdinaryBegin), every
// read takes a context, and MatchEdgesBetween takes a read policy; the entry's
// call had none, so the default SnapshotEdgeMatch is used (the locked-node
// authority is what switches the listing to the current view).

const (
	l2E16R2X    = NodeID("test:x")
	l2E16R2Y    = NodeID("test:y")
	l2E16R2Edge = EdgeID("test:r")
)

type l2E16R2Outcome struct {
	listed   int
	pointErr error
}

// l2E16R2Run performs steps 1-6; lock selects step 4 and policy the listing's
// read policy.
func l2E16R2Run(t *testing.T, lock bool, policy EdgeMatchReadPolicy, edgeBeforeBegin bool) l2E16R2Outcome {
	t.Helper()
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	for _, id := range []NodeID{l2E16R2X, l2E16R2Y} {
		if _, err := engine.CreateNode(ctx, &Node{ID: id}); err != nil {
			t.Fatalf("CreateNode %s: %v", id, err)
		}
	}
	edge := &Edge{ID: l2E16R2Edge, StartNode: l2E16R2X, EndNode: l2E16R2Y, Type: "LINK"}
	if edgeBeforeBegin {
		if err := engine.CreateEdge(ctx, edge); err != nil {
			t.Fatalf("CreateEdge: %v", err)
		}
	}
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("SetNamespace: %v", err)
	}
	if _, err := tx.GetNode(ctx, l2E16R2X); err != nil {
		t.Fatalf("tx.GetNode: %v", err)
	}
	if !edgeBeforeBegin {
		if err := engine.CreateEdge(ctx, edge); err != nil {
			t.Fatalf("CreateEdge: %v", err)
		}
	}
	if lock {
		if _, err := tx.LockNodeForWrite(ctx, l2E16R2X); err != nil {
			t.Fatalf("LockNodeForWrite: %v", err)
		}
	}
	listed, err := tx.MatchEdgesBetween(ctx, l2E16R2X, l2E16R2Y, "LINK", []string{}, func(*Edge) bool { return true }, policy)
	if err != nil {
		t.Fatalf("MatchEdgesBetween: %v", err)
	}
	_, pointErr := tx.GetEdge(ctx, l2E16R2Edge)
	return l2E16R2Outcome{listed: len(listed), pointErr: pointErr}
}

func l2E16R2Agree(o l2E16R2Outcome) bool {
	if o.listed > 0 {
		return o.pointErr == nil
	}
	return errors.Is(o.pointErr, ErrNotFound)
}

func TestL2_e16_r2_Repro(t *testing.T) {
	o := l2E16R2Run(t, true, SnapshotEdgeMatch, false)
	verdict := "OK"
	if !l2E16R2Agree(o) {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E16-R2 %s locked=true policy=snapshot listed=%d getEdgeErr=%v", verdict, o.listed, o.pointErr)
	if verdict == "DEFECT" {
		t.Errorf("MatchEdgesBetween listed %d relationship(s) but GetEdge(%s) returned %v: listing and point read disagree", o.listed, l2E16R2Edge, o.pointErr)
	}
}

// Control: the relationship committed before the transaction began; listing
// and point read must both find it.
func TestL2_e16_r2_Control(t *testing.T) {
	o := l2E16R2Run(t, true, SnapshotEdgeMatch, true)
	t.Logf("control listed=%d getEdgeErr=%v", o.listed, o.pointErr)
	if o.listed != 1 || o.pointErr != nil {
		t.Errorf("control: listed=%d getEdgeErr=%v, want 1 and nil", o.listed, o.pointErr)
	}
}

// Cause: the listing's authority switch. adjacencyReadAuthorityLocked
// (entity_write_lock_adjacency.go:6) makes a write-locked node read the
// current adjacency, and MatchEdgesBetween then records the returned
// relationships (recordLatestEndpointsLocked) so GetEdge can resolve them.
// The matrix logs lock x policy so the branch that disagrees is visible.
func TestL2_e16_r2_Cause(t *testing.T) {
	for _, c := range []struct {
		name   string
		lock   bool
		policy EdgeMatchReadPolicy
	}{
		{"unlocked/snapshot", false, SnapshotEdgeMatch},
		{"locked/snapshot", true, SnapshotEdgeMatch},
		{"unlocked/current", false, CurrentEdgeMatch},
		{"locked/current", true, CurrentEdgeMatch},
	} {
		o := l2E16R2Run(t, c.lock, c.policy, false)
		t.Logf("cause %s listed=%d getEdgeErr=%v agree=%t", c.name, o.listed, o.pointErr, l2E16R2Agree(o))
	}
}
