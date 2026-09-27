package fabric

import (
	"fmt"
	"strings"
	"testing"
)

func TestFabricTransaction_NewTransaction(t *testing.T) {
	tx := NewFabricTransaction("tx-001")
	if tx.TxID() != "tx-001" {
		t.Errorf("expected tx-001, got %s", tx.TxID())
	}
	if tx.State() != "open" {
		t.Errorf("expected open, got %s", tx.State())
	}
	if tx.WriteShard() != "" {
		t.Errorf("expected empty write shard, got %s", tx.WriteShard())
	}
}

func TestFabricTransaction_ReadMultipleShards(t *testing.T) {
	tx := NewFabricTransaction("tx-002")

	// Read from multiple shards — should succeed.
	sub1, err := tx.GetOrOpen("shard_a", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub1.ShardName != "shard_a" {
		t.Errorf("expected shard_a, got %s", sub1.ShardName)
	}
	if sub1.IsWrite {
		t.Error("expected IsWrite=false")
	}

	sub2, err := tx.GetOrOpen("shard_b", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub2.ShardName != "shard_b" {
		t.Errorf("expected shard_b, got %s", sub2.ShardName)
	}

	// Both shards should be participants.
	participants := tx.Participants()
	if len(participants) != 2 {
		t.Errorf("expected 2 participants, got %d", len(participants))
	}
}

func TestFabricTransaction_WriteOneShard(t *testing.T) {
	tx := NewFabricTransaction("tx-003")

	// Write to one shard — should succeed.
	sub, err := tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sub.IsWrite {
		t.Error("expected IsWrite=true")
	}
	if tx.WriteShard() != "shard_a" {
		t.Errorf("expected shard_a as write shard, got %s", tx.WriteShard())
	}
}

func TestFabricTransaction_WriteToSameShardTwice(t *testing.T) {
	tx := NewFabricTransaction("tx-004")

	// Write to the same shard twice — should succeed.
	_, err := tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("unexpected error for same shard write: %v", err)
	}
}

func TestFabricTransaction_SecondWriteShardRejected(t *testing.T) {
	tx := NewFabricTransaction("tx-005")

	// Write to first shard — succeeds.
	_, err := tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Write to second shard — must be rejected with deterministic error.
	_, err = tx.GetOrOpen("shard_b", true)
	if err == nil {
		t.Fatal("expected error for second write shard")
	}
	if err != ErrSecondWriteShard {
		t.Errorf("expected ErrSecondWriteShard, got: %v", err)
	}
	// Verify the error message matches Neo4j's contract.
	if !strings.Contains(err.Error(), "Neo.ClientError.Statement.AccessMode") {
		t.Errorf("expected Neo4j-compatible error code, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Writing to more than one database") {
		t.Errorf("expected descriptive message, got: %v", err)
	}
}

func TestFabricTransaction_ReadThenWriteUpgrade(t *testing.T) {
	tx := NewFabricTransaction("tx-006")

	// Read from shard first.
	_, err := tx.GetOrOpen("shard_a", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Upgrade to write on same shard — should succeed.
	sub, err := tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("unexpected error on upgrade: %v", err)
	}
	if !sub.IsWrite {
		t.Error("expected IsWrite=true after upgrade")
	}
	if tx.WriteShard() != "shard_a" {
		t.Errorf("expected shard_a as write shard after upgrade, got %s", tx.WriteShard())
	}
}

func TestFabricTransaction_ReadThenWriteDifferentShard(t *testing.T) {
	tx := NewFabricTransaction("tx-007")

	// Read from shard_a.
	_, err := tx.GetOrOpen("shard_a", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Write to shard_b — should succeed (first write shard).
	_, err = tx.GetOrOpen("shard_b", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Now try to write to shard_a — must fail.
	_, err = tx.GetOrOpen("shard_a", true)
	if err != ErrSecondWriteShard {
		t.Errorf("expected ErrSecondWriteShard, got: %v", err)
	}
}

func TestFabricTransaction_CommitAll(t *testing.T) {
	tx := NewFabricTransaction("tx-008")

	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", true)

	commitCount := 0
	err := tx.Commit(
		func(sub *SubTransaction) error {
			commitCount++
			return nil
		},
		func(sub *SubTransaction) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if commitCount != 2 {
		t.Errorf("expected 2 commits, got %d", commitCount)
	}
	if tx.State() != "committed" {
		t.Errorf("expected committed, got %s", tx.State())
	}
}

func TestFabricTransaction_CommitAfterCommit(t *testing.T) {
	tx := NewFabricTransaction("tx-009")
	_ = tx.Commit(func(sub *SubTransaction) error { return nil }, nil)

	err := tx.Commit(func(sub *SubTransaction) error { return nil }, nil)
	if err == nil {
		t.Fatal("expected error for double commit")
	}
}

func TestFabricTransaction_CommitUsesBoundParticipantCallbacks(t *testing.T) {
	tx := NewFabricTransaction("tx-009b")
	_, err := tx.GetOrOpen("shard_a", true)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	committed := false
	rolledBack := false
	if err := tx.BindParticipantCallbacks(
		"shard_a",
		func(_ *SubTransaction) error {
			committed = true
			return nil
		},
		func(_ *SubTransaction) error {
			rolledBack = true
			return nil
		},
	); err != nil {
		t.Fatalf("BindParticipantCallbacks failed: %v", err)
	}

	if err := tx.Commit(nil, nil); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if !committed {
		t.Fatal("expected bound commit callback to run")
	}
	if rolledBack {
		t.Fatal("did not expect rollback callback during successful commit")
	}
}

func TestFabricTransaction_RollbackAll(t *testing.T) {
	tx := NewFabricTransaction("tx-010")

	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", true)

	rollbackCount := 0
	err := tx.Rollback(func(sub *SubTransaction) error {
		rollbackCount++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rollbackCount != 2 {
		t.Errorf("expected 2 rollbacks, got %d", rollbackCount)
	}
	if tx.State() != "rolledback" {
		t.Errorf("expected rolledback, got %s", tx.State())
	}
}

func TestFabricTransaction_RollbackAfterRollback(t *testing.T) {
	tx := NewFabricTransaction("tx-011")
	_ = tx.Rollback(nil)

	err := tx.Rollback(nil)
	if err == nil {
		t.Fatal("expected error for double rollback")
	}
}

func TestFabricTransaction_CommitFailureRollsBackRemaining(t *testing.T) {
	tx := NewFabricTransaction("tx-012")

	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", false)

	commitCount := 0
	rollbackCount := 0

	err := tx.Commit(
		func(sub *SubTransaction) error {
			commitCount++
			if commitCount == 2 {
				return fmt.Errorf("simulated commit failure on %s", sub.ShardName)
			}
			return nil
		},
		func(sub *SubTransaction) error {
			rollbackCount++
			return nil
		},
	)
	if err == nil {
		t.Fatal("expected error for partial commit failure")
	}
	if !strings.Contains(err.Error(), "commit failed on shard") {
		t.Errorf("expected commit failure message, got: %v", err)
	}
	if tx.State() != "rolledback" {
		t.Errorf("expected rolledback after partial commit failure, got %s", tx.State())
	}
}

func TestFabricTransaction_CommitFailureCompensatesCommittedShards(t *testing.T) {
	tx := NewFabricTransaction("tx-013")

	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", false)

	committed := make([]string, 0, 2)
	rolledBack := make([]string, 0, 2)
	err := tx.Commit(
		func(sub *SubTransaction) error {
			committed = append(committed, sub.ShardName)
			if sub.ShardName == "shard_b" {
				return fmt.Errorf("commit failed")
			}
			return nil
		},
		func(sub *SubTransaction) error {
			rolledBack = append(rolledBack, sub.ShardName)
			return nil
		},
	)
	if err == nil {
		t.Fatal("expected commit error")
	}
	if tx.State() != "rolledback" {
		t.Fatalf("expected rolledback state, got %s", tx.State())
	}

	if len(rolledBack) != 2 {
		t.Fatalf("expected compensation rollback on all participants, got %v", rolledBack)
	}

	state := tx.SubTransactions()
	for name, sub := range state {
		if sub.State != "rolledback" {
			t.Fatalf("expected subtransaction %s to be rolledback, got %s", name, sub.State)
		}
	}
}

func TestFabricTransaction_SubTransactions(t *testing.T) {
	tx := NewFabricTransaction("tx-014")

	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", true)

	subs := tx.SubTransactions()
	if len(subs) != 2 {
		t.Fatalf("expected 2 sub-transactions, got %d", len(subs))
	}

	subA, ok := subs["shard_a"]
	if !ok {
		t.Fatal("expected shard_a in sub-transactions")
	}
	if subA.IsWrite {
		t.Error("expected shard_a IsWrite=false")
	}
	if subA.State != "open" {
		t.Errorf("expected open, got %s", subA.State)
	}

	subB, ok := subs["shard_b"]
	if !ok {
		t.Fatal("expected shard_b in sub-transactions")
	}
	if !subB.IsWrite {
		t.Error("expected shard_b IsWrite=true")
	}
}

func TestFabricTransaction_GetOrOpenOnClosedTx(t *testing.T) {
	tx := NewFabricTransaction("tx-015")
	_ = tx.Commit(func(sub *SubTransaction) error { return nil }, nil)

	_, err := tx.GetOrOpen("shard_a", false)
	if err == nil {
		t.Fatal("expected error for GetOrOpen on committed tx")
	}
}

func TestFabricTransaction_CommitWithBoundRollbackOnFailure(t *testing.T) {
	tx := NewFabricTransaction("tx-016")
	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", false)

	boundRolledBack := false
	_ = tx.BindParticipantCallbacks("shard_b",
		func(_ *SubTransaction) error { return fmt.Errorf("shard_b commit fail") },
		func(_ *SubTransaction) error {
			boundRolledBack = true
			return nil
		},
	)

	err := tx.Commit(nil, nil) // nil global callbacks → uses bound.
	if err == nil {
		t.Fatal("expected commit error")
	}
	if tx.State() != "rolledback" {
		t.Fatalf("expected rolledback, got %s", tx.State())
	}
	// Bound rollback should have run.
	if !boundRolledBack {
		t.Fatal("expected bound rollback callback to run on commit failure")
	}
}

func TestFabricTransaction_RollbackOnClosedTx(t *testing.T) {
	tx := NewFabricTransaction("tx-017")
	_ = tx.Commit(nil, nil) // Committed (no subs).

	err := tx.Rollback(nil)
	if err == nil {
		t.Fatal("expected error for rollback on committed tx")
	}
}

func TestFabricTransaction_ReadThenWriteUpgrade_SecondShardFail(t *testing.T) {
	tx := NewFabricTransaction("tx-018")
	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", true) // shard_b is write shard.

	// Now upgrade shard_a to write → should fail.
	_, err := tx.GetOrOpen("shard_a", true)
	if err != ErrSecondWriteShard {
		t.Fatalf("expected ErrSecondWriteShard, got: %v", err)
	}
}

func TestFabricTransaction_Commit_SkipsAlreadyCommitted(t *testing.T) {
	tx := NewFabricTransaction("tx-019")
	sub, _ := tx.GetOrOpen("shard_a", false)
	sub.State = "committed" // Pre-mark as committed.

	commitCount := 0
	err := tx.Commit(func(_ *SubTransaction) error {
		commitCount++
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if commitCount != 0 {
		t.Fatalf("expected 0 commits (sub already committed), got %d", commitCount)
	}
}

func TestFabricTransaction_Rollback_SkipsAlreadyRolledBack(t *testing.T) {
	tx := NewFabricTransaction("tx-020")
	sub, _ := tx.GetOrOpen("shard_a", false)
	sub.State = "rolledback" // Pre-mark.

	rollbackCount := 0
	err := tx.Rollback(func(_ *SubTransaction) error {
		rollbackCount++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rollbackCount != 0 {
		t.Fatalf("expected 0 rollbacks, got %d", rollbackCount)
	}
}

func TestFabricTransaction_Commit_NilGlobalUseBoundRollbackOnFail(t *testing.T) {
	tx := NewFabricTransaction("tx-021")
	_, _ = tx.GetOrOpen("shard_a", false)

	_ = tx.BindParticipantCallbacks("shard_a",
		func(_ *SubTransaction) error { return nil },
		nil,
	)

	// shard_a commits successfully with bound callback, then rolls back
	// with nil rollback → no panic.
	err := tx.Commit(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
