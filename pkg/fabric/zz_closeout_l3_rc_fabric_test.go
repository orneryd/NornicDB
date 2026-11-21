// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Root-cause proof for P-NGEN-27 (NOR-373).
package fabric

import "testing"

// Owner: FabricTransaction.GetOrOpen, transaction.go:151 (`return sub, nil`)
// returns the coordinator's own *SubTransaction stored in t.subTxns (:131-137),
// not a copy (SubTransactions, :174-184, copies).
// (a) Diagnostic: the returned pointer is identical to the coordinator's map entry.
// (b) Bypass: a test-side GetOrOpen that hands back a copy (as SubTransactions
// does); the same caller mutation no longer changes commit dispatch.
func TestCloseoutL3_RC_NGEN27_GetOrOpenReturnsCoordinatorEntry(t *testing.T) {
	tx := NewFabricTransaction("closeout-l3-rc")
	writer, err := tx.GetOrOpen("nornic.writer", true)
	if err != nil {
		t.Fatal(err)
	}
	tx.mu.Lock()
	same := tx.subTxns["nornic.writer"] == writer
	tx.mu.Unlock()
	t.Logf("CLOSEOUT_L3_RC NGEN27 returnedIsCoordinatorEntry=%v", same)

	copyingGetOrOpen := func(tx *FabricTransaction, shard string, isWrite bool) (*SubTransaction, error) {
		sub, err := tx.GetOrOpen(shard, isWrite)
		if err != nil {
			return nil, err
		}
		view := *sub
		return &view, nil
	}
	bypassTx := NewFabricTransaction("closeout-l3-rc-bypass")
	reader, err := copyingGetOrOpen(bypassTx, "nornic.reader", false)
	if err != nil {
		t.Fatal(err)
	}
	bypassWriter, err := copyingGetOrOpen(bypassTx, "nornic.writer", true)
	if err != nil {
		t.Fatal(err)
	}
	reader.State = "committed"
	bypassWriter.IsWrite = false
	bypassWriter.ShardName = "nornic.spoofed"
	committed, err := closeoutL3CommitShards(t, bypassTx)
	t.Logf("CLOSEOUT_L3_RC NGEN27 bypass(copy returned) committed=%v err=%v", committed, err)

	if err != nil || len(committed) != 2 || committed[0] != "nornic.reader" || committed[1] != "nornic.writer" {
		t.Fatalf("BYPASS: with a copied view the coordinator must still commit both participants, got %v err=%v", committed, err)
	}
	if !same {
		t.Fatalf("MECHANISM: GetOrOpen returned a view distinct from the coordinator entry")
	}
}
