// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Item P-NGEN-27: GetOrOpen returns the coordinator's own *SubTransaction, so an
// embedded caller that mutates the returned view changes coordinator authority.
package fabric

import (
	"context"
	"sort"
	"testing"
)

func closeoutL3CommitShards(t *testing.T, tx *FabricTransaction) ([]string, error) {
	t.Helper()
	var committed []string
	ctx := context.Background()
	err := tx.Commit(TerminalContexts{Commit: ctx, Cleanup: ctx},
		func(_ context.Context, sub *SubTransaction) error {
			committed = append(committed, sub.ShardName)
			return nil
		},
		func(context.Context, *SubTransaction) error { return nil })
	sort.Strings(committed)
	return committed, err
}

func TestCloseoutL3_NGEN27_ReturnedViewDoesNotChangeCoordinator(t *testing.T) {
	// Control: no mutation; both participants are committed by the coordinator.
	control := NewFabricTransaction("closeout-l3-control")
	if _, err := control.GetOrOpen("nornic.reader", false); err != nil {
		t.Fatal(err)
	}
	if _, err := control.GetOrOpen("nornic.writer", true); err != nil {
		t.Fatal(err)
	}
	committed, err := closeoutL3CommitShards(t, control)
	t.Logf("DECISIVE control committed=%v err=%v", committed, err)
	if err != nil || len(committed) != 2 {
		t.Fatalf("control: expected both participants committed, got %v err=%v", committed, err)
	}

	tx := NewFabricTransaction("closeout-l3-mutated")
	reader, err := tx.GetOrOpen("nornic.reader", false)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := tx.GetOrOpen("nornic.writer", true)
	if err != nil {
		t.Fatal(err)
	}
	// Embedded caller edits the values it was handed.
	reader.State = "committed"
	writer.IsWrite = false
	writer.ShardName = "nornic.spoofed"

	snapshot := tx.SubTransactions()
	t.Logf("DECISIVE after mutation coordinator reader.State=%q writer.IsWrite=%v writer.ShardName=%q writeShard=%q",
		snapshot["nornic.reader"].State, snapshot["nornic.writer"].IsWrite, snapshot["nornic.writer"].ShardName, tx.WriteShard())
	committed, err = closeoutL3CommitShards(t, tx)
	t.Logf("DECISIVE mutated committed=%v err=%v", committed, err)

	if snapshot["nornic.reader"].State != "open" || !snapshot["nornic.writer"].IsWrite || snapshot["nornic.writer"].ShardName != "nornic.writer" {
		t.Errorf("NOR-373: mutating the view returned by GetOrOpen changed the coordinator's participant state")
	}
	if len(committed) != 2 || committed[0] != "nornic.reader" {
		t.Errorf("NOR-373: coordinator commit dispatch followed the caller-mutated view: committed=%v (want [nornic.reader nornic.writer])", committed)
	}
}
