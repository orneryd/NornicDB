package nornicdb

// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): hunt lead
// L-H2-012. The fix 63d210a21's bound (propertyUpdateJournalPublicBound) on
// the journal record of a property-only update, for the lead's UNWIND form
// and for variants that separate "UNWIND" from "two writes to the same node
// in one statement". Each case logs every new journal record's operation and
// size. Test-only, not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL8_H2_012_UnwindPropertyUpdateJournal(t *testing.T) {
	const chunks = 64
	for _, tc := range []struct {
		name, statement string
		lead            bool
	}{
		{"lead-unwind-two", "UNWIND [1, 2] AS i MATCH (n:Doc {text: 'alpha'}) SET n.k = i", true},
		{"control-set-one", "MATCH (n:Doc {text: 'alpha'}) SET n.k = 1", false},
		{"unwind-one", "UNWIND [1] AS i MATCH (n:Doc {text: 'alpha'}) SET n.k = i", false},
		{"two-sets-one-clause", "MATCH (n:Doc {text: 'alpha'}) SET n.k = 1, n.k = 2", false},
		{"two-set-clauses", "MATCH (n:Doc {text: 'alpha'}) SET n.k = 1 SET n.j = 2", false},
		{"foreach-two", "MATCH (n:Doc {text: 'alpha'}) FOREACH (x IN [1, 2] | SET n.k = x)", false},
		{"with-unwind-two", "MATCH (n:Doc {text: 'alpha'}) WITH n UNWIND [1, 2] AS i SET n.k = i", false},
		{"unwind-two-other-property", "UNWIND [1, 2] AS i MATCH (n:Doc {text: 'alpha'}) SET n.k = 7", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _, dir, _, _ := propertyUpdateJournalPublicFixture(t, chunks, true)
			ctx := context.Background()
			walDir := filepath.Join(dir, "wal")
			before, err := storage.ReadWALEntriesFromDir(ctx, walDir)
			if err != nil {
				t.Fatalf("read journal: %v", err)
			}
			if _, err := db.ExecuteCypher(ctx, tc.statement, nil); err != nil {
				t.Fatalf("%s: %v", tc.statement, err)
			}
			entries, err := storage.ReadWALEntriesFromDir(ctx, walDir)
			if err != nil {
				t.Fatalf("read journal: %v", err)
			}
			records := []string{}
			largest := 0
			for _, entry := range entries[len(before):] {
				records = append(records, fmt.Sprintf("%v:%d", entry.Operation, len(entry.Data)))
				if entry.Operation == storage.OpEntityCommit && len(entry.Data) > largest {
					largest = len(entry.Data)
				}
			}
			data, _ := json.Marshal(map[string]any{"lead": "L-H2-012", "case": tc.name, "statement": tc.statement, "records": records, "largest_entity_commit": largest, "bound": propertyUpdateJournalPublicBound})
			t.Logf("L8_JOURNAL %s", data)
			if largest > propertyUpdateJournalPublicBound {
				t.Errorf("%s: largest entity-commit record %d bytes > bound %d", tc.statement, largest, propertyUpdateJournalPublicBound)
			}
			_ = db.Close()
		})
	}
}

// Owner run: the same property-only update through one storage transaction,
// once and twice for the same node. The second update of a node finds it in
// the transaction's pending nodes (badger_transaction.go:1081), which leaves
// the operation not anchor-eligible (:1080/:1103), so managedReferences
// (managed_vector_reference.go:133) does not reference the unchanged vectors.
func TestL8Owner_H2_012_TransactionSecondUpdate(t *testing.T) {
	for _, updates := range []int{1, 2} {
		t.Run(fmt.Sprintf("updates-%d", updates), func(t *testing.T) {
			db, engine, dir, _, id := propertyUpdateJournalPublicFixture(t, 64, true)
			ctx := context.Background()
			walDir := filepath.Join(dir, "wal")
			before, err := storage.ReadWALEntriesFromDir(ctx, walDir)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := engine.BeginTransaction(ctx, storage.OrdinaryBegin)
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= updates; i++ {
				current, err := tx.GetNode(ctx, id)
				if err != nil {
					t.Fatalf("read in tx: %v", err)
				}
				next := storage.CopyNode(current)
				next.Properties["k"] = int64(i)
				if err := tx.UpdateGraphNode(ctx, storage.NodeUpdateRequest{Before: current, After: next}); err != nil {
					t.Fatalf("update %d: %v", i, err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			entries, _ := storage.ReadWALEntriesFromDir(ctx, walDir)
			records := []string{}
			largest := 0
			for _, entry := range entries[len(before):] {
				records = append(records, fmt.Sprintf("%v:%d", entry.Operation, len(entry.Data)))
				if entry.Operation == storage.OpEntityCommit && len(entry.Data) > largest {
					largest = len(entry.Data)
				}
			}
			data, _ := json.Marshal(map[string]any{"lead": "L-H2-012", "step": "storage transaction updates of one node", "updates": updates, "records": records, "largest_entity_commit": largest})
			t.Logf("L8_OWNER %s", data)
			_ = db.Close()
		})
	}
}
