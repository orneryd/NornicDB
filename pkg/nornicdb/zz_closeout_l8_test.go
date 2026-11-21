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
