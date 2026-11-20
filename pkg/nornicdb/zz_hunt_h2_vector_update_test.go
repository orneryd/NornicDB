package nornicdb

// Hunt ground H2 (v1.5.2 open hunt, "around the fixes"), variant of
// 63d210a21 "Preserve exact vectors with anchored WAL property updates"
// (units of pkg/storage/managed_vector_reference.go, wal_entity_commit.go and
// the accepted entity commit path it changed). The fix's own assertions
// (property_update_journal_bytes_test.go
// TestPropertyOnlyCypherUpdateJournalsUnchangedVectorBytes: after a
// property-only Cypher update the managed chunk vectors and the named vector
// are exact, also after a close and reopen; the journal record is bounded by
// propertyUpdateJournalPublicBound) applied to the sibling statement forms
// the regression test does not run: SET +=, SET of the unchanged text value,
// two properties, MERGE ON MATCH, UNWIND, CALL subquery, FOREACH, label add,
// add-then-remove, and a relationship created from the node. Prints
// "HUNT_LEAD <json>" per disagreement and one "HUNT_COVERAGE <json>".
// Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func h2VectorLead(lead map[string]any) {
	lead["ground"] = "H2"
	data, err := json.Marshal(lead)
	if err != nil {
		data = []byte(fmt.Sprintf("%q", fmt.Sprint(lead)))
	}
	fmt.Fprintf(os.Stdout, "HUNT_LEAD %s\n", data)
}

// h2VectorsExact reports the first difference between node's vectors and the
// fixture's (chunk i seeded from index i+1, named title from index 0).
func h2VectorsExact(node *storage.Node, chunks int) string {
	if len(node.ChunkEmbeddings) != chunks {
		return fmt.Sprintf("chunk vectors: %d, want %d", len(node.ChunkEmbeddings), chunks)
	}
	for i, vector := range node.ChunkEmbeddings {
		want := propertyUpdateJournalPublicVector(i + 1)
		if len(vector) != len(want) {
			return fmt.Sprintf("chunk %d has %d dimensions, want %d", i, len(vector), len(want))
		}
		for j := range vector {
			if vector[j] != want[j] {
				return fmt.Sprintf("chunk %d coordinate %d is %v, want %v", i, j, vector[j], want[j])
			}
		}
	}
	title := node.NamedEmbeddings["title"]
	want := propertyUpdateJournalPublicVector(0)
	if len(title) != len(want) {
		return fmt.Sprintf("named title has %d dimensions, want %d", len(title), len(want))
	}
	for j := range title {
		if title[j] != want[j] {
			return fmt.Sprintf("named title coordinate %d is %v, want %v", j, title[j], want[j])
		}
	}
	return ""
}

func TestHuntH2PropertyUpdateVectorSiblings(t *testing.T) {
	const chunks = 64
	cases, leads := 0, 0
	statements := []struct {
		name       string
		statements []string
		// property that must hold after the statements (nil: not checked)
		property string
		want     any
	}{
		{name: "control-set-flag", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n.flag = true"}, property: "flag", want: true},
		{name: "set-map-merge", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n += {flag: true}"}, property: "flag", want: true},
		{name: "set-unchanged-text", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n.text = 'alpha', n.flag = true"}, property: "flag", want: true},
		{name: "set-two-properties", statements: []string{"MATCH (n:Doc) WHERE n.text = 'alpha' SET n.flag = true, n.k = 1"}, property: "k", want: int64(1)},
		{name: "merge-on-match", statements: []string{"MERGE (n:Doc {text: 'alpha'}) ON MATCH SET n.flag = true"}, property: "flag", want: true},
		{name: "unwind-set", statements: []string{"UNWIND [1, 2] AS i MATCH (n:Doc {text: 'alpha'}) SET n.k = i"}, property: "k", want: int64(2)},
		{name: "call-subquery-set", statements: []string{"MATCH (n:Doc {text: 'alpha'}) CALL (n) { SET n.flag = true }"}, property: "flag", want: true},
		{name: "foreach-set", statements: []string{"MATCH (n:Doc {text: 'alpha'}) FOREACH (x IN [1] | SET n.flag = true)"}, property: "flag", want: true},
		{name: "add-label", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n:Extra"}},
		{name: "add-then-remove-property", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n.tmp = 1", "MATCH (n:Doc {text: 'alpha'}) REMOVE n.tmp"}, property: "tmp", want: nil},
		{name: "create-relationship-from-node", statements: []string{"MATCH (n:Doc {text: 'alpha'}) CREATE (n)-[:LINKS]->(:Other {k: 1})"}},
		{name: "set-then-set-again", statements: []string{"MATCH (n:Doc {text: 'alpha'}) SET n.k = 1", "MATCH (n:Doc {text: 'alpha'}) SET n.k = 2"}, property: "k", want: int64(2)},
	}
	for _, tc := range statements {
		t.Run(tc.name, func(t *testing.T) {
			cases++
			db, engine, dir, config, id := propertyUpdateJournalPublicFixture(t, chunks, true)
			ctx := context.Background()
			walDir := filepath.Join(dir, "wal")
			before, err := storage.ReadWALEntriesFromDir(ctx, walDir)
			if err != nil {
				t.Fatalf("read journal: %v", err)
			}
			lead := map[string]any{"harness": "vector-update", "unit": "63d210a21", "case": tc.name, "statements": tc.statements,
				"fixture": fmt.Sprintf("nornicdb.Open; node :Doc {text:'alpha'} with named vector title (%d dims) and %d managed chunk vectors", propertyUpdateJournalPublicDims, chunks)}
			for _, statement := range tc.statements {
				if _, err := db.ExecuteCypher(ctx, statement, nil); err != nil {
					lead["kind"], lead["error"], lead["statement"] = "error", err.Error(), statement
					leads++
					h2VectorLead(lead)
					return
				}
			}
			var problems []string
			stored, err := engine.GetNode(ctx, id)
			if err != nil {
				problems = append(problems, "read after update: "+err.Error())
			} else {
				if diff := h2VectorsExact(stored, chunks); diff != "" {
					problems = append(problems, "after update: "+diff)
				}
				if tc.property != "" && fmt.Sprint(stored.Properties[tc.property]) != fmt.Sprint(tc.want) {
					problems = append(problems, fmt.Sprintf("after update: property %s = %v, want %v", tc.property, stored.Properties[tc.property], tc.want))
				}
			}
			if entries, err := storage.ReadWALEntriesFromDir(ctx, walDir); err == nil {
				largest := 0
				for _, entry := range entries[len(before):] {
					if entry.Operation == storage.OpEntityCommit && len(entry.Data) > largest {
						largest = len(entry.Data)
					}
				}
				lead["largest_record_bytes"] = largest
				if largest > propertyUpdateJournalPublicBound {
					lead["cost"] = fmt.Sprintf("largest entity-commit record %d bytes > bound %d", largest, propertyUpdateJournalPublicBound)
				}
			}
			if err := db.Close(); err != nil {
				problems = append(problems, "close: "+err.Error())
			}
			reopened, err := Open(dir, config)
			if err != nil {
				problems = append(problems, "reopen: "+err.Error())
			} else {
				t.Cleanup(func() { _ = reopened.Close() })
				recovered, err := unwrapToBadgerEngine(reopened.GetBaseStorageForManager()).GetNode(ctx, id)
				if err != nil {
					problems = append(problems, "read after reopen: "+err.Error())
				} else {
					if diff := h2VectorsExact(recovered, chunks); diff != "" {
						problems = append(problems, "after reopen: "+diff)
					}
					if tc.property != "" && fmt.Sprint(recovered.Properties[tc.property]) != fmt.Sprint(tc.want) {
						problems = append(problems, fmt.Sprintf("after reopen: property %s = %v, want %v", tc.property, recovered.Properties[tc.property], tc.want))
					}
				}
			}
			if len(problems) > 0 {
				lead["kind"], lead["problems"] = "wrong-result", problems
				leads++
				h2VectorLead(lead)
			} else if lead["cost"] != nil {
				lead["kind"] = "cost"
				leads++
				h2VectorLead(lead)
			}
		})
	}
	data, _ := json.Marshal(map[string]any{"ground": "H2", "harness": "vector-update", "commit": "63d210a21", "cases": cases, "leads": leads})
	fmt.Fprintf(os.Stdout, "HUNT_COVERAGE %s\n", data)
}
