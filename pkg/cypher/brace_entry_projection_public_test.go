package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Author: #1663 implementation lane. Source entry-projection allocation:
// orchestrator Nornic2. The public procedure sees the executor's carried view.
func TestBraceEntryProjectionThroughPublicExecution(t *testing.T) {
	for _, item := range []struct {
		name, query, key, value string
		droppedSeparator        bool
	}{
		{"trivia", "WITH {  target:  coalesce(1, 2)  } AS n CALL custom.brace_entries() YIELD checked RETURN checked", "target", "coalesce(1, 2)", false},
		{"nested_comprehension", "CYPHER 25 WITH {target: {k: v IN {a: {b: 1}} | k: {kk: vv IN v | kk: vv + 1}}} AS n CALL custom.brace_entries() YIELD checked RETURN checked", "target", "__", false},
		{"pattern_separator", "MATCH (a:BraceEntryProjection)-[:BRACE_ENTRY]->(b {k: a.k, target: 2}) CALL custom.brace_entries() YIELD checked RETURN checked", "target", "2", true},
		{"quoted_selector", "WITH 7 AS `x y`, {} AS n WITH n{`x y`} AS value CALL custom.brace_entries() YIELD checked RETURN checked", "`x y`", "nornicq", false},
	} {
		t.Run(item.name, func(t *testing.T) {
			ClearUserProcedures()
			t.Cleanup(ClearUserProcedures)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_entry_projection"))
			if _, err := executor.Execute(t.Context(), "CREATE (:BraceEntryProjection {k: 1})-[:BRACE_ENTRY]->(:BraceEntryProjection {k: 1, target: 2})", nil); err != nil {
				t.Fatal(err)
			}
			calls := 0
			err := RegisterUserProcedure(ProcedureSpec{Name: "custom.brace_entries", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "checked", Type: "INTEGER"}}}, func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				calls++
				view, carried := cyphertext.SourceView(ctx)
				if !carried {
					t.Fatal("public execution lost Source")
				}
				root := view.OriginalView()
				if root.Original != item.query {
					t.Fatalf("wrong statement identity: %s", root.Original)
				}
				found := false
				for _, record := range root.BraceFacts().Braces() {
					for _, entry := range record.Entries() {
						if entry.Name.End <= entry.Name.Start || root.Text[entry.Name.Start:entry.Name.End] != item.key {
							continue
						}
						found = true
						projected, err := view.ProjectEntry(entry)
						if err != nil || len(projected) != 1 {
							t.Fatalf("retained entry: %+v, %v", projected, err)
						}
						current := projected[0]
						if current.Kind != cyphertext.KeyedBraceEntry || !strings.Contains(view.Text[current.Value.Start:current.Value.End], item.value) {
							t.Fatalf("wrong emitted entry/value: %+v; %s", current, view.Text)
						}
						selected, ok, err := view.EntryAt(current.Name)
						if err != nil || !ok || selected.Span != current.Span {
							t.Fatalf("span API disagrees: %+v, %v", selected, err)
						}
						if item.droppedSeparator && len(view.ProjectFrom(root, entry.SeparatorSpans().Before)) != 0 {
							t.Fatal("removed original comma became an exact byte copy")
						}
					}
				}
				if !found {
					t.Fatal("original entry record missing")
				}
				return &ExecuteResult{Columns: []string{"checked"}, Rows: [][]interface{}{{int64(1)}}}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Execute(t.Context(), item.query, nil)
			if err != nil || calls != 1 || result == nil || len(result.Rows) != 1 {
				t.Fatalf("calls=%d result=%+v err=%v", calls, result, err)
			}
		})
	}
}
