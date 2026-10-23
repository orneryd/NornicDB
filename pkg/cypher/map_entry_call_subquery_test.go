package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMapEntrySyntaxWriteFirstCalls(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				query   string
				returns bool
			}{
				{"create_return", "CALL { CREATE (n:MapEntryReview {k: 1}) RETURN n.k AS k } RETURN k", true},
				{"merge_return", "CALL { MERGE (n:MapEntryReview {k: 1}) RETURN n.k AS k } RETURN k", true},
				{"create_unit", "CALL { CREATE (:MapEntryReview {k: 1}) }", false},
				{"merge_unit", "CALL { MERGE (:MapEntryReview {k: 1}) }", false},
				{"scope_return_control", "CALL () { CREATE (n:MapEntryReview {k: 1}) RETURN n.k AS k } RETURN k", true},
				{"scope_unit_control", "CALL () { CREATE (:MapEntryReview {k: 1}) }", false},
				{"nested_return", "CALL { CALL { CREATE (n:MapEntryReview {k: 1}) RETURN n.k AS k } RETURN k } RETURN k", true},
				{"foreach_unit", "CALL { FOREACH (k IN [1] | CREATE (:MapEntryReview {k: k})) }", false},
			} {
				t.Run(test.name, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_entry_call"))
					ctx := context.Background()
					if mode == "explicit" {
						if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := executor.Execute(ctx, test.query, nil)
					if err != nil {
						t.Fatal(err)
					}
					if test.returns && (len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(1)) {
						t.Fatalf("returning CALL rows=%#v, want [[1]]", result.Rows)
					}
					stored, err := executor.Execute(ctx, "MATCH (n:MapEntryReview) RETURN count(n) AS c", nil)
					if err != nil {
						t.Fatal(err)
					}
					if len(stored.Rows) != 1 || len(stored.Rows[0]) != 1 || stored.Rows[0][0] != int64(1) {
						t.Fatalf("stored rows=%#v, want one created node", stored.Rows)
					}
				})
			}
		})
	}
}

func TestMapEntrySyntaxValidatesInsideWriteFirstCalls(t *testing.T) {
	for _, query := range []string{
		"CALL { CREATE (n:MapEntryReview {1}) RETURN n } RETURN n",
		"CALL { MERGE (n:MapEntryReview {[]}) RETURN n } RETURN n",
		"CALL { CALL { CREATE (n:MapEntryReview {{}}) RETURN n } RETURN n } RETURN n",
	} {
		t.Run(query, func(t *testing.T) {
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			_, err := executor.Execute(context.Background(), query, nil)
			if err == nil {
				t.Fatal("expected malformed property map inside CALL to be rejected")
			}
			requireSemanticDetail(t, err, "UnexpectedSyntax")
		})
	}
}

func TestMapEntrySyntaxProjectionWithSubqueryName(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	result, err := executor.Execute(ctx, "WITH {} AS count RETURN count {key: 1} AS literal", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("projection rows=%#v", result.Rows)
	}
	value, ok := result.Rows[0][0].(map[string]interface{})
	if !ok || value["key"] != int64(1) {
		t.Fatalf("projection value=%#v, want {key: 1}", result.Rows[0][0])
	}
	_, err = executor.Execute(ctx, "WITH {} AS count RETURN count {bad entry} AS literal", nil)
	if err == nil {
		t.Fatal("expected malformed map projection to be rejected")
	}
	requireSemanticDetail(t, err, "UnexpectedSyntax")
}
