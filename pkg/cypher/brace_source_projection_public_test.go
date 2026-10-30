package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Author: #1663 implementation lane. Public source-projection contract: Nornic2.
// This file uses pre-existing APIs so the same test overlay runs on release.
func TestBraceSourceProjectionThroughLabelRewrite(t *testing.T) {
	for _, query := range []string{
		"MATCH (a)-->(b {k: coalesce(a.k, {x: 1})}) RETURN b",
		"MATCH (a)-->(b {keep: {x: 1}, k: coalesce(a.k, {x: 1})}) RETURN b",
		"MATCH (a)-->(b WHERE b.k = coalesce(a.k, {x: 1})) RETURN b",
	} {
		t.Run(query, func(t *testing.T) {
			ctx, _ := cyphertext.WithSource(t.Context(), query)
			text, rewrite, err := desugarLabelExpressionView(retainedBraceView(ctx, query), nil, false)
			if err != nil || rewrite == nil {
				t.Fatalf("rewrite missing: %v", err)
			}
			ctx = withSourceRewrite(ctx, rewrite)
			view := cyphertext.SourceFor(ctx, text)
			// The final nested map is the moved expression's map, even if an
			// earlier map has exactly the same bytes. Value lookup cannot prove origin.
			original := strings.LastIndex(query, "{x: 1}")
			copied := strings.LastIndex(text, "{x: 1}")
			if original < 0 || copied < 0 {
				t.Fatalf("nested map lost: %s", text)
			}
			for _, extent := range []cyphertext.Span{{Start: 0, End: 1}, {Start: 1, End: 2}, {Start: 0, End: 6}} {
				got, valid := view.OriginalSpan(cyphertext.Span{Start: copied + extent.Start, End: copied + extent.End}, cyphertext.SourceExtent)
				want := cyphertext.Span{Start: original + extent.Start, End: original + extent.End}
				if !valid || got != want {
					t.Errorf("copy %v -> %v, want %v; %s", extent, got, want, text)
				}
			}
		})
	}
}

func TestBraceSourceProjectionThroughExecute(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_source_projection"))
	if _, err := executor.Execute(t.Context(), "CREATE (a:BraceProjection {k: 1})-[:BRACE_PROJECTION]->(b:BraceProjection {k: 1})", nil); err != nil {
		t.Fatal(err)
	}
	query := "MATCH (a:BraceProjection)-[:BRACE_PROJECTION]->(b {k: coalesce(a.k, {x: 1})}) CALL custom.brace_source() YIELD value RETURN value"
	text, _, err := desugarLabelExpressions(query, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = RegisterUserProcedure(ProcedureSpec{Name: "custom.brace_source", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
		func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
			calls++
			view := cyphertext.SourceFor(ctx, text)
			if view.Original != query {
				t.Errorf("public execution lost original source: %s", view.Original)
			}
			original, copied := strings.Index(query, "{x: 1}"), strings.Index(text, "{x: 1}")
			if got, valid := view.OriginalSpan(cyphertext.Span{Start: copied + 1, End: copied + 2}, cyphertext.SourceExtent); !valid || got != (cyphertext.Span{Start: original + 1, End: original + 2}) {
				t.Errorf("public execution copy: %v", got)
			}
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), query, nil)
	if err != nil || result == nil || len(result.Rows) != 1 || calls != 1 {
		t.Fatalf("calls=%d result=%+v err=%v", calls, result, err)
	}
}
