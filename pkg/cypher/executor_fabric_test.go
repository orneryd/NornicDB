package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestStripLeadingWithImportsForFabricRecord_MatchIsStripped(t *testing.T) {
	query := "WITH textKey128 MATCH (tt) WHERE tt.textKey128 = textKey128 RETURN collect(tt) AS texts"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"textKey128": "h1"})
	want := "MATCH (tt) WHERE tt.textKey128 = $textKey128 RETURN collect(tt) AS texts"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestStripLeadingWithImportsForFabricRecord_UnwindPipelineIsStripped(t *testing.T) {
	query := "WITH rows UNWIND rows AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"rows": []interface{}{}})
	// Generated keyword-spelled names use the shared quoted renderer.
	want := "UNWIND $`rows` AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestStripLeadingWithImportsForFabricRecord_UseClauseIsStripped(t *testing.T) {
	query := "WITH textKey128 USE translations.txr MATCH (tt) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"textKey128": "abc"})
	want := "USE translations.txr MATCH (tt) WHERE tt.translationId = $textKey128 RETURN collect(tt) AS texts"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestFindLeadingWithEndLocalAndSplitCommaTopLevelLocal(t *testing.T) {
	query := "WITH a, collect({k: b, t: c}) AS rows USE translations.txr MATCH (n) RETURN n"
	idx := findLeadingWithEndLocal(query)
	if idx <= 0 {
		t.Fatalf("expected positive with-end index, got %d", idx)
	}
	parts := splitTopLevelComma("a, collect({k: b, t: c}) AS rows, `quoted`")
	if len(parts) != 3 {
		t.Fatalf("expected 3 top-level parts, got %#v", parts)
	}
}

func TestCypherFabricExecutor_ExecuteQuery_DelegatesAndReturnsRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(baseStore)

	cf := &cypherFabricExecutor{
		base:       exec,
		authToken:  "",
		autoCommit: true,
	}

	cols, rows, err := cf.ExecuteQuery(context.Background(), "nornic", baseStore, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("unexpected ExecuteQuery error: %v", err)
	}
	if len(cols) != 1 || cols[0] != "one" {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecute_TrailingSemicolonCompatibility(t *testing.T) {
	base := newTestMemoryEngine(t)
	mgr, err := multidb.NewDatabaseManager(base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	if err := mgr.CreateDatabase("leaf_a"); err != nil {
		t.Fatalf("CreateDatabase leaf_a failed: %v", err)
	}
	if err := mgr.CreateDatabase("leaf_b"); err != nil {
		t.Fatalf("CreateDatabase leaf_b failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "leaf_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "leaf_b", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	t.Run("plain return with delimiter", func(t *testing.T) {
		res, err := exec.Execute(ctx, "RETURN 1 AS one;", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != int64(1) {
			t.Fatalf("unexpected rows: %#v", res.Rows)
		}
	})

	t.Run("leading use with delimiter", func(t *testing.T) {
		res, err := exec.Execute(ctx, "USE leaf_a RETURN 7 AS seven;", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != int64(7) {
			t.Fatalf("unexpected rows: %#v", res.Rows)
		}
	})

	t.Run("fabric call-use query with trailing delimiter", func(t *testing.T) {
		_, err := exec.Execute(ctx, "USE cmp CALL { USE cmp.a RETURN 1 AS v } RETURN v;", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestShouldUseFabricPlanner_OnlyForCompositeUseTargets(t *testing.T) {
	inner := newTestMemoryEngine(t)
	mgr, err := multidb.NewDatabaseManager(inner, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	if err := mgr.CreateDatabase("leaf_a"); err != nil {
		t.Fatalf("CreateDatabase leaf_a failed: %v", err)
	}
	if err := mgr.CreateDatabase("leaf_b"); err != nil {
		t.Fatalf("CreateDatabase leaf_b failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "leaf_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "leaf_b", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "payload contains word use does not trigger fabric",
			query: "UNWIND [{content:'USE cmp.a MATCH (n) RETURN n'}] AS row CREATE (n:Doc) SET n = row RETURN count(n) AS c",
			want:  false,
		},
		{
			name:  "non-composite use does not trigger fabric",
			query: "USE leaf_a MATCH (n) RETURN n",
			want:  false,
		},
		{
			name:  "composite root use triggers fabric",
			query: "USE cmp CALL { USE cmp.a MATCH (n) RETURN count(n) AS c } RETURN c",
			want:  true,
		},
		{
			name:  "composite constituent use triggers fabric",
			query: "CALL { USE cmp.a MATCH (n) RETURN n } RETURN 1 AS ok",
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := exec.shouldUseFabricPlanner(tc.query)
			if got != tc.want {
				t.Fatalf("shouldUseFabricPlanner(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}
