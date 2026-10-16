package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
	"time"
)

func TestReleaseRevealAfterExplicitTransactionLabelScan(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	schema := engine.GetSchemaForNamespace("testns")
	if err := schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "episode_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "bind_episode",
		ProfileRef:   "episode_decay",
		TargetLabels: []string{"MemoryEpisode"},
	}); err != nil {
		t.Fatal(err)
	}
	engine.SetDecayEnabled(true)
	createdAt := time.Now().Add(-720 * time.Hour)
	if _, err := engine.CreateNode(&storage.Node{
		ID: "testns:old_episode", Labels: []string{"MemoryEpisode"},
		Properties: map[string]interface{}{"name": "old"},
		CreatedAt:  createdAt, UpdatedAt: createdAt,
	}); err != nil {
		t.Fatal(err)
	}

	executor := NewStorageExecutorWithQueryCachePolicy(engine, 0, time.Minute)
	filtered, err := executor.Execute(context.Background(), "MATCH (n:MemoryEpisode) RETURN n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Rows) != 0 {
		t.Fatalf("ordinary MATCH returned %d decayed nodes", len(filtered.Rows))
	}
	revealed, err := executor.Execute(context.Background(), "MATCH (n:MemoryEpisode) RETURN reveal(n)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(revealed.Rows) != 1 {
		t.Fatalf("reveal(n) returned %d rows, want one", len(revealed.Rows))
	}
	node, ok := revealed.Rows[0][0].(*storage.Node)
	if !ok || node.ID != "testns:old_episode" {
		t.Fatalf("revealed value = %#v, want node testns:old_episode", revealed.Rows[0][0])
	}

	if _, err := executor.Execute(context.Background(), "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	txFiltered, err := executor.Execute(context.Background(), "MATCH (n:MemoryEpisode) RETURN n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(txFiltered.Rows) != 0 {
		t.Fatalf("ordinary MATCH in explicit transaction returned %d decayed nodes", len(txFiltered.Rows))
	}
	txRevealed, err := executor.Execute(context.Background(), "MATCH (n:MemoryEpisode) RETURN reveal(n)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(txRevealed.Rows) != 1 {
		t.Fatalf("reveal(n) in explicit transaction returned %d rows, want one", len(txRevealed.Rows))
	}
	for _, query := range []string{"MATCH (n:MemoryEpisode) RETURN n", "MATCH (n:MemoryEpisode) RETURN reveal(n)", "MATCH (n:MemoryEpisode) RETURN n", "MATCH (n:MemoryEpisode) RETURN reveal(n)", "MATCH (n:MemoryEpisode) RETURN n.name", "MATCH (n:MemoryEpisode) RETURN reveal(n).name", "MATCH (n:MemoryEpisode) RETURN n.name", "MATCH (n:MemoryEpisode) RETURN reveal(n) LIMIT 1", "MATCH (n:MemoryEpisode) RETURN n LIMIT 1"} {
		result, err := executor.Execute(context.Background(), query, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if hasRevealCall(query) {
			want = 1
		}
		if len(result.Rows) != want {
			t.Fatalf("alternating query %q returned %d rows, want %d", query, len(result.Rows), want)
		}
	}
	if _, err := executor.txContext.storageWrapper.CreateNode(&storage.Node{ID: "testns:pending", Labels: []string{"MemoryEpisode"}, Properties: map[string]interface{}{"name": "pending"}, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		want  int64
	}{{"MATCH (n:MemoryEpisode) RETURN count(n)", 1}, {"MATCH (n:MemoryEpisode) RETURN count(reveal(n))", 2}, {"MATCH (n:MemoryEpisode) RETURN count(n)", 1}} {
		result, err := executor.Execute(context.Background(), tc.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != tc.want {
			t.Fatalf("pending-write query %q result %#v, want %d", tc.query, result, tc.want)
		}
	}
	if _, err := executor.Execute(context.Background(), "COMMIT", nil); err != nil {
		t.Fatal(err)
	}
}
