package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestHasRevealCall(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"MATCH (n:Foo) RETURN reveal(n)", true},
		{"MATCH (n:Foo) RETURN REVEAL(n)", true},
		{"MATCH (n:Foo) RETURN Reveal(n)", true},
		{"MATCH (n:Foo) RETURN REVEAL (n)", true},
		{"MATCH (n:Foo) RETURN reveal /* gap */ (n)", true},
		{"MATCH (n:Foo) RETURN n", false},
		{"MATCH (n:Foo) RETURN n.revealed", false},
		{"MATCH (n:Foo) RETURN 'reveal(n)'", false},
		{"MATCH (n:Foo) RETURN \"reveal(n)\"", false},
		{"MATCH (n:Foo) RETURN `reveal(n)`", false},
		{"MATCH (n:Foo) // reveal(n)\r RETURN n", false},
		{"MATCH (n:Foo) /* reveal(n) */ RETURN n", false},
		{"MATCH (n:Foo) WITH reveal AS alias RETURN alias, reveal(n)", true},
		{"", false},
	}
	for _, tt := range tests {
		if got := hasRevealCall(tt.query); got != tt.want {
			t.Errorf("hasRevealCall(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestRevealExpressionBypassesDecayDuringQuery(t *testing.T) {
	for _, mode := range []struct {
		name   string
		policy []QueryCachePolicy
	}{
		{"default cache", nil},
		{"disabled cache explicit transaction scans", []QueryCachePolicy{{MaxEntries: 0, TTL: time.Minute}}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			policy := mode.policy
			engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			if engine != nil {
				fixtureowner.Own(t, engine)
			}
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

			executor := NewStorageExecutor(engine, policy...)
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
			if len(policy) != 0 {
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
			}
			if _, err := executor.Execute(context.Background(), "COMMIT", nil); err != nil {
				t.Fatal(err)
			}

		})
	}
}

func TestUnwrapBadgerEngine_Direct(t *testing.T) {
	eng, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if eng != nil {
		fixtureowner.Own(t, eng)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	got := unwrapBadgerEngine(eng)
	if got != eng {
		t.Error("expected to unwrap to same BadgerEngine")
	}
}

func TestUnwrapBadgerEngine_NilInput(t *testing.T) {
	got := unwrapBadgerEngine(nil)
	if got != nil {
		t.Error("expected nil for nil engine")
	}
}

func TestSetRevealOnEngine_BadgerEngine(t *testing.T) {
	eng, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if eng != nil {
		fixtureowner.Own(t, eng)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_, cleanup, _ := setRevealOnEngine(context.Background(), eng, true)

	// Verify cleanup is callable and doesn't panic.
	cleanup()
}

func TestSetRevealOnEngine_NilEngine(t *testing.T) {
	_, cleanup, _ := setRevealOnEngine(context.Background(), nil, false)
	cleanup() // should not panic
}

func TestSetRevealOnEngine_BlocksConcurrentNonRevealScope(t *testing.T) {
	eng, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if eng != nil {
		fixtureowner.Own(t, eng)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	ctx, cleanup, _ := setRevealOnEngine(context.Background(), eng, true)

	done := make(chan struct{})
	go func() {
		_, nestedCleanup, _ := setRevealOnEngine(context.Background(), eng, false)
		nestedCleanup()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("non-reveal scope should wait while reveal scope is active")
	case <-time.After(20 * time.Millisecond):
	}

	cleanup()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("non-reveal scope did not resume after reveal scope cleanup")
	}

	_, nestedCleanup, _ := setRevealOnEngine(ctx, eng, false)
	nestedCleanup()
}

func TestSetRevealOnEngine_ExecutionStateBlocksConcurrentRead(t *testing.T) {
	eng, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if eng != nil {
		fixtureowner.Own(t, eng)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	revealCtx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
	revealCtx, releaseReveal, revealEngine := setRevealOnEngine(revealCtx, eng, true)
	if revealEngine != eng {
		t.Fatal("expected reveal scope to be tracked in execution state")
	}
	revealActive := true
	defer func() {
		if revealActive {
			clearRevealScope(revealCtx, revealEngine)
			releaseReveal()
		}
	}()

	readDone := make(chan struct{})
	go func() {
		readCtx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
		readCtx, releaseRead, readEngine := setRevealOnEngine(readCtx, eng, false)
		clearRevealScope(readCtx, readEngine)
		releaseRead()
		close(readDone)
	}()

	select {
	case <-readDone:
		t.Fatal("ordinary read scope must wait for the active reveal scope")
	case <-time.After(20 * time.Millisecond):
	}

	clearRevealScope(revealCtx, revealEngine)
	releaseReveal()
	revealActive = false
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("ordinary read scope did not resume after reveal scope ended")
	}
}
