package cypher

import (
	"context"
	"testing"
	"time"

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
		{"MATCH (n:Foo) RETURN n", false},
		{"MATCH (n:Foo) RETURN n.revealed", false},
		{"MATCH (n:Foo) WITH reveal AS alias RETURN alias, reveal(n)", true},
		{"", false},
	}
	for _, tt := range tests {
		if got := hasRevealCall(tt.query); got != tt.want {
			t.Errorf("hasRevealCall(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestUnwrapBadgerEngine_Direct(t *testing.T) {
	eng, err := storage.NewBadgerEngineInMemory()
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
	eng, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_, cleanup := setRevealOnEngine(context.Background(), eng, true)

	// Verify cleanup is callable and doesn't panic.
	cleanup()
}

func TestSetRevealOnEngine_NilEngine(t *testing.T) {
	_, cleanup := setRevealOnEngine(context.Background(), nil, false)
	cleanup() // should not panic
}

func TestSetRevealOnEngine_BlocksConcurrentNonRevealScope(t *testing.T) {
	eng, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	ctx, cleanup := setRevealOnEngine(context.Background(), eng, true)

	done := make(chan struct{})
	go func() {
		_, nestedCleanup := setRevealOnEngine(context.Background(), eng, false)
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

	_, nestedCleanup := setRevealOnEngine(ctx, eng, false)
	nestedCleanup()
}
