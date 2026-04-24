package cypher

import (
	"testing"

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
		{"MATCH (n:Foo) RETURN n", false},
		{"MATCH (n:Foo) RETURN n.revealed", false},
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

	cleanup := setRevealOnEngine(eng)

	// Verify cleanup is callable and doesn't panic.
	cleanup()
}

func TestSetRevealOnEngine_NilEngine(t *testing.T) {
	cleanup := setRevealOnEngine(nil)
	cleanup() // should not panic
}
