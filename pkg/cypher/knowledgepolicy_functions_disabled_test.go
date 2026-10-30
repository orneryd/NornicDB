package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestDecayMismatchLoggedOnce(t *testing.T) {
	_, exec := setupDecayEngine(t, false)
	exec.decayMismatchLogged = false

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}
	ctx := context.Background()

	exec.evaluateKnowledgePolicyFunction(ctx, "decayScore(n)", "", nodes, rels)
	if !exec.decayMismatchLogged {
		t.Error("expected decayMismatchLogged=true after first call")
	}

	exec.decayMismatchLogged = false
	exec.evaluateKnowledgePolicyFunction(ctx, "decayScore(n)", "", nodes, rels)
	if !exec.decayMismatchLogged {
		t.Error("flag should be set again after reset")
	}
}
