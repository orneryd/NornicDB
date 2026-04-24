package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupDisabledDecayEngine(t *testing.T) *StorageExecutor {
	t.Helper()
	eng, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return NewStorageExecutor(eng)
}

func TestDecayScore_DisabledReturns1(t *testing.T) {
	exec := setupDisabledDecayEngine(t)

	node := makeNode("nornic:old_episode", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n)", nodes, rels)
	score, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if score != 1.0 {
		t.Errorf("expected 1.0 when decay disabled, got %.6f", score)
	}
}

func TestDecay_DisabledAppliesFalse(t *testing.T) {
	exec := setupDisabledDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecay("decay(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	applies, _ := m["applies"].(bool)
	if applies {
		t.Error("expected applies=false when decay disabled")
	}

	reason, _ := m["reason"].(string)
	if reason == "" {
		t.Error("expected non-empty reason when decay disabled")
	}
}

func TestPolicy_DisabledEmptyMeta(t *testing.T) {
	exec := setupDisabledDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalPolicy("policy(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	tid, _ := m["targetId"].(string)
	if tid != "nornic:ep" {
		t.Errorf("expected targetId, got %q", tid)
	}
}

func TestDecayMismatchLoggedOnce(t *testing.T) {
	exec := setupDisabledDecayEngine(t)
	exec.decayMismatchLogged = false

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	exec.evalDecayScore("decayScore(n)", nodes, rels)
	if !exec.decayMismatchLogged {
		t.Error("expected decayMismatchLogged=true after first call")
	}

	exec.decayMismatchLogged = false
	exec.evalDecayScore("decayScore(n)", nodes, rels)
	if !exec.decayMismatchLogged {
		t.Error("flag should be set again after reset")
	}
}
