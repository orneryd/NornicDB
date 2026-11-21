package cypher

// Hunt ground H10: replay the input a fuzz worker was running when it died
// (the target's .last file on the lab dataset). Prints the input, then runs it
// once through the same path as FuzzHuntH10Execute, without recovering, so a
// crash prints its stack in the job log. Skipped unless HUNT_H10_REPLAY is set.

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestHuntH10ReplayLast(t *testing.T) {
	path := os.Getenv("HUNT_H10_REPLAY")
	if path == "" {
		t.Skip("HUNT_H10_REPLAY not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	query := string(data)
	t.Logf("HUNT_H10_REPLAY_INPUT len=%d hex=%x", len(data), data)
	t.Logf("HUNT_H10_REPLAY_INPUT quoted=%q", query)
	engine := storage.NewMemoryEngine()
	defer engine.Close()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	done := make(chan struct{})
	var execErr error
	start := time.Now()
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, execErr = executor.Execute(ctx, query, nil)
	}()
	select {
	case <-done:
		t.Logf("HUNT_H10_REPLAY_RESULT returned after %s, err=%v", time.Since(start), execErr)
	case <-time.After(60 * time.Second):
		t.Fatalf("HUNT_H10_REPLAY_RESULT no answer within 60s (2 s deadline)")
	}
}
