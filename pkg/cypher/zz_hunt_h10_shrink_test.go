package cypher

// Hunt ground H10: hand shrink of the Execute worker crash (replay input
// "0000 ) {Q\x87\x04\xd2:A)", panic in labelExpressionRewriter.opensSubquery).
// Each candidate runs through the FuzzHuntH10Execute path on an empty
// in-memory database; a panic is recovered here only to report it per input.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestHuntH10ShrinkExecuteCrash(t *testing.T) {
	candidates := []string{
		"0000 ) {Q\x87\x04\xd2:A)",
		"0000 ) {}",
		") {}",
		"RETURN 1) {}",
		"RETURN 1 AS x) {}",
		"MATCH (n)) {}",
		"MATCH (n)) RETURN n {.name}",
		"MATCH (n)) RETURN n",
		"CALL db.labels()) {}",
		"MATCH (n) WHERE n.x = 1) {}",
		"MATCH (n:Person)) SET n += {name: 'a'}",
		"CREATE (n:Person)) {name: 'a'}",
		"RETURN (1)) {}",
		"MATCH (n) CALL (n)) { RETURN 1 AS y } RETURN y",
	}
	for _, query := range candidates {
		engine := storage.NewMemoryEngine()
		executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
		outcome := make(chan string, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					outcome <- fmt.Sprintf("PANIC %v", r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := executor.Execute(ctx, query, nil)
			outcome <- fmt.Sprintf("returned err=%v", err)
		}()
		select {
		case got := <-outcome:
			t.Logf("HUNT_H10_SHRINK input=%q -> %s", query, got)
		case <-time.After(30 * time.Second):
			t.Logf("HUNT_H10_SHRINK input=%q -> no answer within 30s", query)
		}
		engine.Close()
	}
}
