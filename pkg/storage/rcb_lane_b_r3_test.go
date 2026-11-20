package storage

// Root-cause diagnostic round 3, campaign lane B (test-only).
// G-namespace-list-close: the maintainer's overlap of an admitted listing and
// Close, with the decisive value (was the edge-counter map cleared while the
// listing still held its read lock?) and a test-side listing that captures
// both map references under the lock before iterating.

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

func rcbListCaptured(b *BadgerEngine) []string {
	b.namespaceCountsMu.RLock()
	nodes, edges := b.namespaceNodeCounts, b.namespaceEdgeCounts
	seen := map[string]struct{}{}
	for prefix := range nodes {
		if name := strings.TrimSuffix(prefix, ":"); name != "" {
			seen[name] = struct{}{}
		}
	}
	for prefix := range edges {
		if name := strings.TrimSuffix(prefix, ":"); name != "" {
			seen[name] = struct{}{}
		}
	}
	b.namespaceCountsMu.RUnlock()
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}

func TestRCBNamespaceListingDuringClose(t *testing.T) {
	for _, variant := range []string{"product-ListNamespaces", "variant-captured-maps"} {
		t.Run(variant, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			const namespaces = 250000
			engine.namespaceCountsMu.Lock()
			for i := 0; i < namespaces; i++ {
				engine.namespaceNodeCounts[fmt.Sprintf("nor77_%06d:", i)] = 1
			}
			engine.namespaceEdgeCounts["nor77_edge_only:"] = 1
			engine.namespaceCountsMu.Unlock()
			done := make(chan []string, 1)
			go func() {
				if variant == "product-ListNamespaces" {
					done <- engine.ListNamespaces()
				} else {
					done <- rcbListCaptured(engine)
				}
			}()
			deadline := time.Now().Add(5 * time.Second)
			for engine.namespaceCountsMu.TryLock() {
				engine.namespaceCountsMu.Unlock()
				if time.Now().After(deadline) {
					t.Fatal("listing did not take its read lock")
				}
				runtime.Gosched()
			}
			closed := make(chan error, 1)
			go func() { closed <- engine.Close() }()
			clearedWhileHeld := false
			for time.Now().Before(deadline) {
				if engine.namespaceEdgeCounts == nil {
					if !engine.namespaceCountsMu.TryLock() {
						clearedWhileHeld = true
					} else {
						engine.namespaceCountsMu.Unlock()
					}
					break
				}
				runtime.Gosched()
			}
			got := <-done
			closeErr := <-closed
			hasEdgeOnly := false
			for _, name := range got {
				if name == "nor77_edge_only" {
					hasEdgeOnly = true
				}
			}
			t.Logf("RCB77 %s: edge-counter map set to nil while the listing still held its read lock=%v; listed=%d (want %d) edge-only present=%v closeErr=%v",
				variant, clearedWhileHeld, len(got), namespaces+1, hasEdgeOnly, closeErr)
		})
	}
}
