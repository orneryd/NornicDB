package storage

// Author: L4 fixture ownership migration (Codex), 1 November 2026.

import "testing"

// Constructor registration is enforced repository-wide by internal/testfixture.
// Keep the actual cleanup-boundary assertion here.
func TestMemoryFixtureCleanupOwnsUnderlyingEngine(t *testing.T) {
	var fixture *MemoryEngine
	t.Run("owner", func(t *testing.T) { fixture = newTestMemoryEngine(t) })
	if fixture == nil || !fixture.closed {
		t.Fatal("test boundary did not close the underlying engine")
	}
}
