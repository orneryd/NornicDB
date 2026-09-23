// Package storage provides storage engine implementations for NornicDB.
//
// The storage package defines the Engine interface and provides multiple implementations:
//   - MemoryEngine: In-memory storage using BadgerDB's in-memory mode (for testing)
//   - BadgerEngine: Persistent disk-based storage
//
// All storage engines are thread-safe and support concurrent operations.
//
// Example Usage:
//
//	// Create in-memory storage (for testing)
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create a node
//	node := &storage.Node{
//		ID:     "user-001",
//		Labels: []string{"User"},
//		Properties: map[string]any{
//			"name": "Alice",
//		},
//	}
//	engine.CreateNode(node)
package storage

import (
	"fmt"
	"strings"
)

// normalizeLabel converts a label to lowercase for case-insensitive matching.
// This makes NornicDB compatible with Neo4j's case-insensitive label handling.
func normalizeLabel(label string) string {
	return strings.ToLower(label)
}

// MemoryEngine is a thread-safe in-memory graph storage implementation.
// It wraps BadgerDB's in-memory mode for testing purposes.
//
// Use Cases:
//   - Unit testing (no disk I/O, fast cleanup)
//   - Loading Neo4j exports into memory for analysis
//   - Small datasets that fit entirely in RAM
//   - Development and prototyping
//
// Implementation Note:
//
//	MemoryEngine is a thin wrapper around BadgerEngine with InMemory=true.
//	This ensures tests use the exact same code path as production.
type MemoryEngine struct {
	*BadgerEngine
}

// NewMemoryEngine creates a new in-memory storage engine for testing.
// It uses BadgerDB's in-memory mode internally.
//
// Example:
//
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
// Note: For tests, prefer storage.NewTestEngine(t) which handles cleanup.
func NewMemoryEngine() *MemoryEngine {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		// In testing context, panic is acceptable for setup failures
		panic(fmt.Sprintf("failed to create in-memory BadgerEngine: %v", err))
	}
	return &MemoryEngine{BadgerEngine: engine}
}

// BeginTransaction starts a new transaction.
// Returns *BadgerTransaction for compatibility with the executor.
func (m *MemoryEngine) BeginTransaction() (*BadgerTransaction, error) {
	return m.BadgerEngine.BeginTransaction()
}

// DeleteByPrefix delegates to the underlying BadgerEngine.
func (m *MemoryEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	return m.BadgerEngine.DeleteByPrefix(prefix)
}

// Backup is intentionally unsupported for in-memory engines so the DB API
// retains its portable JSON fallback for non-persistent storage.
func (m *MemoryEngine) Backup(string) error {
	return ErrNotImplemented
}

// Restore is intentionally unsupported for in-memory engines; the DB API
// restores its portable JSON fallback instead.
func (m *MemoryEngine) Restore(string) error {
	return ErrNotImplemented
}
