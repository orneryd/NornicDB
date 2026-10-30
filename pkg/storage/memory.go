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
)

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

// Backup delegates to the embedded Badger engine's consistent streaming backup.
func (m *MemoryEngine) Backup(path string) error {
	return m.BadgerEngine.Backup(path)
}

// Restore delegates to the embedded Badger engine's streaming restore.
func (m *MemoryEngine) Restore(path string) error {
	return m.BadgerEngine.Restore(path)
}
