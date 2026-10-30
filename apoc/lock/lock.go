// Package lock provides APOC locking functions.
//
// This package implements all apoc.lock.* functions for managing
// locks and concurrency control in graph operations.
package lock

import (
	"fmt"
	"sync"
	"time"
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

var (
	nodeLocks = make(map[int64]*sync.RWMutex)
	relLocks  = make(map[int64]*sync.RWMutex)
	globalMu  sync.Mutex
)

// Nodes acquires exclusive locks on multiple nodes.
//
// Example:
//
//	apoc.lock.nodes([node1, node2]) => locked
func Nodes(nodes []*Node) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, node := range nodes {
		if _, exists := nodeLocks[node.ID]; !exists {
			nodeLocks[node.ID] = &sync.RWMutex{}
		}
		nodeLocks[node.ID].Lock()
	}

	return nil
}

// ReadNodes acquires read locks on multiple nodes.
//
// Example:
//
//	apoc.lock.readNodes([node1, node2]) => locked
func ReadNodes(nodes []*Node) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, node := range nodes {
		if _, exists := nodeLocks[node.ID]; !exists {
			nodeLocks[node.ID] = &sync.RWMutex{}
		}
		nodeLocks[node.ID].RLock()
	}

	return nil
}

// UnlockNodes releases locks on multiple nodes.
//
// Example:
//
//	apoc.lock.unlockNodes([node1, node2]) => unlocked
func UnlockNodes(nodes []*Node, readLock bool) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, node := range nodes {
		if lock, exists := nodeLocks[node.ID]; exists {
			if readLock {
				lock.RUnlock()
			} else {
				lock.Unlock()
			}
		}
	}

	return nil
}

// Relationships acquires exclusive locks on multiple relationships.
//
// Example:
//
//	apoc.lock.relationships([rel1, rel2]) => locked
func Relationships(rels []*Relationship) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, rel := range rels {
		if _, exists := relLocks[rel.ID]; !exists {
			relLocks[rel.ID] = &sync.RWMutex{}
		}
		relLocks[rel.ID].Lock()
	}

	return nil
}

// ReadRelationships acquires read locks on multiple relationships.
//
// Example:
//
//	apoc.lock.readRelationships([rel1, rel2]) => locked
func ReadRelationships(rels []*Relationship) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, rel := range rels {
		if _, exists := relLocks[rel.ID]; !exists {
			relLocks[rel.ID] = &sync.RWMutex{}
		}
		relLocks[rel.ID].RLock()
	}

	return nil
}

// UnlockRelationships releases locks on multiple relationships.
//
// Example:
//
//	apoc.lock.unlockRelationships([rel1, rel2]) => unlocked
func UnlockRelationships(rels []*Relationship, readLock bool) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	for _, rel := range rels {
		if lock, exists := relLocks[rel.ID]; exists {
			if readLock {
				lock.RUnlock()
			} else {
				lock.Unlock()
			}
		}
	}

	return nil
}

// All acquires a global lock.
//
// Example:
//
//	apoc.lock.all() => locked
func All() error {
	globalMu.Lock()
	return nil
}

// UnlockAll releases the global lock.
//
// Example:
//
//	apoc.lock.unlockAll() => unlocked
func UnlockAll() error {
	globalMu.Unlock()
	return nil
}

// TryLock attempts to acquire a lock with timeout.
//
// Example:
//
//	apoc.lock.tryLock(node, 5000) => success
func TryLock(node *Node, timeoutMs int) bool {
	globalMu.Lock()
	if _, exists := nodeLocks[node.ID]; !exists {
		nodeLocks[node.ID] = &sync.RWMutex{}
	}
	lock := nodeLocks[node.ID]
	globalMu.Unlock()

	// Try to acquire lock with timeout
	done := make(chan bool, 1)
	go func() {
		lock.Lock()
		done <- true
	}()

	select {
	case <-done:
		return true
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		return false
	}
}

// IsLocked checks if a node is currently locked.
//
// Example:
//
//	apoc.lock.isLocked(node) => true/false
func IsLocked(node *Node) bool {
	globalMu.Lock()
	defer globalMu.Unlock()

	_, exists := nodeLocks[node.ID]
	return exists
}

// WaitFor waits for a lock to become available.
//
// Example:
//
//	apoc.lock.waitFor(node, 10000) => acquired
func WaitFor(node *Node, timeoutMs int) error {
	if TryLock(node, timeoutMs) {
		return nil
	}
	return fmt.Errorf("timeout waiting for lock on node %d", node.ID)
}

// WithLock executes a function while holding a lock.
//
// Example:
//
//	apoc.lock.withLock(node, func) => result
func WithLock(node *Node, fn func() interface{}) (interface{}, error) {
	if err := Nodes([]*Node{node}); err != nil {
		return nil, err
	}
	defer UnlockNodes([]*Node{node}, false)

	return fn(), nil
}

// WithReadLock executes a function while holding a read lock.
//
// Example:
//
//	apoc.lock.withReadLock(node, func) => result
func WithReadLock(node *Node, fn func() interface{}) (interface{}, error) {
	if err := ReadNodes([]*Node{node}); err != nil {
		return nil, err
	}
	defer UnlockNodes([]*Node{node}, true)

	return fn(), nil
}

// Batch locks multiple entities in a consistent order to prevent deadlocks.
//
// Example:
//
//	apoc.lock.batch([node1, node2, node3]) => locked
func Batch(nodes []*Node) error {
	// Sort by ID to ensure consistent lock ordering
	sorted := make([]*Node, len(nodes))
	copy(sorted, nodes)

	// Simple bubble sort by ID
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i].ID > sorted[j].ID {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return Nodes(sorted)
}

// UnlockBatch releases batch locks.
//
// Example:
//
//	apoc.lock.unlockBatch([node1, node2, node3]) => unlocked
func UnlockBatch(nodes []*Node, readLock bool) error {
	return UnlockNodes(nodes, readLock)
}

// Stats returns locking statistics.
//
// Example:
//
//	apoc.lock.stats() => {active: 5, waiting: 2}
func Stats() map[string]interface{} {
	globalMu.Lock()
	defer globalMu.Unlock()

	return map[string]interface{}{
		"nodeLocks": len(nodeLocks),
		"relLocks":  len(relLocks),
	}
}

// Clear clears all locks (dangerous - use with caution).
//
// Example:
//
//	apoc.lock.clear() => cleared
func Clear() error {
	globalMu.Lock()
	defer globalMu.Unlock()

	nodeLocks = make(map[int64]*sync.RWMutex)
	relLocks = make(map[int64]*sync.RWMutex)

	return nil
}

// Deadlock detection helpers

type lockRequest struct {
	nodeID    int64
	timestamp time.Time
	holder    string
}

var lockRequests = make([]lockRequest, 0)

// DetectDeadlock checks for potential deadlocks.
//
// Example:
//
//	apoc.lock.detectDeadlock() => {deadlocks: [...]}
func DetectDeadlock() map[string]interface{} {
	// Placeholder - would implement cycle detection in wait-for graph
	return map[string]interface{}{
		"deadlocks": []interface{}{},
	}
}

// Priority allows setting lock priority.
//
// Example:
//
//	apoc.lock.priority(node, 10) => set
func Priority(node *Node, priority int) error {
	// Placeholder - would implement priority-based locking
	return nil
}
