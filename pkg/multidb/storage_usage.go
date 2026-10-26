package multidb

import (
	"github.com/orneryd/nornicdb/pkg/localization"
)

// storageUsage describes a complete logical publication, rather than an
// individual route's estimate. Negative deltas do not reserve future capacity.
type storageUsage struct{ nodes, edges, nodeBytes, edgeBytes int64 }
type reservedUsage struct{ nodes, edges, bytes int64 }

// reserveStorageUsage admits one logical commit against actual usage and all
// accepted positive growth. Caller cancellation does not retire its allowance.
func (l *DatabaseInfo) reserveStorageUsage(database, id string, delta storageUsage, limits StorageLimits) error {
	l = l.accountingOwner()
	l.sizeMu.Lock()
	defer l.sizeMu.Unlock()
	if _, exists := l.pending[id]; exists {
		return storageReservationExists{id}
	}
	r := reservedUsage{max(delta.nodes, 0), max(delta.edges, 0), max(delta.nodeBytes+delta.edgeBytes, 0)}
	// Subtraction avoids overflowing a proposed total. Growth is rejected if
	// existing usage already exceeds a newly lowered limit; nongrowth remains
	// available so users can reduce it.
	if r.nodes > 0 && limits.MaxNodes > 0 && r.nodes > limits.MaxNodes-l.usageNodes-l.reserved.nodes {
		return localizedError(localization.MultidbMaxNodesReached(database, l.usageNodes+l.reserved.nodes+r.nodes, limits.MaxNodes), ErrStorageLimitExceeded)
	}
	if r.edges > 0 && limits.MaxEdges > 0 && r.edges > limits.MaxEdges-l.usageEdges-l.reserved.edges {
		return localizedError(localization.MultidbMaxEdgesReached(database, l.usageEdges+l.reserved.edges+r.edges, limits.MaxEdges), ErrStorageLimitExceeded)
	}
	if r.bytes > 0 && limits.MaxBytes > 0 && r.bytes > limits.MaxBytes-l.nodeSize-l.edgeSize-l.reserved.bytes {
		return localizedError(localization.MultidbMaxBytesExceeded(database, l.nodeSize+l.edgeSize+l.reserved.bytes, limits.MaxBytes, r.bytes), ErrStorageLimitExceeded)
	}
	if l.pending == nil {
		l.pending = make(map[string]reservedUsage)
	}
	l.pending[id] = r
	l.reserved.nodes += r.nodes
	l.reserved.edges += r.edges
	l.reserved.bytes += r.bytes
	return nil
}

type storageReservationExists struct{ id string }

func (e storageReservationExists) Error() string {
	return "storage reservation already exists: " + e.id
}

// publish consumes immutable, deduplicated storage publication pairs. Storage
// owns logical publication identity and calls this once, including on peers
// that never admitted a local reservation.
func (l *DatabaseInfo) publishStorageUsage(id string, delta storageUsage) {
	l = l.accountingOwner()
	l.sizeMu.Lock()
	defer l.sizeMu.Unlock()
	l.usageNodes += delta.nodes
	l.usageEdges += delta.edges
	l.nodeSize += delta.nodeBytes
	l.edgeSize += delta.edgeBytes
	l.totalSize = l.nodeSize + l.edgeSize
	l.removeReservationLocked(id)
}

func (l *DatabaseInfo) discardStorageUsage(id string) {
	l = l.accountingOwner()
	l.sizeMu.Lock()
	defer l.sizeMu.Unlock()
	l.removeReservationLocked(id)
}

func (l *DatabaseInfo) removeReservationLocked(id string) {
	if r, exists := l.pending[id]; exists {
		l.reserved.nodes -= r.nodes
		l.reserved.edges -= r.edges
		l.reserved.bytes -= r.bytes
		delete(l.pending, id)
	}
}
