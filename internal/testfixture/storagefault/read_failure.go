// Package storagefault injects committed-state read failures into real engines.
// It stays separate from the resource-owning testfixture package so that
// package remains free of storage dependencies.
package storagefault

import (
	"context"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ReadOperation selects one committed-state admission seam. Empty disables
// injection so the same real engine can stage writes and verify rollback.
type ReadOperation string

const (
	NodeRead       ReadOperation = "node"
	EdgeRead       ReadOperation = "edge"
	NodeCountRead  ReadOperation = "nodes"
	EdgeCountRead  ReadOperation = "edges"
	NodeStreamRead ReadOperation = "bytes"
)

// ReadEngine is the real engine capability required by the shared read seams.
type ReadEngine interface {
	storage.StreamingEngine
	storage.PrefixStatsEngine
}

// ReadFailureEngine delegates writes and transaction ownership to its real
// engine, failing only the selected committed-state read with the supplied
// error. For example: fixture.SetFailure(storagefault.NodeRead).
type ReadFailureEngine struct {
	ReadEngine
	storage.StoredEntityReader
	failure   error
	operation atomic.Value // ReadOperation, initialized before publication
}

// NewReadFailureEngine decorates an existing engine and its stored-reader
// capability. The caller retains ownership of the real engine's lifetime.
func NewReadFailureEngine(engine ReadEngine, stored storage.StoredEntityReader, failure error) *ReadFailureEngine {
	fixture := &ReadFailureEngine{ReadEngine: engine, StoredEntityReader: stored, failure: failure}
	fixture.operation.Store(ReadOperation(""))
	return fixture
}

// SetFailure switches injection atomically; an empty operation disables it.
func (e *ReadFailureEngine) SetFailure(operation ReadOperation) { e.operation.Store(operation) }

// GetInnerEngine preserves transaction discovery through storage decorators.
func (e *ReadFailureEngine) GetInnerEngine() storage.Engine { return e.ReadEngine }
func (e *ReadFailureEngine) GetStoredNode(id storage.NodeID) (*storage.Node, error) {
	if e.operation.Load() == NodeRead {
		return nil, e.failure
	}
	return e.StoredEntityReader.GetStoredNode(id)
}
func (e *ReadFailureEngine) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e.operation.Load() == EdgeRead {
		return nil, e.failure
	}
	return e.StoredEntityReader.GetStoredEdge(id)
}
func (e *ReadFailureEngine) NodeCount() (int64, error) {
	if e.operation.Load() == NodeCountRead {
		return 0, e.failure
	}
	return e.ReadEngine.NodeCount()
}
func (e *ReadFailureEngine) EdgeCount() (int64, error) {
	if e.operation.Load() == EdgeCountRead {
		return 0, e.failure
	}
	return e.ReadEngine.EdgeCount()
}
func (e *ReadFailureEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if e.operation.Load() == NodeCountRead {
		return 0, e.failure
	}
	return e.ReadEngine.NodeCountByPrefix(prefix)
}
func (e *ReadFailureEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if e.operation.Load() == EdgeCountRead {
		return 0, e.failure
	}
	return e.ReadEngine.EdgeCountByPrefix(prefix)
}
func (e *ReadFailureEngine) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	if e.operation.Load() == NodeStreamRead {
		return e.failure
	}
	return e.ReadEngine.StreamNodes(ctx, visit)
}
func (e *ReadFailureEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if e.operation.Load() == NodeStreamRead {
		return e.failure
	}
	return e.ReadEngine.StreamNodesWithOptions(ctx, opts, visit)
}

// WithStoredEntityRead preserves this decorator at the actual native snapshot
// reader boundary. On success the supplied original read runs exactly once;
// a selected failure returns before that read without substituting newer data.
func (e *ReadFailureEngine) WithStoredEntityRead(kind storage.StoredEntityReadKind, _ string, read func() error) error {
	operation := e.operation.Load()
	fail := false
	switch kind {
	case storage.StoredEntityNodePriorRead:
		fail = operation == NodeRead
	case storage.StoredEntityEdgePriorRead:
		fail = operation == EdgeRead
	case storage.StoredEntityNodeSnapshotScan:
		fail = operation == NodeCountRead || operation == NodeStreamRead
	case storage.StoredEntityEdgeSnapshotScan:
		fail = operation == EdgeCountRead
	}
	if fail {
		return e.failure
	}
	return read()
}
