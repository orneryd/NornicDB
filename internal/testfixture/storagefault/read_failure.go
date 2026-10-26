// Package storagefault supports shared storage admission and publication fault
// fixtures using real engines.
// It stays separate from the resource-owning testfixture package so that
// package remains free of storage dependencies.
package storagefault

import (
	"context"
	"reflect"
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
	namespace func() string
	failure   error
	operation atomic.Value // ReadOperation, initialized before publication
}

// NewReadFailureEngine decorates an existing engine and its stored-reader
// capability. The caller retains ownership of the real engine's lifetime.
func NewReadFailureEngine(engine ReadEngine, stored storage.StoredEntityReader, failure error) *ReadFailureEngine {
	fixture := &ReadFailureEngine{ReadEngine: engine, StoredEntityReader: stored, failure: failure}
	if provider, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](engine); ok {
		fixture.namespace = provider.Namespace
	}
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
	return storage.ReadWithCapability(e.StoredEntityReader, func(reader storage.StoredEntityReader) (*storage.Node, error) { return reader.GetStoredNode(id) }, nil)
}
func (e *ReadFailureEngine) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e.operation.Load() == EdgeRead {
		return nil, e.failure
	}
	return storage.ReadWithCapability(e.StoredEntityReader, func(reader storage.StoredEntityReader) (*storage.Edge, error) { return reader.GetStoredEdge(id) }, nil)
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
	return storage.ReadWithCapability(e.ReadEngine, func(reader storage.PrefixStatsEngine) (int64, error) { return reader.NodeCountByPrefix(prefix) }, nil)
}
func (e *ReadFailureEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if e.operation.Load() == EdgeCountRead {
		return 0, e.failure
	}
	return storage.ReadWithCapability(e.ReadEngine, func(reader storage.PrefixStatsEngine) (int64, error) { return reader.EdgeCountByPrefix(prefix) }, nil)
}
func (e *ReadFailureEngine) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	if e.operation.Load() == NodeStreamRead {
		return e.failure
	}
	return storage.VisitWithCapability(e.ReadEngine, func(reader storage.StreamingEngine, deliver func(*storage.Node) error) error {
		return reader.StreamNodes(ctx, deliver)
	}, nil, visit)
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

// Fault injection is an issued data view, not transparent discovery. Membership
// and managed commit references remain the original real engine's owners.
func (e *ReadFailureEngine) CapabilityOwnerSource(_ storage.OwnerPolicy) (storage.Engine, bool) {
	return e.ReadEngine, true
}
func (*ReadFailureEngine) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }

var readFailureEngineReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "ReadStored StoredReadPolicy Namespace", Disposition: storage.CapabilityOwned, Scope: "Fact and namespace reads retain the issued fault view; mandatory read failures propagate before any body fallback."},
	{Methods: "GetStoredNode GetStoredEdge GetStoredOutgoingEdges GetStoredIncomingEdges GetStoredNodesByLabel StreamStoredNodesByLabelProjected StreamStoredEdgesByType NodeCountByPrefix EdgeCountByPrefix StreamNodes StreamEdges StreamNodeChunks GetInnerEngine", Disposition: storage.CapabilityOwned, Scope: "Existing fixture methods retain selected fault interception and the real read source."},
	{Methods: "SetFailure WithStoredEntityRead", Disposition: storage.CapabilityRefused, Scope: "Exact fixture control/read continuation, never a data-read discovery grant."},
}

func (*ReadFailureEngine) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return readFailureEngineReadPolicy.Copy()
}
func (*ReadFailureEngine) ReadCapabilityScoped(request reflect.Type) bool {
	return readFailureEngineReadPolicy.Scoped(request)
}
func (e *ReadFailureEngine) Namespace() string {
	if e.namespace != nil {
		return e.namespace()
	}
	return ""
}
func (*ReadFailureEngine) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingOwned, Membership: storage.BindingDelegated, Controller: storage.BindingDelegated, PendingActions: storage.BindingDelegated, SchemaPublication: storage.BindingDelegated, ReadLease: storage.BindingDelegated, StatementMode: storage.BindingDelegated, Identity: storage.BindingDelegated, Enforcement: storage.BindingDelegated, CompoundDependencies: storage.BindingDelegated, SameVersionFallback: storage.BindingDelegated, Delivery: storage.BindingDelegated, ExactEffects: storage.BindingOwned, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}
func (e *ReadFailureEngine) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.StoredReadPolicyForEngine(e.ReadEngine)
}
func (e *ReadFailureEngine) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	operation := e.operation.Load()
	if operation == NodeRead && (len(r.Nodes) > 0 || len(r.Edges) > 0 || len(r.Adjacency) > 0) || operation == EdgeRead && (len(r.Edges) > 0 || len(r.Adjacency) > 0) {
		return nil, e.failure
	}
	return storage.ReadStored(ctx, e.ReadEngine, r)
}
