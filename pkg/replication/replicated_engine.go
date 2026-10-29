package replication

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ReplicatedEngine wraps a storage.Engine and routes write operations through a Replicator.
//
// Design:
//   - Reads are served locally from the embedded Engine.
//   - Writes are turned into replication Commands and applied via the Replicator.
//   - The embedded Engine is used only for reads; replicated writes are applied to the
//     *inner* engine by the StorageAdapter on each node.
//
// This wrapper intentionally operates on the *base* storage (the engine that stores
// fully-qualified IDs like "<db>:<id>") so multi-database isolation is preserved.
type ReplicatedEngine struct {
	storage.Engine
	readBinding storage.ReadBindingSlot

	replicator Replicator
	timeout    time.Duration
}

// ReplicatedEngine declares every optional method; admission is derived from this policy.
var replicatedEngineReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "EdgeCountByTypeInNamespace EdgeCountByStartLabelInNamespace EdgeCountByEndLabelInNamespace", Disposition: storage.CapabilityTransparent, Scope: "The maintainer namespace-counter owner retains explicit namespace/tier point-read semantics; common cursor admission adds no scan or counter state."},
	{Methods: "CheckEntityCommitAdmission", Disposition: storage.CapabilityRefused, Scope: "Leader admission belongs to the exact entity submitter, not data-read discovery."},
	{Methods: "ForwardEntityChanges", Disposition: storage.CapabilityRefused, Scope: "Stored-image forwarding retains the original managed publication route."},
	{Methods: "SubmitEntityCommit", Disposition: storage.CapabilityRefused, Scope: "The managed replication terminal owns the prepared commit and its receipt."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityTransparent, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "Backup GetInnerEngine GraphMutationVersion GraphMutationVersionInNamespace IsLeader " +
		"OpenReadSnapshot Restore SubscribeEvents",
		Disposition: storage.CapabilityTransparent, Scope: "Read view delegates to ReadCapabilitySource; writes and decorator metadata retain their exact owner."},
}

func (*ReplicatedEngine) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return replicatedEngineReadPolicy.Copy()
}

func NewReplicatedEngine(inner storage.Engine, replicator Replicator, timeout time.Duration) *ReplicatedEngine {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	view := &ReplicatedEngine{
		Engine:     inner,
		replicator: replicator,
		timeout:    timeout,
	}
	view.readBinding.Store(storage.RebindReadView(view))
	return view
}

// GetInnerEngine exposes the local storage chain for maintenance operations.
func (e *ReplicatedEngine) GetInnerEngine() storage.Engine {
	if e == nil {
		return nil
	}
	return e.Engine
}

// ReadCapabilitySource exposes the delegated local read view. Replicated writes
// remain operations of this engine, independently of read capability discovery.
func (e *ReplicatedEngine) ReadCapabilitySource() (storage.Engine, bool) {
	return e.GetInnerEngine(), true
}

// Backup delegates a native backup to the local storage chain. Replication
// writes are already applied locally before their acknowledgement returns.
func (e *ReplicatedEngine) Backup(path string) error {
	if e == nil || e.Engine == nil {
		return storage.ErrStorageClosed
	}
	if backupable, ok := storage.OwnedCapability[interface{ Backup(string) error }](e.Engine); ok {
		return backupable.Backup(path)
	}
	return storage.ErrNotImplemented
}

// Restore delegates native restore to the local storage chain.
func (e *ReplicatedEngine) Restore(path string) error {
	if e == nil || e.Engine == nil {
		return storage.ErrStorageClosed
	}
	if restorable, ok := storage.OwnedCapability[interface{ Restore(string) error }](e.Engine); ok {
		return restorable.Restore(path)
	}
	return storage.ErrNotImplemented
}

// GraphMutationVersion exposes graph mutations applied to the local engine.
func (e *ReplicatedEngine) GraphMutationVersion() (uint64, bool) {
	provider, ok := storage.ViewMetadataForEngine[storage.GraphMutationVersionProvider](e.Engine)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

// GraphMutationVersionInNamespace preserves database-scoped invalidation
// through the replication wrapper.
func (e *ReplicatedEngine) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	provider, ok := storage.ViewMetadataForEngine[storage.NamespaceGraphMutationVersionProvider](e.Engine)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersionInNamespace(namespace)
}

// IsLeader reports whether this node can accept writes in the current replication mode.
// This is a convenience for higher-level components (e.g. multidb startup) that need
// to avoid performing metadata migrations on standby/followers.
func (e *ReplicatedEngine) IsLeader() bool {
	if e == nil || e.replicator == nil {
		return true
	}
	return e.replicator.IsLeader()
}

func (e *ReplicatedEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if err := e.applyEntityCommand(CmdCreateNode, node); err != nil {
		return "", err
	}
	return node.ID, nil
}

func (e *ReplicatedEngine) UpdateNode(node *storage.Node) error {
	return e.applyEntityCommand(CmdUpdateNode, node)
}

func (e *ReplicatedEngine) DeleteNode(id storage.NodeID) error {
	return e.applyEntityCommand(CmdDeleteNode, id)
}

func (e *ReplicatedEngine) CreateEdge(edge *storage.Edge) error {
	return e.applyEntityCommand(CmdCreateEdge, edge)
}

func (e *ReplicatedEngine) UpdateEdge(edge *storage.Edge) error {
	return e.applyEntityCommand(CmdUpdateEdge, edge)
}

func (e *ReplicatedEngine) DeleteEdge(id storage.EdgeID) error {
	return e.applyEntityCommand(CmdDeleteEdge, id)
}

func (e *ReplicatedEngine) BulkCreateNodes(nodes []*storage.Node) error {
	return e.applyEntityCommand(CmdBulkCreateNodes, nodes)
}

func (e *ReplicatedEngine) BulkCreateEdges(edges []*storage.Edge) error {
	return e.applyEntityCommand(CmdBulkCreateEdges, edges)
}

func (e *ReplicatedEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	return e.applyEntityCommand(CmdBulkDeleteNodes, ids)
}

func (e *ReplicatedEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	return e.applyEntityCommand(CmdBulkDeleteEdges, ids)
}

func (e *ReplicatedEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	payload, err := storage.EncodeCommandPayload(struct {
		Prefix string `json:"prefix"`
	}{Prefix: prefix})
	if err != nil {
		return 0, 0, localizedError(localization.ReplicationEngineEncodeDeletePrefixFailed(err), err)
	}
	if err := e.replicator.Apply(&Command{Type: CmdDeleteByPrefix, Data: payload, Timestamp: time.Now()}, e.timeout); err != nil {
		return 0, 0, err
	}
	// Best-effort counts are not returned by the replication protocol today.
	return 0, 0, nil
}

// OpenReadSnapshot forwards to the local engine reads are served from.
func (r *ReplicatedEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	return storage.OpenReadSnapshot(ctx, r.Engine)
}

var _ storage.ReadSnapshotter = (*ReplicatedEngine)(nil)
var _ storage.EventSubscriber = (*ReplicatedEngine)(nil)

// SubscribeEvents forwards to the local storage chain: leader and follower
// writes are both applied to it, and it publishes them.
func (e *ReplicatedEngine) SubscribeEvents(handlers storage.EventHandlers) error {
	if e == nil || e.Engine == nil {
		return storage.ErrStorageClosed
	}
	return storage.SubscribeEvents(e.Engine, handlers)
}

// applyEntityCommand keeps native direct/bulk wire kinds and their producer policy.
func (e *ReplicatedEngine) applyEntityCommand(kind CommandType, value any) error {
	data, err := encodeEntityCommandPayload(kind, value)
	if err != nil {
		return err
	}
	return e.replicator.Apply(&Command{Type: kind, Data: data, Timestamp: time.Now()}, e.timeout)
}
func (e *ReplicatedEngine) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	// Active replication owns physical mutations. It has no native transaction
	// adapter, so discovery cannot authorize a local raw commit around it.
	if policy == storage.CommitOwnerPolicy && e.replicator != nil {
		return nil, false
	}
	return e.GetInnerEngine(), true
}

func (*ReplicatedEngine) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineDecorator, ReadState: storage.BindingDelegated, Membership: storage.BindingDelegated, Controller: storage.BindingDelegated, PendingActions: storage.BindingDelegated, SchemaPublication: storage.BindingDelegated, ReadLease: storage.BindingDelegated, StatementMode: storage.BindingDelegated, Identity: storage.BindingDelegated, Enforcement: storage.BindingDelegated, CompoundDependencies: storage.BindingDelegated, SameVersionFallback: storage.BindingDelegated, Delivery: storage.BindingDelegated, ExactEffects: storage.BindingOwned, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

func (e *ReplicatedEngine) ReadCapabilityBinding() *storage.ReadViewBinding {
	return e.readBinding.Binding(e, e.Engine)
}

func (e *ReplicatedEngine) PublishReadCapabilityBinding(binding *storage.ReadViewBinding) {
	e.readBinding.Store(binding)
}

// Stored integrity facts may not bypass this view.
func (e *ReplicatedEngine) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.StoredReadPolicyForEngine(e.Engine)
}
func (e *ReplicatedEngine) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	return storage.ReadStored(ctx, e.Engine, r)
}
