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
// Write contexts control leader preparation and admission; accepted native work
// retains an independent completion lifetime. Reads pass their context to the
// embedded local engine.
type ReplicatedEngine struct {
	storage.Engine

	replicator Replicator
	timeout    time.Duration
}

func NewReplicatedEngine(inner storage.Engine, replicator Replicator, timeout time.Duration) *ReplicatedEngine {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &ReplicatedEngine{
		Engine:     inner,
		replicator: replicator,
		timeout:    timeout,
	}
}

// GetInnerEngine exposes the local storage chain for maintenance operations.
func (e *ReplicatedEngine) GetInnerEngine() storage.Engine {
	if e == nil {
		return nil
	}
	return e.Engine
}

// Backup delegates a native backup to the local storage chain. Replication
// writes are already applied locally before their acknowledgement returns.
func (e *ReplicatedEngine) Backup(path string) error {
	if e == nil || e.Engine == nil {
		return storage.ErrStorageClosed
	}
	if backupable, ok := e.Engine.(interface{ Backup(string) error }); ok {
		return backupable.Backup(path)
	}
	return storage.ErrNotImplemented
}

// Restore delegates native restore to the local storage chain.
func (e *ReplicatedEngine) Restore(path string) error {
	if e == nil || e.Engine == nil {
		return storage.ErrStorageClosed
	}
	if restorable, ok := e.Engine.(interface{ Restore(string) error }); ok {
		return restorable.Restore(path)
	}
	return storage.ErrNotImplemented
}

// GraphMutationVersion exposes graph mutations applied to the local engine.
func (e *ReplicatedEngine) GraphMutationVersion() (uint64, bool) {
	provider, ok := e.Engine.(storage.GraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

// GraphMutationVersionInNamespace preserves database-scoped invalidation
// through the replication wrapper.
func (e *ReplicatedEngine) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	provider, ok := e.Engine.(storage.NamespaceGraphMutationVersionProvider)
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

func (e *ReplicatedEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	if err := e.applyEntityCommand(ctx, CmdCreateNode, node); err != nil {
		return "", err
	}
	return node.ID, nil
}

// UpdateNode sends the blind update image when the caller supplied no observed
// preimage and kept the client-edit policy, and otherwise forwards the typed
// request so its leader can plan against Before.
func (e *ReplicatedEngine) UpdateNode(ctx context.Context, updateRequest storage.NodeUpdateRequest) error {
	if updateRequest.Before == nil && updateRequest.Policy == storage.ClientNodeEdits {
		return e.applyEntityCommand(ctx, CmdUpdateNode, updateRequest.After)
	}
	return e.applyEntityCommand(ctx, CmdUpdateNodeRequest, updateRequest)
}

func (e *ReplicatedEngine) DeleteNode(ctx context.Context, id storage.NodeID) error {
	return e.applyEntityCommand(ctx, CmdDeleteNode, id)
}

func (e *ReplicatedEngine) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	return e.applyEntityCommand(ctx, CmdCreateEdge, edge)
}

// UpdateEdge keeps the blind update image for a request with no observed
// preimage and otherwise forwards the typed relationship request.
func (e *ReplicatedEngine) UpdateEdge(ctx context.Context, updateRequest storage.EdgeUpdateRequest) error {
	if updateRequest.Before == nil {
		return e.applyEntityCommand(ctx, CmdUpdateEdge, updateRequest.After)
	}
	return e.applyEntityCommand(ctx, CmdUpdateEdgeRequest, updateRequest)
}

func (e *ReplicatedEngine) DeleteEdge(ctx context.Context, id storage.EdgeID) error {
	return e.applyEntityCommand(ctx, CmdDeleteEdge, id)
}

func (e *ReplicatedEngine) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	return e.applyEntityCommand(ctx, CmdBulkCreateNodes, nodes)
}

func (e *ReplicatedEngine) BulkCreateEdges(ctx context.Context, edges []*storage.Edge) error {
	return e.applyEntityCommand(ctx, CmdBulkCreateEdges, edges)
}

func (e *ReplicatedEngine) BulkDeleteNodes(ctx context.Context, ids []storage.NodeID) error {
	return e.applyEntityCommand(ctx, CmdBulkDeleteNodes, ids)
}

func (e *ReplicatedEngine) BulkDeleteEdges(ctx context.Context, ids []storage.EdgeID) error {
	return e.applyEntityCommand(ctx, CmdBulkDeleteEdges, ids)
}

func (e *ReplicatedEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	payload, err := storage.EncodeCommandPayload(struct {
		Prefix string `json:"prefix"`
	}{Prefix: prefix})
	if err != nil {
		return 0, 0, localizedError(localization.ReplicationEngineEncodeDeletePrefixFailed(err), err)
	}
	if err := e.replicator.Apply(context.Background(), &Command{Type: CmdDeleteByPrefix, Data: payload, Timestamp: time.Now()}, e.timeout); err != nil {
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

// PreservesEventView declares that local reads and replicated writes use the
// wrapped engine's identifiers, committed rows and storage notifications.
func (e *ReplicatedEngine) PreservesEventView() {}

var _ storage.EventViewForwarder = (*ReplicatedEngine)(nil)

// EventNotificationsSettled reports delivery from the same local publisher.
func (e *ReplicatedEngine) EventNotificationsSettled() bool {
	return storage.EventNotificationsSettled(e.Engine)
}

// applyEntityCommand keeps native direct/bulk wire kinds and their producer policy.
func (e *ReplicatedEngine) applyEntityCommand(ctx context.Context, kind CommandType, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := encodeEntityCommandPayload(kind, value)
	if err != nil {
		return err
	}
	return e.replicator.Apply(ctx, &Command{Type: kind, Data: data, Timestamp: time.Now()}, e.timeout)
}
