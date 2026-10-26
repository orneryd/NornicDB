// Author: Codex implementation lane, 2026-10-27 (NOR-148 snapshot reader contract).
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

func (r *ReplicatedEngine) OwnsSnapshotReader(issued storage.Engine) bool { return issued == r }

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
