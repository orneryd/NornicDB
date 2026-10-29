package multidb

import (
	"context"

	"errors"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// sizeTrackingEngine wraps a database-scoped engine and maintains cached storage-size
// bytes in DatabaseManager metadata for O(1) reads.
type sizeTrackingEngine struct {
	storage.Engine
	readBinding storage.ReadBindingSlot
	manager     *DatabaseManager
	dbName      string
	checker     *databaseLimitChecker
	writeMu     *writeAdmission
	runtime     *managedDatabaseRuntime
	runtimeErr  error
	info        *DatabaseInfo
}

// sizeTrackingEngine declares every optional method; admission is derived from this policy.
var sizeTrackingEngineReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "EdgeCountByTypeInNamespace EdgeCountByStartLabelInNamespace EdgeCountByEndLabelInNamespace", Disposition: storage.CapabilityTransparent, Scope: "The maintainer namespace-counter owner retains explicit namespace/tier point-read semantics; common cursor admission adds no scan or counter state."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityTransparent, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "SetManualSuppression", Disposition: storage.CapabilityRefused, Scope: "Exact visibility metadata mutation; stored quota/accounting population is unchanged."},
	{Methods: "CommitTransaction ForEachNodeIDByLabel GetEdgeCurrentHead GetEdgeLatestVisible GetEdgeVisibleAt " +
		"GetEdgesBetweenVisibleAt GetEdgesByTypeVisibleAt GetIncomingEdgesVisibleAt GetInnerEngine GetNodeCurrentHead " +
		"GetNodeLatestVisible GetNodeVisibleAt GetNodesByLabelVisibleAt GetOutgoingEdgesVisibleAt GetQueryLimitChecker " +
		"GetQueryNode GraphMutationVersion LifecycleStatus MatchEdgesBetween Namespace " +
		"NodeCountByLabel OpenReadSnapshot PauseLifecycle PropertyKeyKnown RecordMaterializedAccess " +
		"RegisterSnapshotReader ResumeLifecycle SetLifecycleSchedule StreamEdges StreamEdgesByType " +
		"StreamNodeChunks StreamNodes StreamNodesByLabelProjected StreamNodesByPrefix SubscribeEvents " +
		"TopLifecycleDebtKeys TriggerPruneNow",
		Disposition: storage.CapabilityTransparent, Scope: "Read view delegates to ReadCapabilitySource; writes and decorator metadata retain their exact owner."},
}

func (*sizeTrackingEngine) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return sizeTrackingEngineReadPolicy.Copy()
}

var _ storage.Engine = (*sizeTrackingEngine)(nil)

// Preserve optional streaming capabilities of wrapped engines so query execution
// can keep LIMIT short-circuit behavior on hot paths.
var _ storage.StreamingEngine = (*sizeTrackingEngine)(nil)
var _ storage.LabelNodeIDLookupEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCVisibilityEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCIndexedVisibilityEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCHeadEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCLifecycleEngine = (*sizeTrackingEngine)(nil)
var _ storage.ProjectedLabelNodeReader = (*sizeTrackingEngine)(nil)
var _ storage.PropertyKeyLookup = (*sizeTrackingEngine)(nil)

func newSizeTrackingEngine(engine storage.Engine, manager *DatabaseManager, dbName string, checkers ...*databaseLimitChecker) storage.Engine {
	wrapped := &sizeTrackingEngine{writeMu: &writeAdmission{},
		Engine:  engine,
		manager: manager,
		dbName:  dbName,
	}
	if len(checkers) > 0 {
		wrapped.checker = checkers[0]
	}
	if manager == nil {
		return wrapped
	}
	// Constructor callers hold manager metadata ownership (or have not yet
	// published their test fixture). Binding and registration never scan or wait.
	wrapped.info = manager.databases[dbName]
	if wrapped.info != nil {
		owner := managedEntityOwnerFor(engine)
		wrapped.runtime, wrapped.runtimeErr = bindManagedRuntime(wrapped.info, wrapped.info.limitsSnapshot(), dbName, owner)
		if wrapped.runtime != nil {
			wrapped.writeMu = &wrapped.runtime.writeMu
		}
		if wrapped.checker != nil {
			wrapped.checker.info.Store(wrapped.info)
		}
		// Catalog writes retain their existing internal native journal owner.
		if dbName != manager.configSystemDatabase() {
			if registrar, ok := storage.OwnedCapability[interface {
				RegisterManagedEntityPreparer(string, storage.ManagedEntityPrepare, storage.ManagedEntityInitialize)
			}](owner.observer); ok {
				registrar.RegisterManagedEntityPreparer(dbName, wrapped.prepareManagedDirect, wrapped.initializeManagedUsage)
			}
		}
	}

	wrapped.readBinding.Store(storage.RebindReadView(wrapped))
	return wrapped
}

func (t *sizeTrackingEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return t.checker
}

func (t *sizeTrackingEngine) GetInnerEngine() storage.Engine {
	return t.Engine
}

// ReadCapabilitySource keeps delegated reads transparent while writes continue
// through this wrapper's admission and size accounting.
func (t *sizeTrackingEngine) ReadCapabilitySource() (storage.Engine, bool) {
	return t.GetInnerEngine(), true
}

// GetQueryNode forwards the query-row node read (storage.QueryNodeReader),
// else reads the full node.
func (t *sizeTrackingEngine) GetQueryNode(id storage.NodeID) (*storage.Node, error) {
	return storage.ReadWithCapability(t.Engine, func(reader storage.QueryNodeReader) (*storage.Node, error) {
		return reader.GetQueryNode(id)
	}, func() (*storage.Node, error) { return t.Engine.GetNode(id) })
}

// RecordMaterializedAccess preserves result-access accounting across the
// database size-tracking wrapper.
func (t *sizeTrackingEngine) RecordMaterializedAccess(entityID string) {
	storage.RecordMaterializedAccess(t.Engine, entityID)
}

// RegisterSnapshotReader preserves MVCC lifecycle admission and tracking across
// size-tracking wrapper boundaries.
func (t *sizeTrackingEngine) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	if provider, ok := storage.ViewMetadataForEngine[storage.MVCCLifecycleEngine](t.Engine); ok {
		return provider.RegisterSnapshotReader(info)
	}
	return func() {}
}

// LifecycleStatus delegates lifecycle status when supported by the wrapped engine.
func (t *sizeTrackingEngine) LifecycleStatus() map[string]interface{} {
	if provider, ok := storage.ViewMetadataForEngine[storage.MVCCLifecycleEngine](t.Engine); ok {
		return provider.LifecycleStatus()
	}
	return map[string]interface{}{"enabled": false}
}

// TriggerPruneNow delegates lifecycle prune-now when supported by the wrapped engine.
func (t *sizeTrackingEngine) TriggerPruneNow(ctx context.Context) error {
	if provider, ok := storage.OwnedCapability[storage.MVCCLifecycleEngine](t.Engine); ok {
		return provider.TriggerPruneNow(ctx)
	}
	return nil
}

// PauseLifecycle delegates lifecycle pause when supported by the wrapped engine.
func (t *sizeTrackingEngine) PauseLifecycle() {
	if provider, ok := storage.OwnedCapability[storage.MVCCLifecycleEngine](t.Engine); ok {
		provider.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported by the wrapped engine.
func (t *sizeTrackingEngine) ResumeLifecycle() {
	if provider, ok := storage.OwnedCapability[storage.MVCCLifecycleEngine](t.Engine); ok {
		provider.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported by the wrapped engine.
func (t *sizeTrackingEngine) SetLifecycleSchedule(interval time.Duration) error {
	if provider, ok := storage.OwnedCapability[storage.MVCCLifecycleScheduleEngine](t.Engine); ok {
		return provider.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported by the wrapped engine.
func (t *sizeTrackingEngine) TopLifecycleDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	return storage.ReadValueWithCapability(t.Engine, func(reader storage.MVCCLifecycleDebtEngine) []storage.MVCCLifecycleDebtKey {
		return reader.TopLifecycleDebtKeys(limit)
	}, nil)
}

// StreamNodes delegates streaming iteration to the wrapped engine when available.
func (t *sizeTrackingEngine) StreamNodes(ctx context.Context, fn func(*storage.Node) error) error {
	return storage.StreamNodesWithFallback(ctx, t.Engine, 1000, fn)
}

// StreamEdges delegates streaming iteration to the wrapped engine when available.
func (t *sizeTrackingEngine) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	err := storage.StreamEdgesWithFallback(ctx, t.Engine, 1000, fn)
	if errors.Is(err, storage.ErrIterationStopped) {
		return nil
	}
	return err
}

// StreamNodeChunks delegates chunked streaming to wrapped engine when available.
func (t *sizeTrackingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*storage.Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunk := make([]*storage.Node, 0, chunkSize)
	err := t.StreamNodes(ctx, func(node *storage.Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			if err := fn(chunk); err != nil {
				return err
			}
			chunk = make([]*storage.Node, 0, chunkSize)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		return fn(chunk)
	}
	return nil
}

// StreamNodesByPrefix preserves prefix-scoped streaming when supported by wrapped engine.
func (t *sizeTrackingEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(*storage.Node) error) error {
	return t.Engine.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Prefix: prefix, WithEmbeddings: true, ApplyDecayFilter: true}, fn)
}

// StreamNodesWithOptions delegates the unified options-driven scan to the
// wrapped engine. Size tracking only concerns writes, so the scan passes
// through untouched.
func (t *sizeTrackingEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(node *storage.Node) error) error {
	return t.Engine.StreamNodesWithOptions(ctx, opts, fn)
}

// StreamNodesByLabelProjected preserves label-indexed candidate streaming
// across this wrapper boundary. Embedding storage.Engine only promotes the
// methods declared on that interface, not every method the wrapped engine's
// dynamic type happens to implement, so a label-scoped MATCH would otherwise
// fail this type assertion and fall back to a whole-namespace StreamNodes
// scan (O(store) instead of O(label); see #561).
func (t *sizeTrackingEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return storage.VisitWithCapability(t.Engine, func(reader storage.ProjectedLabelNodeReader, deliver func(*storage.Node) error) error {
		return reader.StreamNodesByLabelProjected(label, properties, deliver)
	}, nil, visit)
}

// ForEachNodeIDByLabel preserves label->ID lookup capabilities across wrapper
// boundaries so LIMIT + label paths can avoid full node decode.
func (t *sizeTrackingEngine) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	return storage.VisitNodeIDsByLabel(t.Engine, label, visit)
}

func (t *sizeTrackingEngine) GetNodeLatestVisible(id storage.NodeID) (*storage.Node, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCVisibilityEngine) (*storage.Node, error) {
		return provider.GetNodeLatestVisible(id)
	}, func() (*storage.Node, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetNodeVisibleAt(id storage.NodeID, version storage.MVCCVersion) (*storage.Node, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCVisibilityEngine) (*storage.Node, error) {
		return provider.GetNodeVisibleAt(id, version)
	}, func() (*storage.Node, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetEdgeLatestVisible(id storage.EdgeID) (*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCVisibilityEngine) (*storage.Edge, error) {
		return provider.GetEdgeLatestVisible(id)
	}, func() (*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetEdgeVisibleAt(id storage.EdgeID, version storage.MVCCVersion) (*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCVisibilityEngine) (*storage.Edge, error) {
		return provider.GetEdgeVisibleAt(id, version)
	}, func() (*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCIndexedVisibilityEngine) ([]*storage.Node, error) {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}, func() ([]*storage.Node, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		return provider.GetOutgoingEdgesVisibleAt(nodeID, version)
	}, func() ([]*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		return provider.GetIncomingEdgesVisibleAt(nodeID, version)
	}, func() ([]*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}, func() ([]*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

// MatchEdgesBetween keeps projected edge-between matching on the wrapped
// engine (storage.EdgesBetweenMatcher).
func (t *sizeTrackingEngine) MatchEdgesBetween(startID, endID storage.NodeID, edgeType string, properties []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	return storage.MatchEdgesBetween(t.Engine, startID, endID, edgeType, properties, match)
}

func (t *sizeTrackingEngine) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}, func() ([]*storage.Edge, error) { return nil, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetNodeCurrentHead(id storage.NodeID) (storage.MVCCHead, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCHeadEngine) (storage.MVCCHead, error) {
		return provider.GetNodeCurrentHead(id)
	}, func() (storage.MVCCHead, error) { return storage.MVCCHead{}, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) GetEdgeCurrentHead(id storage.EdgeID) (storage.MVCCHead, error) {
	return storage.ReadWithCapability(t.Engine, func(provider storage.MVCCHeadEngine) (storage.MVCCHead, error) {
		return provider.GetEdgeCurrentHead(id)
	}, func() (storage.MVCCHead, error) { return storage.MVCCHead{}, storage.ErrNotImplemented })
}

func (t *sizeTrackingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	var id storage.NodeID
	err := t.applyEntityOperations([]storage.Operation{{Type: storage.OpCreateNode, Node: node}}, &id, storage.CommitModeDirect)
	return id, err
}
func (t *sizeTrackingEngine) UpdateNode(node *storage.Node) error {
	return t.applyEntityOperations([]storage.Operation{{Type: storage.OpUpdateNode, Node: node}}, nil, storage.CommitModeDirect)
}
func (t *sizeTrackingEngine) DeleteNode(id storage.NodeID) error {
	return t.applyEntityOperations([]storage.Operation{{Type: storage.OpDeleteNode, NodeID: id}}, nil, storage.CommitModeDirect)
}
func (t *sizeTrackingEngine) CreateEdge(edge *storage.Edge) error {
	return t.applyEntityOperations([]storage.Operation{{Type: storage.OpCreateEdge, Edge: edge}}, nil, storage.CommitModeDirect)
}
func (t *sizeTrackingEngine) UpdateEdge(edge *storage.Edge) error {
	return t.applyEntityOperations([]storage.Operation{{Type: storage.OpUpdateEdge, Edge: edge}}, nil, storage.CommitModeDirect)
}
func (t *sizeTrackingEngine) DeleteEdge(id storage.EdgeID) error {
	return t.applyEntityOperations([]storage.Operation{{Type: storage.OpDeleteEdge, EdgeID: id}}, nil, storage.CommitModeDirect)
}

// Bulk entity operations stage one native transaction and share the ordinary
// managed coordinator. Rejection leaves no partially committed prefix.
func (t *sizeTrackingEngine) BulkCreateNodes(nodes []*storage.Node) error {
	ops := make([]storage.Operation, len(nodes))
	for i, node := range nodes {
		ops[i] = storage.Operation{Type: storage.OpCreateNode, Node: node}
	}
	return t.applyEntityOperations(ops, nil, storage.CommitModeImplicit)
}
func (t *sizeTrackingEngine) BulkCreateEdges(edges []*storage.Edge) error {
	ops := make([]storage.Operation, len(edges))
	for i, edge := range edges {
		ops[i] = storage.Operation{Type: storage.OpCreateEdge, Edge: edge}
	}
	return t.applyEntityOperations(ops, nil, storage.CommitModeImplicit)
}
func (t *sizeTrackingEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	ops := make([]storage.Operation, len(ids))
	for i, id := range ids {
		ops[i] = storage.Operation{Type: storage.OpDeleteNode, NodeID: id}
	}
	return t.applyEntityOperations(ops, nil, storage.CommitModeImplicit)
}
func (t *sizeTrackingEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	ops := make([]storage.Operation, len(ids))
	for i, id := range ids {
		ops[i] = storage.Operation{Type: storage.OpDeleteEdge, EdgeID: id}
	}
	return t.applyEntityOperations(ops, nil, storage.CommitModeImplicit)
}

func (t *sizeTrackingEngine) applyEntityOperations(ops []storage.Operation, createResult *storage.NodeID, mode storage.EntityCommitMode) error {
	if len(ops) == 0 {
		return nil
	}
	_, limits, err := t.managedUsageState()
	if err != nil {
		return err
	}
	owner := managedEntityOwnerFor(t.Engine)
	if owner.route.CheckAdmission != nil {
		if admissionErr := owner.route.CheckAdmission(); admissionErr != nil {
			if owner.route.ForwardDirect == nil {
				return admissionErr
			}
			qualified, err := t.qualifyEntityOperations(ops, owner)
			if err != nil {
				return err
			}
			if err := owner.route.ForwardDirect(context.Background(), qualified, mode); err != nil {
				return err
			}
			if createResult != nil {
				*createResult = qualified[0].Node.ID
				if owner.namespace != nil {
					*createResult = storage.NodeID(storage.StripDatabasePrefix(t.dbName, string(*createResult)))
				}
			}
			return nil
		}
	}
	if !t.runtime.requiresManaged(limits) && (!owner.found || !owner.route.RequiresSubmission) && (len(owner.writeBoundaries) == 0 || mode == storage.CommitModeDirect) && (mode == storage.CommitModeDirect || (ops[0].Type != storage.OpDeleteNode && ops[0].Type != storage.OpDeleteEdge)) {
		if mode == storage.CommitModeDirect {
			if createResult != nil {
				id, err := t.Engine.CreateNode(ops[0].Node)
				*createResult = id
				return err
			}
			return storage.ApplyEntityChange(t.Engine, ops[0], t.Engine.UpdateNode, t.Engine.DeleteNode)
		}
		switch ops[0].Type {
		case storage.OpCreateNode:
			nodes := make([]*storage.Node, len(ops))
			for i, op := range ops {
				nodes[i] = op.Node
			}
			return t.Engine.BulkCreateNodes(nodes)
		case storage.OpCreateEdge:
			edges := make([]*storage.Edge, len(ops))
			for i, op := range ops {
				edges[i] = op.Edge
			}
			return t.Engine.BulkCreateEdges(edges)
		case storage.OpDeleteNode:
			ids := make([]storage.NodeID, len(ops))
			for i, op := range ops {
				ids[i] = op.NodeID
			}
			return t.Engine.BulkDeleteNodes(ids)
		case storage.OpDeleteEdge:
			ids := make([]storage.EdgeID, len(ops))
			for i, op := range ops {
				ids[i] = op.EdgeID
			}
			return t.Engine.BulkDeleteEdges(ids)
		}
	}

	prepared, settle, err := t.prepareManagedDirect(context.Background(), ops, mode)
	if err != nil {
		return err
	}
	if prepared != nil {
		receipt, err := owner.route.Submitter.SubmitEntityCommit(context.Background(), prepared, settle)
		if err != nil {
			prepared.Abort()
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
			return nornicerrors.MarkCommitRolledBack(err)
		}
		outcome := receipt.WaitOutcome(context.Background())
		if outcome.State == storage.CommitDiscarded {
			return nornicerrors.MarkCommitRolledBack(outcome.Err)
		}
		if outcome.Err != nil {
			return outcome.Err
		}
	}
	if createResult != nil {
		*createResult = ops[0].Node.ID
		if owner.namespace != nil {
			*createResult = storage.NodeID(storage.StripDatabasePrefix(t.dbName, string(*createResult)))
		}
	}
	return nil
}

// qualifyEntityOperations uses the same namespace/token input owner as direct
// Engine methods. It never materializes a final stored image prematurely.
func (t *sizeTrackingEngine) qualifyEntityOperations(ops []storage.Operation, owner managedEntityOwner) ([]storage.Operation, error) {
	if owner.namespace == nil {
		return ops, nil
	}
	qualified := append([]storage.Operation(nil), ops...)
	for i := range qualified {
		op := &qualified[i]
		var err error
		if op.Node != nil {
			op.Node, err = owner.namespace.NodeWriteInput(op.Node)
			if err != nil {
				return nil, err
			}
		}
		if op.Edge != nil {
			op.Edge, err = owner.namespace.EdgeWriteInput(op.Edge)
			if err != nil {
				return nil, err
			}
		}
		if op.Type == storage.OpDeleteNode {
			op.NodeID = owner.namespace.PrefixNodeID(op.NodeID)
		}
		if op.Type == storage.OpDeleteEdge {
			op.EdgeID = owner.namespace.PrefixEdgeID(op.EdgeID)
		}
	}
	return qualified, nil
}

// prepareManagedDirect is also registered for leader raw-command admission.
// It stages and admits once; its caller owns submission and terminal settlement.
func (t *sizeTrackingEngine) prepareManagedDirect(ctx context.Context, ops []storage.Operation, mode storage.EntityCommitMode) (prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome), prepareErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nornicerrors.MarkCommitRolledBack(err)
	}
	owner := managedEntityOwnerFor(t.Engine)
	if owner.route.CheckAdmission != nil {
		if err := owner.route.CheckAdmission(); err != nil {
			return nil, nil, nornicerrors.MarkCommitRolledBack(err)
		}
	}
	if !owner.found || owner.begin == nil || owner.observer == nil || owner.registry == nil {
		return nil, nil, nornicerrors.MarkCommitRolledBack(t.managedCapabilityError())
	}
	qualified, err := t.qualifyEntityOperations(ops, owner)
	if err != nil {
		return nil, nil, nornicerrors.MarkCommitRolledBack(err)
	}
	tx, err := storage.BeginEntityTransaction(t.Engine, mode)
	if err != nil {
		return nil, nil, nornicerrors.MarkCommitRolledBack(err)
	}
	defer func() {
		if prepared == nil {
			_ = tx.Rollback()
		}
		if prepareErr != nil {
			prepareErr = nornicerrors.MarkCommitRolledBack(prepareErr)
		}
	}()
	if err := tx.SetNamespace(t.dbName); err != nil {
		return nil, nil, err
	}
	for _, op := range qualified {
		apply := func() error {
			return storage.ApplyEntityChange(tx, op, func(node *storage.Node) error { return storage.ApplyNodeUpdate(tx, node) }, func(id storage.NodeID) error { return tx.DeleteGraphNode(id, storage.CascadeEdges) })
		}
		for i := len(owner.writeBoundaries) - 1; i >= 0; i-- {
			policy, next := owner.writeBoundaries[i], apply
			apply = func() error { return policy.WithEntityChange(op, next) }
		}
		if err := apply(); err != nil {
			return nil, nil, err
		}
	}
	return t.admitPrepared(ctx, tx, owner)
}

// NodeCountByLabel preserves the label counter across this wrapper, so the
// count-only MATCH fast path (and its transaction-visible form, #683) reads
// it instead of scanning the label. An engine without one counts its label's
// nodes.
func (t *sizeTrackingEngine) NodeCountByLabel(label string) (int64, error) {
	return storage.CountNodesWithLabel(context.Background(), t.Engine, label)
}

// Namespace preserves the wrapped engine's namespace (its database name), so
// what a caller reads through this wrapper is the database it tracks; it is
// "" when the wrapped engine has none, which every caller treats as unknown.
func (t *sizeTrackingEngine) Namespace() string {
	if scoped, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](t.Engine); ok {
		return scoped.Namespace()
	}
	return ""
}

// GraphMutationVersion preserves query-cache invalidation through this wrapper.
func (t *sizeTrackingEngine) GraphMutationVersion() (uint64, bool) {
	provider, ok := storage.ViewMetadataForEngine[storage.GraphMutationVersionProvider](t.Engine)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

// PropertyKeyKnown forwards the database's property-key lookup
// (storage.PropertyKeyLookup), so Cypher's SET counter sees the key names
// through the server's wrapper as it does on the namespaced engine (#907).
func (t *sizeTrackingEngine) PropertyKeyKnown(name string) bool {
	lookup, ok := storage.ViewMetadataForEngine[storage.PropertyKeyLookup](t.Engine)
	return ok && lookup.PropertyKeyKnown(name)
}

// OpenReadSnapshot forwards to the tracked engine; reads are not charged.
func (t *sizeTrackingEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	return storage.OpenReadSnapshot(ctx, t.Engine)
}

var _ storage.ReadSnapshotter = (*sizeTrackingEngine)(nil)
var _ storage.EventSubscriber = (*sizeTrackingEngine)(nil)

// SubscribeEvents forwards to the tracked engine.
func (t *sizeTrackingEngine) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(t.Engine, handlers)
}

func (t *sizeTrackingEngine) initializeManagedUsage(ctx context.Context) error {
	if t.runtimeErr != nil {
		return t.runtimeErr
	}
	if t.runtime == nil {
		return ErrDatabaseNotFound
	}
	return t.runtime.initializeManagedUsage(ctx, managedEntityOwnerFor(t.Engine).boundary)
}
func (t *sizeTrackingEngine) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return t.GetInnerEngine(), true
}

func (*sizeTrackingEngine) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineDecorator, ReadState: storage.BindingDelegated, Membership: storage.BindingDelegated, Controller: storage.BindingDelegated, PendingActions: storage.BindingDelegated, SchemaPublication: storage.BindingDelegated, ReadLease: storage.BindingDelegated, StatementMode: storage.BindingDelegated, Identity: storage.BindingDelegated, Enforcement: storage.BindingDelegated, CompoundDependencies: storage.BindingDelegated, SameVersionFallback: storage.BindingDelegated, Delivery: storage.BindingDelegated, ExactEffects: storage.BindingOwned, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

func (t *sizeTrackingEngine) ReadCapabilityBinding() *storage.ReadViewBinding {
	return t.readBinding.Binding(t, t.Engine)
}

func (t *sizeTrackingEngine) PublishReadCapabilityBinding(binding *storage.ReadViewBinding) {
	t.readBinding.Store(binding)
}

// Stored integrity facts may not bypass this view.
func (t *sizeTrackingEngine) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.StoredReadPolicyForEngine(t.Engine)
}
func (t *sizeTrackingEngine) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	return storage.ReadStored(ctx, t.Engine, r)
}

func (t *sizeTrackingEngine) SetManualSuppression(kind storage.StoredEntityKind, id string, suppressed bool) error {
	if owner, ok := storage.OwnedCapability[storage.ManualSuppressionEngine](t.Engine); ok {
		return owner.SetManualSuppression(kind, id, suppressed)
	}
	return storage.ErrNotImplemented
}
