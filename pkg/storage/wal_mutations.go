package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// beginNativeMutation joins wrapper admission to the native writer's existing
// final publication decision. The binding is call-local and never stored on a handle.
type nativeWALAdmission struct {
	owner *WALEngine
	state atomic.Uint32 // local, transferred to the existing receipt, released
}

func (a *nativeWALAdmission) releaseAccepted() {
	if a.state.CompareAndSwap(1, 2) {
		a.owner.mutationMu.RUnlock()
	}
}

// transfer preserves the preparation barrier without reacquiring a read lock
// behind a queued snapshot or restore writer.
func (a *nativeWALAdmission) transfer(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	if !a.state.CompareAndSwap(0, 1) {
		return nil, ErrTransactionClosed
	}
	selected := context.WithValue(ctx, nativeWALKey{}, (*nativeWALAdmission)(nil))
	return a.owner.submitEntityCommit(selected, prepared, settle, a.releaseAccepted)
}

func (w *WALEngine) entityPreparationRoute(id string, observed bool) (EntityCommitRoute, string, *NamespacedEngine, bool) {
	route, found := EntityCommitSubmitterFor(w.engine)
	var registry *storedEntityObserverRegistry
	if native, ok := UnwrapEngine(w.engine).(*BadgerEngine); ok {
		registry = native.storedEntityObservers.Load()
	}
	if (!found || !route.RequiresSubmission) && (!observed || registry == nil) {
		return route, "", nil, false
	}
	var scope *NamespacedEngine
	for engine := w.engine; engine != nil; {
		if current, ok := engine.(*NamespacedEngine); ok {
			scope = current
			break
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			break
		}
		next := wrapper.GetInnerEngine()
		if next == engine {
			break
		}
		engine = next
	}
	namespace, _, _ := ParseDatabasePrefix(id)
	if scope != nil {
		namespace = scope.Namespace()
	}
	if namespace == "" {
		namespace = w.getDatabaseName()
	}
	managed := observed && registry != nil && (*registry)[namespace].preparer != nil
	if managed {
		// The registered coordinator qualifies its own inputs.
		scope = nil
	}
	return route, namespace, scope, (found && route.RequiresSubmission) || managed
}

func (w *WALEngine) submitRoutedMutations(ctx context.Context, admission *nativeWALAdmission, route EntityCommitRoute, namespace string, scope *NamespacedEngine, operations []Operation, mode EntityCommitMode) (NodeID, error) {
	var prepared *PreparedEntityCommit
	var receipt *CommitReceipt
	var created NodeID
	// Preparation and submission observe both caller cancellation and wrapper
	// shutdown. Join only this pre-acceptance phase: holding it while waiting
	// on the accepted receipt would let a stopped waiter delay engine shutdown.
	err := func() (err error) {
		if !w.work.Enter() {
			return ErrStorageClosed
		}
		defer w.work.Leave()
		preparation, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		stop := lifecycle.AfterFuncJoined(w.work.Context(), func() { cancel(ErrStorageClosed) })
		defer stop()
		defer func() {
			if errors.Is(err, context.Canceled) && context.Cause(preparation) == ErrStorageClosed {
				err = ErrStorageClosed
			}
		}()
		if route.CheckAdmission != nil {
			if err := route.CheckAdmission(); err != nil {
				return err
			}
		}
		if scope != nil {
			for i := range operations {
				if err = scope.QualifyOperation(preparation, &operations[i]); err != nil {
					return err
				}
			}
		}
		var settle func(CommitOutcome)
		prepared, settle, err = PrepareEntityChanges(preparation, w.engine, namespace, operations, mode)
		if err != nil || prepared == nil {
			return err
		}
		if len(operations) > 0 && operations[0].Type == OpCreateNode {
			for _, op := range prepared.operations {
				if op.Type == OpCreateNode {
					created = op.Node.ID
					break
				}
			}
			if scope != nil {
				created = NodeID(StripDatabasePrefix(namespace, string(created)))
			}
		}
		receipt, err = admission.transfer(preparation, prepared, settle)
		if err != nil {
			prepared.Abort()
			if settle != nil {
				settle(CommitOutcome{State: CommitDiscarded, Err: err})
			}
		}
		return err
	}()
	if err != nil {
		return "", err
	}
	if receipt == nil {
		return created, nil
	}
	// The accepted owner has its own completion lifetime. Neither the stopped
	// preparation context nor its shutdown callback owns this receipt wait.
	outcome := receipt.WaitOutcome(ctx)
	if outcome.Err == nil || outcome.State == CommitDiscarded {
		return created, outcome.Err
	}
	return "", &AcceptedCommitError{State: outcome.State, LogicalID: prepared.LogicalID(), Err: outcome.Err}
}

func (w *WALEngine) finishNativeMutation(a *nativeWALAdmission) {
	if a == nil || a.state.CompareAndSwap(0, 2) {
		w.mutationMu.RUnlock()
	}
}

func (w *WALEngine) beginNativeMutation(ctx context.Context) (context.Context, *nativeWALAdmission, error) {
	if err := w.mutationMu.RLockContext(ctx); err != nil {
		return nil, nil, err
	}
	if !config.IsWALEnabled() {
		return ctx, nil, nil
	}
	authority := UnwrapEngine(w.engine)
	if issued, ok := authority.(interface{ StatementOwner() any }); ok && issued.StatementOwner() != nil {
		// An issued router/remote view already sends each operation to its
		// statement's participant. The participant owns acceptance and durable
		// publication; journaling here would make pending effects independently
		// replayable even when that owner rolls back. Keep the decorator lease,
		// but do not grant an independent native publication admission.
		return ctx, nil, nil
	}
	if _, ok := authority.(*BadgerEngine); !ok {
		w.mutationMu.RUnlock()
		return nil, nil, ErrInvalidData
	}
	admission := &nativeWALAdmission{owner: w}
	return context.WithValue(ctx, nativeWALKey{}, admission), admission, nil
}
func (w *WALEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return "", err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		id := ""
		if node != nil {
			id = string(node.ID)
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, false); required {
			return w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{{Type: OpCreateNode, Node: node}}, CommitModeDirect)
		}
	}
	return w.engine.CreateNode(selected, node)
}
func (w *WALEngine) UpdateNode(ctx context.Context, request NodeUpdateRequest) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		id := ""
		if request.After != nil {
			id = string(request.After.ID)
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, request.Before != nil || request.Policy != ClientNodeEdits); required {
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{request.Operation()}, CommitModeDirect)
			return err
		}
	}
	return w.engine.UpdateNode(selected, request)
}
func (w *WALEngine) DeleteNode(ctx context.Context, id NodeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		if route, namespace, scope, required := w.entityPreparationRoute(string(id), false); required {
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{{Type: OpDeleteNode, NodeID: id}}, CommitModeDirect)
			return err
		}
	}
	return w.engine.DeleteNode(selected, id)
}
func (w *WALEngine) CreateEdge(ctx context.Context, edge *Edge) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		id := ""
		if edge != nil {
			id = string(edge.ID)
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, false); required {
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{{Type: OpCreateEdge, Edge: edge}}, CommitModeDirect)
			return err
		}
	}
	return w.engine.CreateEdge(selected, edge)
}
func (w *WALEngine) UpdateEdge(ctx context.Context, request EdgeUpdateRequest) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		id := ""
		if request.After != nil {
			id = string(request.After.ID)
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, request.Before != nil); required {
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{request.Operation()}, CommitModeDirect)
			return err
		}
	}
	return w.engine.UpdateEdge(selected, request)
}
func (w *WALEngine) DeleteEdge(ctx context.Context, id EdgeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil {
		if route, namespace, scope, required := w.entityPreparationRoute(string(id), false); required {
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, []Operation{{Type: OpDeleteEdge, EdgeID: id}}, CommitModeDirect)
			return err
		}
	}
	return w.engine.DeleteEdge(selected, id)
}
func (w *WALEngine) BulkCreateNodes(ctx context.Context, nodes []*Node) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if config.IsWALEnabled() {
		database := ""
		for _, node := range nodes {
			if node == nil {
				return ErrInvalidData
			}
			candidate := w.databaseFromNode(node)
			if database == "" {
				database = candidate
			} else if database != candidate {
				return fmt.Errorf("wal: bulk create contains multiple databases: %q vs %q", database, candidate)
			}
		}
	}
	if admission != nil && len(nodes) > 0 {
		if route, namespace, scope, required := w.entityPreparationRoute(string(nodes[0].ID), false); required {
			operations := make([]Operation, len(nodes))
			for i, node := range nodes {
				operations[i] = Operation{Type: OpCreateNode, Node: node}
			}
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, operations, CommitModeImplicit)
			return err
		}
	}
	return w.engine.BulkCreateNodes(selected, nodes)
}
func (w *WALEngine) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if config.IsWALEnabled() {
		database := ""
		for _, edge := range edges {
			if edge == nil {
				return ErrInvalidData
			}
			candidate, err := w.databaseFromEdge(edge)
			if err != nil {
				return err
			}
			if database == "" {
				database = candidate
			} else if database != candidate {
				return fmt.Errorf("wal: bulk create contains multiple databases: %q vs %q", database, candidate)
			}
		}
	}
	if admission != nil && len(edges) > 0 {
		if route, namespace, scope, required := w.entityPreparationRoute(string(edges[0].ID), false); required {
			operations := make([]Operation, len(edges))
			for i, edge := range edges {
				operations[i] = Operation{Type: OpCreateEdge, Edge: edge}
			}
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, operations, CommitModeImplicit)
			return err
		}
	}
	return w.engine.BulkCreateEdges(selected, edges)
}
func (w *WALEngine) BulkDeleteNodes(ctx context.Context, ids []NodeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil && len(ids) > 0 {
		id := ""
		for _, candidate := range ids {
			if candidate != "" {
				id = string(candidate)
				break
			}
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, false); required {
			operations := make([]Operation, 0, len(ids))
			for _, item := range ids {
				if item != "" {
					operations = append(operations, Operation{Type: OpDeleteNode, NodeID: item, skipMissingDelete: true})
				}
			}
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, operations, CommitModeImplicit)
			return err
		}
	}
	return w.engine.BulkDeleteNodes(selected, ids)
}
func (w *WALEngine) BulkDeleteEdges(ctx context.Context, ids []EdgeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if admission != nil && len(ids) > 0 {
		id := ""
		for _, candidate := range ids {
			if candidate != "" {
				id = string(candidate)
				break
			}
		}
		if route, namespace, scope, required := w.entityPreparationRoute(id, false); required {
			operations := make([]Operation, 0, len(ids))
			for _, item := range ids {
				if item != "" {
					operations = append(operations, Operation{Type: OpDeleteEdge, EdgeID: item, skipMissingDelete: true})
				}
			}
			_, err := w.submitRoutedMutations(selected, admission, route, namespace, scope, operations, CommitModeImplicit)
			return err
		}
	}
	return w.engine.BulkDeleteEdges(selected, ids)
}
