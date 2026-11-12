package cypher

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// profileKindManualSuppressionWriteRequest names the actual manual-suppression
// write request a statement issued: one call to
// storage.ManualSuppressionEngine.SetManualSuppression, the call that hides or
// shows one stored entity. It is a request, not a physical write: the stored
// metadata change is the engine's, so each call is one hit, and it contributed
// one acknowledged row only when the engine returned no error. A statement that
// hides or shows several entities reports one request per call, under this one
// kind.
const profileKindManualSuppressionWriteRequest = "ManualSuppressionWriteRequest"

// kpSuppressionOperation is the one actual visibility change a statement
// requested: the stored entity kind and ID the engine suppresses, and the
// search refresh that publishes the change. kp.suppress and kp.unsuppress are
// the same operation with suppressed true and false, so one owner performs both
// a node's and a relationship's change.
type kpSuppressionOperation struct {
	kind       storage.StoredEntityKind
	id         string
	suppressed bool
}

// callKpSuppress hides (suppressed) or shows a node or
// relationship on demand: kp.suppress(entity) /
// kp.unsuppress(entity) (#1064). A hidden entity is left out of
// MATCH, traversal and search, its data kept, and visible under reveal(),
// whether or not decay is enabled. Inside an explicit transaction the change
// is applied when the transaction commits, and not at all if it rolls back.
func (e *StorageExecutor) callKpSuppress(ctx context.Context, procedure string, args []interface{}, suppressed bool) (*ExecuteResult, error) {
	var entity interface{}
	if len(args) > 0 {
		entity = args[0]
	}
	var operation kpSuppressionOperation
	switch value := entity.(type) {
	case *storage.Node:
		operation = kpSuppressionOperation{
			kind:       storage.StoredEntityNodeKind,
			id:         string(value.ID),
			suppressed: suppressed,
		}
	case *storage.Edge:
		operation = kpSuppressionOperation{
			kind:       storage.StoredEntityEdgeKind,
			id:         string(value.ID),
			suppressed: suppressed,
		}
	default:
		return nil, localizedError(localization.CypherKnowledgePolicyVisibilityEntityRequired(procedure), nil)
	}
	engine := manualSuppressionEngine(e.getStorage(ctx))
	if engine == nil {
		return nil, localizedError(localization.CypherKnowledgePolicyVisibilityUnsupported(procedure), nil)
	}
	// The suppression write runs under the lifetime that executes it: this
	// statement when it is autocommit, or the transaction's admitted commit when
	// the change is deferred to COMMIT (see afterCommit). The operation is a
	// value, so a deferred change retains no statement context or entity
	// snapshot.
	run := func(runCtx context.Context) error {
		if err := e.applyKpSuppression(runCtx, engine, operation); errors.Is(err, storage.ErrNotImplemented) {
			return localizedError(localization.CypherKnowledgePolicyVisibilityUnsupported(procedure), err)
		} else if err != nil {
			return err
		}
		e.InvalidateCommittedWriteCaches()
		return nil
	}
	if wrapper := e.membership(ctx).storage; wrapper != nil {
		if err := wrapper.tx.AdmitWrite(); err != nil {
			return nil, err
		}
	}
	if err := e.afterCommit(ctx, run); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// applyKpSuppression performs the operation once: the one suppression write
// request this statement actually issued, recorded as that request, and then
// the canonical search refresh of the entity it changed. A refused write
// returns its error and refreshes nothing.
//
// Search drops a hidden node now rather than when a search can't load it,
// which also clears its cached results, and indexes a node shown again here
// too: the storage update event doesn't reach search while the server's
// GraphQL handler holds it. Storage hides a suppressed node, so the refresh
// removes it. As for a node, a hidden relationship leaves search now, so its
// vector doesn't take a top-k place, and a shown one returns.
func (e *StorageExecutor) applyKpSuppression(ctx context.Context, engine storage.ManualSuppressionEngine, operation kpSuppressionOperation) error {
	counter := queryprofile.Access(ctx, profileKindManualSuppressionWriteRequest, "")
	err := engine.SetManualSuppression(ctx, operation.kind, operation.id, operation.suppressed)
	queryprofile.RequestResult(counter, err)
	if err != nil {
		return err
	}
	request := search.RefreshRequest{}
	if operation.kind == storage.StoredEntityNodeKind {
		request.Node = storage.NodeID(operation.id)
	} else {
		request.Edge = storage.EdgeID(operation.id)
	}
	e.refreshSearch(ctx, request)
	return nil
}

// manualSuppressionEngine is the outermost engine under engine that
// suppresses on demand, past a transaction's wrapper: the namespaced engine,
// which maps the entity's ID to the stored one.
func manualSuppressionEngine(engine storage.Engine) storage.ManualSuppressionEngine {
	visited := map[storage.Engine]bool{}
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if _, transactional := engine.(*transactionStorageWrapper); !transactional {
			if suppressor, ok := engine.(storage.ManualSuppressionEngine); ok {
				return suppressor
			}
		}
		unwrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = unwrapper.GetInnerEngine()
	}
	return nil
}
