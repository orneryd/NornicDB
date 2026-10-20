package cypher

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

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
	var apply func(storage.ManualSuppressionEngine) error
	switch value := entity.(type) {
	case *storage.Node:
		id, selected := nativeNodeLookupID(e.storage, value)
		if !selected {
			return nil, nativeEntityNotFound(value)
		}
		apply = func(engine storage.ManualSuppressionEngine) error {
			if err := engine.SetNodeManuallySuppressed(id, suppressed); err != nil {
				return err
			}
			// Search drops a hidden node now rather than when a search can't
			// load it, which also clears its cached results, and indexes a
			// node shown again here too: the storage update event doesn't
			// reach search while the server's GraphQL handler holds it.
			if e.searchService == nil {
				return nil
			}
			if suppressed {
				_ = e.searchService.RemoveNode(id)
			} else if reader, ok := engine.(storage.Engine); ok {
				if node, err := reader.GetNode(id); err == nil {
					_ = e.searchService.IndexNode(node)
				}
			}
			return nil
		}
	case *storage.Edge:
		id, selected := nativeEdgeLookupID(e.storage, value)
		if !selected {
			return nil, nativeEntityNotFound(value)
		}
		apply = func(engine storage.ManualSuppressionEngine) error {
			return engine.SetEdgeManuallySuppressed(id, suppressed)
		}
	default:
		return nil, localizedError(localization.CypherKnowledgePolicyVisibilityEntityRequired(procedure), nil)
	}
	engine := manualSuppressionEngine(e.storage)
	if engine == nil {
		return nil, localizedError(localization.CypherKnowledgePolicyVisibilityUnsupported(procedure), nil)
	}
	run := func() error {
		if err := apply(engine); errors.Is(err, storage.ErrNotImplemented) {
			return localizedError(localization.CypherKnowledgePolicyVisibilityUnsupported(procedure), err)
		} else if err != nil {
			return err
		}
		e.InvalidateCommittedWriteCaches()
		return nil
	}
	if e.txContext != nil && e.txContext.active && e.txContext.storageWrapper != nil {
		// An entity the transaction deleted is gone by then; nothing to do.
		e.txContext.storageWrapper.schemaCommitActions = append(e.txContext.storageWrapper.schemaCommitActions, func() { _ = run() })
	} else if err := run(); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
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
