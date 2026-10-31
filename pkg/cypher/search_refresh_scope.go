package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// cypherRefreshScope is a statement's route to search for the node and
// relationship changes it made. A statement inside a transaction defers its
// changes to the transaction, which publishes them once its commit makes them
// visible; every other statement refreshes them now. A post-commit publish
// refreshes through the service captured when the transaction committed.
type cypherRefreshScope struct {
	executor *StorageExecutor
}

// PrepareSearchRefresh prepares the request. Inside a transaction the change
// is not visible yet: the transaction keeps it until its commit
// (refreshSearchAfterCommit), and drops it on rollback. A post-commit publish
// refreshes with the captured service and never defers again.
func (s cypherRefreshScope) PrepareSearchRefresh(ctx context.Context, request search.RefreshRequest) (*search.Service, bool, error) {
	if request.PostCommit {
		return request.PostCommitService, false, nil
	}
	if tx, ok := s.executor.storage.(*transactionStorageWrapper); ok {
		if s.executor.searchResolver != nil || s.executor.search() != nil {
			_, origin := storage.ContinueVectorWork(ctx, "cypher.search-refresh")
			tx.deferSearchRefresh(request.Node, request.Edge, origin)
		}
		return nil, true, nil
	}
	return s.executor.search(), false, nil
}
