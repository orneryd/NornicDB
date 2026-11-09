package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
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
func (s cypherRefreshScope) PrepareSearchRefresh(_ context.Context, request search.RefreshRequest) (*search.Service, bool, error) {
	if request.PostCommit {
		return request.PostCommitService, false, nil
	}
	if tx, ok := s.executor.storage.(*transactionStorageWrapper); ok {
		if s.executor.searchResolver != nil || s.executor.search() != nil {
			tx.deferSearchRefresh(request.Node, request.Edge)
		}
		return nil, true, nil
	}
	return s.executor.search(), false, nil
}

// refreshSearch routes one change through the scope under the lifetime of the
// search service that owns the refresh: that service's BeginShutdown/Close
// end it, not the request that produced the change. Without a service there
// is no refresh work now; a transaction keeps the change for its commit, as
// PrepareSearchRefresh does.
func (e *StorageExecutor) refreshSearch(request search.RefreshRequest) {
	service := request.PostCommitService
	if !request.PostCommit {
		service = e.search()
	}
	if service != nil {
		_ = search.RefreshIndexes(service.LifetimeContext(), cypherRefreshScope{executor: e}, request)
		return
	}
	if request.PostCommit || (request.Node == "" && request.Edge == "") {
		return
	}
	if tx, ok := e.storage.(*transactionStorageWrapper); ok && e.searchResolver != nil {
		tx.deferSearchRefresh(request.Node, request.Edge)
	}
}
