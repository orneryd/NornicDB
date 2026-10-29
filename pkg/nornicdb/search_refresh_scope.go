package nornicdb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// dbRefreshScope refreshes search through a database's own search service. It
// resolves the service exactly as any other DB search caller does, waiting for
// the database's initial build so a stale scan cannot re-add a node the caller
// deleted.
type dbRefreshScope struct {
	db      *DB
	dbName  string
	storage storage.Engine
}

// PrepareSearchRefresh resolves the database's search service. The request is
// never deferred here: the caller made its change visible, so the refresh
// applies it after the initial build settles. Resolution failures, including a
// database without a search service, are the operation's error.
func (s dbRefreshScope) PrepareSearchRefresh(ctx context.Context, _ search.RefreshRequest) (*search.Service, bool, error) {
	svc, err := s.db.EnsureSearchIndexes(ctx, s.dbName, s.storage, SearchIndexBuildWait)
	return svc, false, err
}
