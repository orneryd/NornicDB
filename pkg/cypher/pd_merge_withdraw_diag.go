package cypher

// DIAGNOSTIC (repro only, not a fix): when MERGE creates a relationship, withdraw
// its id from the statement's deleted set, so a relationship the statement
// deleted and MERGE re-made with the same deterministic id is not treated as
// deleted by later reads, SET persistence or deleted-entity views.

import (
	"context"

	"github.com/orneryd/nornicdb/internal/contextslot"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func pdmWithdrawDeleted(ctx context.Context, store storage.Engine, edge *storage.Edge) {
	deleted := contextslot.Get[deletedEntities](ctx)
	if deleted == nil || len(deleted.edges) == 0 || edge == nil {
		return
	}
	delete(deleted.edges, storage.EdgeID(edge.IdentityKey()))
	keyed := &deletedEntities{}
	keyed.addFromStorage(store, nil, map[storage.EdgeID]struct{}{edge.ID: {}})
	for key := range keyed.edges {
		delete(deleted.edges, key)
	}
}
