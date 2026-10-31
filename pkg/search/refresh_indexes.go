package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RefreshRequest is one post-mutation search refresh: the node and/or
// relationship the caller changed, and whether that change is already visible
// to the reader serving the refresh.
type RefreshRequest struct {
	Node storage.NodeID
	Edge storage.EdgeID
	// PostCommit reports that the change is committed and the scope's own
	// writes are no longer what search must read. A scope that defers
	// refreshes while a transaction is open uses it to refresh now instead of
	// deferring again.
	PostCommit bool
	// PostCommitService is the service captured for a transaction's publish.
	// It belongs to this request, not to a long-lived consumer. A nil value is
	// meaningful when no service existed at the start of the publish.
	PostCommitService *Service
}

// RefreshScope decides how a refresh request reaches the service that owns the
// indexes. It prepares the request once; the operation refreshes what the
// scope hands back.
type RefreshScope interface {
	// PrepareSearchRefresh returns the service to refresh, or deferred=true
	// when the scope has retained the request for later. A nil service with no
	// error means there is no work to apply. A prepare error is returned.
	PrepareSearchRefresh(ctx context.Context, request RefreshRequest) (svc *Service, deferred bool, err error)
}

// RefreshIndexes refreshes the indexes for a mutation a caller committed or
// made visible. It is the one path from a producer to Service.RefreshNode and
// Service.RefreshEdge: the scope decides whether the request happens now or
// after a commit, and the refresh itself reads the current stored state, so a
// newer write or delete wins over this request.
//
// A request naming nothing is a no-op and resolves no service. When the scope
// defers the request, the deferring owner refreshes it. Otherwise the node is
// refreshed before the relationship. Both are attempted even if the node
// fails; the node's error is the one returned, as before.
func RefreshIndexes[Scope RefreshScope](ctx context.Context, scope Scope, request RefreshRequest) error {
	if request.Node == "" && request.Edge == "" {
		return nil
	}
	ctx, origin := storage.AdmitVectorWork(ctx)
	svc, deferred, err := scope.PrepareSearchRefresh(ctx, request)
	if err != nil {
		return err
	}
	if deferred || svc == nil {
		return nil
	}
	var nodeErr error
	if request.Node != "" {
		nodeErr = svc.RefreshNode(request.Node, origin)
	}
	if request.Edge != "" {
		if edgeErr := svc.RefreshEdge(request.Edge, origin); edgeErr != nil && nodeErr == nil {
			nodeErr = edgeErr
		}
	}
	return nodeErr
}
