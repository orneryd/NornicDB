package search

import "github.com/orneryd/nornicdb/pkg/storage"

// RefreshRetry is ownership of a node or relationship refresh whose read failed.
// Its owner must dispatch the same reservation or discard it.
type RefreshRetry struct {
	Node NodeMutation
	Edge EdgeMutation
}

func (r RefreshRetry) Key() string {
	if r.Node.mutation.owner() != nil {
		return "n:" + string(r.Node.mutation.id)
	}
	return "e:" + string(r.Edge.mutation.id)
}

func (r RefreshRetry) Current() bool {
	if s := r.Node.mutation.owner(); s != nil {
		return r.Node.mutation.current()
	}
	if s := r.Edge.mutation.owner(); s != nil {
		return r.Edge.mutation.current()
	}
	return false
}

// Retired distinguishes completed or discarded work from a live predecessor
// temporarily superseded by a newer reservation.
func (r RefreshRetry) Retired() bool {
	if s := r.Node.mutation.owner(); s != nil {
		return r.Node.mutation.retire()
	}
	if s := r.Edge.mutation.owner(); s != nil {
		return r.Edge.mutation.retire()
	}
	return true
}

// NewerThan compares reservations issued for the same entity by one service.
func (r RefreshRetry) NewerThan(other RefreshRetry) bool {
	if r.Node.mutation.owner() != nil {
		return r.Node.mutation.owner() == other.Node.mutation.owner() && r.Node.mutation.id == other.Node.mutation.id && r.Node.mutation.generation > other.Node.mutation.generation
	}
	return r.Edge.mutation.owner() != nil && r.Edge.mutation.owner() == other.Edge.mutation.owner() && r.Edge.mutation.id == other.Edge.mutation.id && r.Edge.mutation.generation > other.Edge.mutation.generation
}

func (r RefreshRetry) Discard() {
	if s := r.Node.mutation.owner(); s != nil {
		r.Node.mutation.discard()
	} else if s := r.Edge.mutation.owner(); s != nil {
		r.Edge.mutation.discard()
	}
}

// RefreshRetryFunc is the original notification callback. The reservation is
// discarded before notification; consumers needing retry ownership use
// SetRefreshRetryOwner.
type RefreshRetryFunc func(nodeID storage.NodeID, edgeID storage.EdgeID, err error)

// RefreshRetryOwner takes ownership of the failed reservation, including
// failures from synchronous consumers and the deferred event drain.
type RefreshRetryOwner func(work RefreshRetry, err error)

// SetRefreshRetry preserves the notification API for callers that queue their
// own new work rather than retaining a failed reservation.
func (s *Service) SetRefreshRetry(retry RefreshRetryFunc) {
	if retry == nil {
		s.SetRefreshRetryOwner(nil)
		return
	}
	s.SetRefreshRetryOwner(func(work RefreshRetry, err error) {
		work.Discard()
		retry(work.Node.mutation.id, work.Edge.mutation.id, err)
	})
}

func (s *Service) SetRefreshRetryOwner(owner RefreshRetryOwner) {
	if owner == nil {
		s.refreshRetry.Store(nil)
		return
	}
	s.refreshRetry.Store(&owner)
}

func (s *Service) retryRefresh(work RefreshRetry, err error) {
	if owner := s.refreshRetry.Load(); owner != nil {
		(*owner)(work, err)
	} else {
		work.Discard()
	}
}
