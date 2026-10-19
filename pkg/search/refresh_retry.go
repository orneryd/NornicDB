package search

import "github.com/orneryd/nornicdb/pkg/storage"

// RefreshRetry is ownership of a node or relationship refresh whose read failed.
// Its owner must dispatch the same reservation or discard it.
type RefreshRetry struct {
	Node NodeMutation
	Edge EdgeMutation
}

func (r RefreshRetry) Key() string {
	if r.Node.service != nil {
		return "n:" + string(r.Node.nodeID)
	}
	return "e:" + string(r.Edge.edgeID)
}

func (r RefreshRetry) Current() bool {
	if s := r.Node.service; s != nil {
		s.nodeMutationMu.Lock()
		defer s.nodeMutationMu.Unlock()
		return s.nodeMutationCurrentLocked(r.Node)
	}
	if s := r.Edge.service; s != nil {
		s.nodeMutationMu.Lock()
		defer s.nodeMutationMu.Unlock()
		return s.edgeMutationCurrentLocked(r.Edge)
	}
	return false
}

// Retired distinguishes completed or discarded work from a live predecessor
// temporarily superseded by a newer reservation.
func (r RefreshRetry) Retired() bool {
	if s := r.Node.service; s != nil {
		return s.nodeMutationRetired(r.Node)
	}
	if s := r.Edge.service; s != nil {
		return s.edgeMutationRetired(r.Edge)
	}
	return true
}

// NewerThan compares reservations issued for the same entity by one service.
func (r RefreshRetry) NewerThan(other RefreshRetry) bool {
	if r.Node.service != nil {
		return r.Node.service == other.Node.service && r.Node.nodeID == other.Node.nodeID && r.Node.generation > other.Node.generation
	}
	return r.Edge.service != nil && r.Edge.service == other.Edge.service && r.Edge.edgeID == other.Edge.edgeID && r.Edge.generation > other.Edge.generation
}

func (r RefreshRetry) Discard() {
	if s := r.Node.service; s != nil {
		s.DiscardNodeMutation(r.Node)
	} else if s := r.Edge.service; s != nil {
		s.discardEdgeMutation(r.Edge)
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
		retry(work.Node.nodeID, work.Edge.edgeID, err)
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
