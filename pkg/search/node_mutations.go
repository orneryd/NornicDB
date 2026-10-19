package search

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// NodeMutation is an opaque reservation for a node's next index mutation.
// Keep the reservation with deferred work, including work already drained from
// a queue. It belongs only to the service that issued it.
type NodeMutation struct {
	service    *Service
	nodeID     storage.NodeID
	generation uint64
	// prev is the reservation this one superseded, restored if this one is
	// discarded (a write rejected at entry never happened).
	prev uint64
}

// ReserveNodeMutation supersedes older work for this node. Reservation and
// application share one lock, so a newer reservation cannot slip between the
// generation check and the index write. No lock is retained by the caller.
func (s *Service) ReserveNodeMutation(nodeID storage.NodeID) NodeMutation {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	s.nextNodeMutation++
	if s.pendingNodeMutations == nil {
		s.pendingNodeMutations = make(map[storage.NodeID]uint64)
	}
	prev := s.pendingNodeMutations[nodeID]
	s.pendingNodeMutations[nodeID] = s.nextNodeMutation
	return NodeMutation{service: s, nodeID: nodeID, generation: s.nextNodeMutation, prev: prev}
}

// RefreshNode makes the node's index entries match storage: indexed while it
// is stored and visible, removed otherwise. Live writers and readers that find
// a node missing use it rather than IndexNode or RemoveNode, so none of them
// decides from a read that a newer write may already have outdated.
func (s *Service) RefreshNode(nodeID storage.NodeID) error {
	if s == nil || nodeID == "" {
		return nil
	}
	return s.RefreshNodeWithMutation(s.ReserveNodeMutation(nodeID))
}

// RefreshNodeWithMutation applies a refresh reserved earlier, such as one
// queued for a storage event. Storage is read after the reservation: a write
// this read misses commits later and reserves a newer refresh of its own,
// which makes this one a no-op.
func (s *Service) RefreshNodeWithMutation(mutation NodeMutation) error {
	if mutation.service != s || s.nodeMutationRetired(mutation) {
		return nil
	}
	node, err := s.engine.GetNode(mutation.nodeID)
	if errors.Is(err, storage.ErrNotFound) {
		return s.RemoveNodeWithMutation(mutation)
	}
	if err != nil {
		s.DiscardNodeMutation(mutation)
		s.retryRefresh(mutation.nodeID, "", err)
		return err
	}
	return s.IndexNodeWithMutation(node, mutation)
}

// RefreshRetryFunc receives a node or relationship (the other ID is empty)
// whose refresh could not read storage. The refresh still has to happen.
type RefreshRetryFunc func(nodeID storage.NodeID, edgeID storage.EdgeID, err error)

// SetRefreshRetry sets who retries a refresh whose storage read failed, for
// every caller of RefreshNode and RefreshEdge: the owner of the database's
// search state queues it again. Without one, the error is only returned.
func (s *Service) SetRefreshRetry(retry RefreshRetryFunc) {
	if retry == nil {
		s.refreshRetry.Store(nil)
		return
	}
	s.refreshRetry.Store(&retry)
}

func (s *Service) retryRefresh(nodeID storage.NodeID, edgeID storage.EdgeID, err error) {
	if retry := s.refreshRetry.Load(); retry != nil {
		(*retry)(nodeID, edgeID, err)
	}
}

// nodeMutationRetired reports that no reservation is pending for the node: a
// newer refresh, which read storage after this one was reserved, has already
// been applied, so this one has nothing to read. A pending newer reservation
// does not retire this one, since discarding it would restore this one.
func (s *Service) nodeMutationRetired(mutation NodeMutation) bool {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	_, pending := s.pendingNodeMutations[mutation.nodeID]
	return !pending
}

func (s *Service) applyNodeMutation(mutation NodeMutation, apply func() error) error {
	// The caller has completed lockNodeMutation admission and owns indexMu.
	// Never hold this reservation lock while waiting for NODE population: new
	// queued mutations must be able to supersede a writer waiting there.
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if mutation.service != s || mutation.generation == 0 || s.pendingNodeMutations[mutation.nodeID] != mutation.generation {
		return nil
	}
	// Completed generations need no tombstone: an older reservation also fails
	// when the entry is absent. Memory tracks outstanding work, not all IDs ever
	// indexed, and a later reservation always has a fresh generation.
	defer delete(s.pendingNodeMutations, mutation.nodeID)
	return apply()
}

// DiscardNodeMutation retires work that could not be prepared for application.
// It cannot erase a newer reservation for the same node.
func (s *Service) DiscardNodeMutation(mutation NodeMutation) {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if mutation.service == s && s.pendingNodeMutations[mutation.nodeID] == mutation.generation {
		if mutation.prev != 0 {
			s.pendingNodeMutations[mutation.nodeID] = mutation.prev
		} else {
			delete(s.pendingNodeMutations, mutation.nodeID)
		}
	}
}

// EdgeMutation is NodeMutation for a relationship's vector and relationship
// search entries.
type EdgeMutation struct {
	service    *Service
	edgeID     storage.EdgeID
	generation uint64
	prev       uint64
}

// ReserveEdgeMutation supersedes older work for this relationship. Generations
// come from the same counter and lock as node reservations.
func (s *Service) ReserveEdgeMutation(edgeID storage.EdgeID) EdgeMutation {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	s.nextNodeMutation++
	if s.pendingEdgeMutations == nil {
		s.pendingEdgeMutations = make(map[storage.EdgeID]uint64)
	}
	prev := s.pendingEdgeMutations[edgeID]
	s.pendingEdgeMutations[edgeID] = s.nextNodeMutation
	return EdgeMutation{service: s, edgeID: edgeID, generation: s.nextNodeMutation, prev: prev}
}

// RefreshEdge makes the relationship's index entries match storage, as
// RefreshNode does for nodes; a deleted relationship leaves them.
func (s *Service) RefreshEdge(edgeID storage.EdgeID) error {
	if s == nil || edgeID == "" {
		return nil
	}
	return s.RefreshEdgeWithMutation(s.ReserveEdgeMutation(edgeID))
}

// RefreshEdgeWithMutation applies a relationship refresh reserved earlier;
// storage is read after the reservation, as in RefreshNodeWithMutation.
func (s *Service) RefreshEdgeWithMutation(mutation EdgeMutation) error {
	if mutation.service != s || s.edgeMutationRetired(mutation) {
		return nil
	}
	edge, err := s.engine.GetEdge(mutation.edgeID)
	if errors.Is(err, storage.ErrNotFound) {
		return s.removeEdgeWithMutation(mutation)
	}
	if err != nil {
		s.discardEdgeMutation(mutation)
		s.retryRefresh("", mutation.edgeID, err)
		return err
	}
	return s.indexEdgeWithMutation(edge, mutation)
}

// edgeMutationRetired is nodeMutationRetired for a relationship.
func (s *Service) edgeMutationRetired(mutation EdgeMutation) bool {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	_, pending := s.pendingEdgeMutations[mutation.edgeID]
	return !pending
}

// applyEdgeMutation is applyNodeMutation for relationships: the caller has
// completed lockNodeMutation admission when the write needs it.
func (s *Service) applyEdgeMutation(mutation EdgeMutation, apply func() error) error {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if mutation.service != s || mutation.generation == 0 || s.pendingEdgeMutations[mutation.edgeID] != mutation.generation {
		return nil
	}
	defer delete(s.pendingEdgeMutations, mutation.edgeID)
	return apply()
}

func (s *Service) discardEdgeMutation(mutation EdgeMutation) {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if mutation.service == s && s.pendingEdgeMutations[mutation.edgeID] == mutation.generation {
		if mutation.prev != 0 {
			s.pendingEdgeMutations[mutation.edgeID] = mutation.prev
		} else {
			delete(s.pendingEdgeMutations, mutation.edgeID)
		}
	}
}

func (s *Service) nodeMutationCurrentLocked(mutation NodeMutation) bool {
	return mutation.service == s && mutation.generation != 0 && s.pendingNodeMutations[mutation.nodeID] == mutation.generation
}

// claim is the population sequence's install-time check for a queued mutation
// (deferredIndexWrite.valid): true, and the reservation retired, while it is
// the newest for the node. It is called with indexMu held (lock order indexMu
// -> nodeMutationMu, as on the live path). The population is the only
// installer of queued writes, and a live write arriving meanwhile is queued
// behind it, so nothing applies between claim and the install.
func (mutation NodeMutation) claim() bool {
	s := mutation.service
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if !s.nodeMutationCurrentLocked(mutation) {
		return false
	}
	delete(s.pendingNodeMutations, mutation.nodeID)
	return true
}

func (s *Service) edgeMutationCurrentLocked(mutation EdgeMutation) bool {
	return mutation.service == s && mutation.generation != 0 && s.pendingEdgeMutations[mutation.edgeID] == mutation.generation
}

// claim is NodeMutation.claim for a relationship's vector write.
func (mutation EdgeMutation) claim() bool {
	s := mutation.service
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if !s.edgeMutationCurrentLocked(mutation) {
		return false
	}
	delete(s.pendingEdgeMutations, mutation.edgeID)
	return true
}

// current checks a relationship's reservation without retiring it: the
// relationship-search part of a write is installed ahead of its vector part,
// which retires the reservation.
func (mutation EdgeMutation) current() bool {
	s := mutation.service
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	return s.edgeMutationCurrentLocked(mutation)
}

// updateRelationshipWithMutation applies the relationship-search part of a
// reserved relationship write: during a population it is ordered in the
// population sequence, installed only while the reservation is current;
// otherwise it is applied now, under the reservation lock, so no newer write
// interleaves.
func (s *Service) updateRelationshipWithMutation(write deferredIndexWrite, mutation EdgeMutation) {
	if s.relationshipSearch() == nil {
		return
	}
	write.valid = mutation.current
	if queued, _, _ := s.queueOnPopulation(write); queued {
		return
	}
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	if s.edgeMutationCurrentLocked(mutation) {
		s.updateRelationshipNow(write)
	}
}

// applyNodeMutationHeld applies a live mutation admitted by
// lockNodeMutation with its reservation check, which found it the newest and
// still holds nodeMutationMu.
func (s *Service) applyNodeMutationHeld(mutation NodeMutation, apply func() error) error {
	defer delete(s.pendingNodeMutations, mutation.nodeID)
	return apply()
}

// applyEdgeMutationHeld is applyNodeMutationHeld for a relationship.
func (s *Service) applyEdgeMutationHeld(mutation EdgeMutation, apply func() error) error {
	defer delete(s.pendingEdgeMutations, mutation.edgeID)
	return apply()
}
