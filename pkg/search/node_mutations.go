package search

import (
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// lockMutation orders one identity with its live index application while the
// registry mutex protects generation maps. Hash collisions only serialize
// unrelated work; they cannot change ownership or ordering.
func (s *Service) lockMutation(id string) *sync.Mutex {
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	hash := offset64
	for i := 0; i < len(id); i++ {
		hash ^= uint64(id[i])
		hash *= prime64
	}
	lane := &s.mutationLanes[hash%uint64(len(s.mutationLanes))]
	lane.Lock()
	s.nodeMutationMu.Lock()
	return lane
}

func (s *Service) unlockMutation(lane *sync.Mutex) {
	s.nodeMutationMu.Unlock()
	lane.Unlock()
}

// NodeMutation is an opaque reservation for a node's next index mutation.
// Keep the reservation with deferred work, including work already drained from
// a queue. It belongs only to the service that issued it.
type NodeMutation struct {
	service    *Service
	nodeID     storage.NodeID
	generation uint64
}

// ReserveNodeMutation supersedes older work for this node. Reservation and
// application share one lock, so a newer reservation cannot slip between the
// generation check and the index write. No lock is retained by the caller.
func (s *Service) ReserveNodeMutation(nodeID storage.NodeID) NodeMutation {
	lane := s.lockMutation(string(nodeID))
	defer s.unlockMutation(lane)
	if s.pendingNodeMutations == nil {
		s.pendingNodeMutations = make(map[storage.NodeID]uint64)
	}
	generation := reserveMutation(&s.mutationReservations, s.pendingNodeMutations, nodeID)
	return NodeMutation{service: s, nodeID: nodeID, generation: generation}
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
		s.retryRefresh(RefreshRetry{Node: mutation}, err)
		return err
	}
	return s.IndexNodeWithMutation(node, mutation)
}

// nodeMutationRetired reports completion or discard of this reservation. A
// pending newer reservation does not retire its live predecessors, since its
// discard can restore one. Completion retires the whole predecessor chain.
func (s *Service) nodeMutationRetired(mutation NodeMutation) bool {
	lane := s.lockMutation(string(mutation.nodeID))
	defer s.unlockMutation(lane)
	return mutationRetired(&s.mutationReservations, s.pendingNodeMutations, mutation.nodeID, mutation.generation)
}

// applyReservedIndexMutation completes one node or relationship reservation
// while retaining the identity lane over its index application. pending names
// the service's map field so it is read only under nodeMutationMu.
func applyReservedIndexMutation[ID ~string](s, owner *Service, id ID, generation uint64, pending *map[ID]uint64, apply func() error) error {
	lane := s.lockMutation(string(id))
	if owner != s || generation == 0 || (*pending)[id] != generation {
		s.unlockMutation(lane)
		return nil
	}
	s.nodeMutationMu.Unlock()
	defer func() {
		s.nodeMutationMu.Lock()
		completeMutation(&s.mutationReservations, *pending, id)
		s.unlockMutation(lane)
	}()
	return apply()
}

func (s *Service) nodeMutationCurrentLocked(mutation NodeMutation) bool {
	return mutation.service == s && mutation.generation != 0 && s.pendingNodeMutations[mutation.nodeID] == mutation.generation
}

// DiscardNodeMutation retires work that could not be prepared for application.
// It cannot erase a newer reservation for the same node.
func (s *Service) DiscardNodeMutation(mutation NodeMutation) {
	lane := s.lockMutation(string(mutation.nodeID))
	defer s.unlockMutation(lane)
	if mutation.service == s {
		discardMutation(&s.mutationReservations, s.pendingNodeMutations, mutation.nodeID, mutation.generation)
	}
}

// EdgeMutation is NodeMutation for a relationship's vector and relationship
// search entries.
type EdgeMutation struct {
	service    *Service
	edgeID     storage.EdgeID
	generation uint64
}

// ReserveEdgeMutation supersedes older work for this relationship. Generations
// come from the same counter and registry as node reservations.
func (s *Service) ReserveEdgeMutation(edgeID storage.EdgeID) EdgeMutation {
	lane := s.lockMutation(string(edgeID))
	defer s.unlockMutation(lane)
	if s.pendingEdgeMutations == nil {
		s.pendingEdgeMutations = make(map[storage.EdgeID]uint64)
	}
	generation := reserveMutation(&s.mutationReservations, s.pendingEdgeMutations, edgeID)
	return EdgeMutation{service: s, edgeID: edgeID, generation: generation}
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
		s.retryRefresh(RefreshRetry{Edge: mutation}, err)
		return err
	}
	return s.indexEdgeWithMutation(edge, mutation)
}

// edgeMutationRetired is nodeMutationRetired for a relationship.
func (s *Service) edgeMutationRetired(mutation EdgeMutation) bool {
	lane := s.lockMutation(string(mutation.edgeID))
	defer s.unlockMutation(lane)
	return mutationRetired(&s.mutationReservations, s.pendingEdgeMutations, mutation.edgeID, mutation.generation)
}

func (s *Service) edgeMutationCurrentLocked(mutation EdgeMutation) bool {
	return mutation.service == s && mutation.generation != 0 && s.pendingEdgeMutations[mutation.edgeID] == mutation.generation
}

// current checks a relationship's reservation without retiring it: the
// relationship-search part of a write is installed ahead of its vector part,
// which retires the reservation.
func (mutation EdgeMutation) current() bool {
	s := mutation.service
	lane := s.lockMutation(string(mutation.edgeID))
	defer s.unlockMutation(lane)
	return s.edgeMutationCurrentLocked(mutation)
}

// updateRelationshipWithMutation applies the relationship-search part of a
// reserved relationship write: during a build it is ordered in the build
// sequence, installed only while the reservation is current; otherwise it is
// applied now, under the reservation lock, so no newer write interleaves.
func (s *Service) updateRelationshipWithMutation(write deferredIndexWrite, mutation EdgeMutation) {
	if s.relationshipSearch() == nil {
		return
	}
	write.valid = mutation.current
	if queued, _, _ := s.queueBuildWrite(write); queued {
		return
	}
	lane := s.lockMutation(string(mutation.edgeID))
	if !s.edgeMutationCurrentLocked(mutation) {
		s.unlockMutation(lane)
		return
	}
	s.nodeMutationMu.Unlock()
	defer lane.Unlock()
	s.updateRelationshipNow(write)
}

func (s *Service) discardEdgeMutation(mutation EdgeMutation) {
	lane := s.lockMutation(string(mutation.edgeID))
	defer s.unlockMutation(lane)
	if mutation.service == s {
		discardMutation(&s.mutationReservations, s.pendingEdgeMutations, mutation.edgeID, mutation.generation)
	}
}

// claimReservedIndexMutation is the build installer's shared node/relationship
// claim. Live writes queue behind the build, so completing this reservation and
// installing its write form the build's single ordered operation.
func claimReservedIndexMutation[ID ~string](s *Service, pending *map[ID]uint64, id ID, generation uint64) bool {
	lane := s.lockMutation(string(id))
	defer s.unlockMutation(lane)
	if generation == 0 || (*pending)[id] != generation {
		return false
	}
	completeMutation(&s.mutationReservations, *pending, id)
	return true
}
