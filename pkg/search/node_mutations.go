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

// mutationRegistry binds one identity kind to its service and pending generations.
// It is embedded in Service; pending is accessed only under nodeMutationMu.
type mutationRegistry[ID ~string] struct {
	service *Service
	pending map[ID]mutationReservation
}

// identityMutation is a comparable reservation naming one identity generation
// and the immutable vector-work origin admitted with it. Its registry keeps
// service and map ownership outside the copied token.
type identityMutation[ID ~string] struct {
	registry   *mutationRegistry[ID]
	id         ID
	generation uint64
	origin     uint64
}

// owner returns the issuing service, including for an empty reservation.
func (mutation identityMutation[ID]) owner() *Service {
	if mutation.registry == nil {
		return nil
	}
	return mutation.registry.service
}

// reserveIdentityMutation supersedes older work for id. Caller holds
// nodeMutationMu. A zero generation means newer-origin work is pending.
func reserveIdentityMutation[ID ~string](s *Service, registry *mutationRegistry[ID], id ID, origin uint64) identityMutation[ID] {
	if registry.pending == nil {
		*registry = mutationRegistry[ID]{service: s, pending: make(map[ID]mutationReservation)}
	}
	return identityMutation[ID]{
		registry:   registry,
		id:         id,
		generation: reserveMutation(&s.mutationReservations, registry.pending, id, origin),
		origin:     origin,
	}
}

// retire reports completion or discard of this reservation. A pending newer
// reservation does not retire its live predecessors, since its discard can
// restore one; completion retires the whole predecessor chain.
func (mutation identityMutation[ID]) retire() bool {
	s := mutation.owner()
	if s == nil {
		return true
	}
	lane := s.lockMutation(string(mutation.id))
	defer s.unlockMutation(lane)
	return mutation.retiredLocked(s)
}

// retiredLocked is retire for a caller holding the identity lane.
func (mutation identityMutation[ID]) retiredLocked(s *Service) bool {
	return mutationRetired(&s.mutationReservations, mutation.registry.pending, mutation.id, mutation.generation)
}

// current checks a reservation without retiring it.
func (mutation identityMutation[ID]) current() bool {
	s := mutation.owner()
	if s == nil {
		return false
	}
	lane := s.lockMutation(string(mutation.id))
	defer s.unlockMutation(lane)
	return mutation.held(s)
}

// held is current for a caller holding the identity lane.
func (mutation identityMutation[ID]) held(s *Service) bool {
	return mutation.owner() == s && mutation.generation != 0 && mutation.registry.pending[mutation.id].generation == mutation.generation
}

// discard retires work that could not be prepared for application. It cannot
// erase a newer reservation for the same identity.
func (mutation identityMutation[ID]) discard() {
	s := mutation.owner()
	if s == nil {
		return
	}
	lane := s.lockMutation(string(mutation.id))
	defer s.unlockMutation(lane)
	mutation.discardLocked(s)
}

// discardLocked is discard for a caller holding the identity lane.
func (mutation identityMutation[ID]) discardLocked(s *Service) {
	discardMutation(&s.mutationReservations, mutation.registry.pending, mutation.id, mutation.generation)
}

// complete retires this reservation and every older one for its identity.
// Caller holds nodeMutationMu.
func (mutation identityMutation[ID]) complete(s *Service) {
	completeMutation(&s.mutationReservations, mutation.registry.pending, mutation.id)
}

// NodeMutation is an opaque reservation for a node's next index mutation.
// Keep the reservation with deferred work, including work already drained from
// a queue. It belongs only to the service that issued it.
type NodeMutation struct {
	mutation identityMutation[storage.NodeID]
}

// ReserveNodeMutation supersedes older work for this node. Reservation and
// application share one lock, so a newer reservation cannot slip between the
// generation check and the index write. No lock is retained by the caller.
func (s *Service) ReserveNodeMutation(nodeID storage.NodeID, origin uint64) NodeMutation {
	lane := s.lockMutation(string(nodeID))
	defer s.unlockMutation(lane)
	return NodeMutation{mutation: reserveIdentityMutation(s, &s.nodeMutations, nodeID, origin)}
}

// RefreshNode makes the node's index entries match storage: indexed while it
// is stored and visible, removed otherwise. Live writers and readers that find
// a node missing use it rather than IndexNode or RemoveNode, so none of them
// decides from a read that a newer write may already have outdated.
func (s *Service) RefreshNode(nodeID storage.NodeID, origin uint64) error {
	if s == nil || nodeID == "" {
		return nil
	}
	return s.RefreshNodeWithMutation(s.ReserveNodeMutation(nodeID, origin))
}

// RefreshNodeWithMutation applies a refresh reserved earlier, such as one
// queued for a storage event. Storage is read after the reservation: a write
// this read misses commits later and reserves a newer refresh of its own,
// which makes this one a no-op.
func (s *Service) RefreshNodeWithMutation(mutation NodeMutation) error {
	if mutation.mutation.owner() != s || mutation.mutation.retire() {
		return nil
	}
	node, err := s.engine.GetNode(mutation.mutation.id)
	if errors.Is(err, storage.ErrNotFound) {
		return s.applyReservedNodeRemoval(mutation)
	}
	if err != nil {
		s.retryRefresh(RefreshRetry{Node: mutation}, err)
		return err
	}
	return s.applyReservedNodeIndex(node, mutation)
}

// DiscardNodeMutation retires work that could not be prepared for application.
// It cannot erase a newer reservation for the same node.
func (s *Service) DiscardNodeMutation(mutation NodeMutation) {
	mutation.mutation.discard()
}

// EdgeMutation is NodeMutation for a relationship's vector and relationship
// search entries.
type EdgeMutation struct {
	mutation identityMutation[storage.EdgeID]
}

// ReserveEdgeMutation supersedes older work for this relationship. Generations
// come from the same counter and registry as node reservations.
func (s *Service) ReserveEdgeMutation(edgeID storage.EdgeID, origin uint64) EdgeMutation {
	lane := s.lockMutation(string(edgeID))
	defer s.unlockMutation(lane)
	return EdgeMutation{mutation: reserveIdentityMutation(s, &s.edgeMutations, edgeID, origin)}
}

// RefreshEdge makes the relationship's index entries match storage, as
// RefreshNode does for nodes; a deleted relationship leaves them.
func (s *Service) RefreshEdge(edgeID storage.EdgeID, origin uint64) error {
	if s == nil || edgeID == "" {
		return nil
	}
	return s.RefreshEdgeWithMutation(s.ReserveEdgeMutation(edgeID, origin))
}

// RefreshEdgeWithMutation applies a relationship refresh reserved earlier;
// storage is read after the reservation, as in RefreshNodeWithMutation.
func (s *Service) RefreshEdgeWithMutation(mutation EdgeMutation) error {
	if mutation.mutation.owner() != s || mutation.mutation.retire() {
		return nil
	}
	edge, err := s.engine.GetEdge(mutation.mutation.id)
	if errors.Is(err, storage.ErrNotFound) {
		return s.applyReservedEdgeRemoval(mutation)
	}
	if err != nil {
		s.retryRefresh(RefreshRetry{Edge: mutation}, err)
		return err
	}
	return s.applyReservedEdgeIndex(edge, mutation)
}

// updateRelationshipWithMutation applies the relationship-search part of a
// reserved relationship write: during a build it is ordered in the build
// sequence, installed only while the reservation is current; otherwise it is
// applied now, under the reservation lock, so no newer write interleaves.
func (s *Service) updateRelationshipWithMutation(write deferredIndexWrite, mutation EdgeMutation) {
	if s.relationshipSearch() == nil {
		return
	}
	write.valid = mutation.mutation.current
	if queued, _, _ := s.queueBuildWrite(write); queued {
		return
	}
	lane := s.lockMutation(string(mutation.mutation.id))
	if !mutation.mutation.held(s) {
		s.unlockMutation(lane)
		return
	}
	s.nodeMutationMu.Unlock()
	defer lane.Unlock()
	s.updateRelationshipNow(write)
}

// claimReservedIndexMutation is the build installer's shared node/relationship
// claim. Live writes queue behind the build, so completing this reservation and
// installing its write form the build's single ordered operation.
func claimReservedIndexMutation[ID ~string](mutation identityMutation[ID]) bool {
	s := mutation.owner()
	if s == nil || mutation.generation == 0 {
		return false
	}
	lane := s.lockMutation(string(mutation.id))
	defer s.unlockMutation(lane)
	if mutation.registry.pending[mutation.id].generation != mutation.generation {
		return false
	}
	mutation.complete(s)
	return true
}

// applyReservedIndexMutation completes one node or relationship reservation
// while retaining the identity lane over its index application.
func applyReservedIndexMutation[ID ~string](mutation identityMutation[ID], apply func() error) error {
	s := mutation.owner()
	if s == nil || mutation.generation == 0 {
		return nil
	}
	lane := s.lockMutation(string(mutation.id))
	if mutation.registry.pending[mutation.id].generation != mutation.generation {
		s.unlockMutation(lane)
		return nil
	}
	s.nodeMutationMu.Unlock()
	defer func() {
		s.nodeMutationMu.Lock()
		mutation.complete(s)
		s.unlockMutation(lane)
	}()
	return apply()
}

// IsZero reports a reservation rejected because newer-origin work is pending.
func (mutation NodeMutation) IsZero() bool { return mutation.mutation.generation == 0 }

// IsZero reports a relationship reservation with no admitted work.
func (mutation EdgeMutation) IsZero() bool { return mutation.mutation.generation == 0 }

// VectorWorkOrigin returns the immutable origin transported by this reservation.
func (m NodeMutation) VectorWorkOrigin() uint64 { return m.mutation.origin }

// VectorWorkOrigin returns the immutable relationship-work origin.
func (m EdgeMutation) VectorWorkOrigin() uint64 { return m.mutation.origin }
