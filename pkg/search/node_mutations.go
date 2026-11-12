package search

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	pending map[ID]uint64
}

// identityMutation is a comparable reservation naming one identity generation.
// Its registry keeps service and map ownership outside the copied token.
type identityMutation[ID ~string] struct {
	registry   *mutationRegistry[ID]
	id         ID
	generation uint64
}

// owner returns the issuing service, including for an empty reservation.
func (mutation identityMutation[ID]) owner() *Service {
	if mutation.registry == nil {
		return nil
	}
	return mutation.registry.service
}

// reserveIdentityMutation supersedes older work for id. Caller holds nodeMutationMu.
func reserveIdentityMutation[ID ~string](s *Service, registry *mutationRegistry[ID], id ID) identityMutation[ID] {
	if registry.pending == nil {
		*registry = mutationRegistry[ID]{service: s, pending: make(map[ID]uint64)}
	}
	return identityMutation[ID]{
		registry:   registry,
		id:         id,
		generation: reserveMutation(&s.mutationReservations, registry.pending, id),
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
	return mutation.owner() == s && mutation.generation != 0 && mutation.registry.pending[mutation.id] == mutation.generation
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
func (s *Service) ReserveNodeMutation(nodeID storage.NodeID) NodeMutation {
	lane := s.lockMutation(string(nodeID))
	defer s.unlockMutation(lane)
	return NodeMutation{mutation: reserveIdentityMutation(s, &s.nodeMutations, nodeID)}
}

// RefreshNode makes the node's index entries match storage: indexed while it
// is stored and visible, removed otherwise. Live writers and readers that find
// a node missing use it rather than IndexNode or RemoveNode, so none of them
// decides from a read that a newer write may already have outdated.
func (s *Service) RefreshNode(ctx context.Context, nodeID storage.NodeID) error {
	if s == nil || nodeID == "" {
		return nil
	}
	return s.RefreshNodeWithMutation(ctx, s.ReserveNodeMutation(nodeID))
}

// RefreshNodeWithMutation applies a refresh reserved earlier, such as one
// queued for a storage event. Storage is read after the reservation: a write
// this read misses commits later and reserves a newer refresh of its own,
// which makes this one a no-op.
//
// The refresh installs the stored node entry by entry (installStored): an
// entry a budget or a dimension refuses is decided on its own, and the error
// reports the refusal. A read that fails keeps the reservation for the retry
// owner; the node's entries lag until it lands.
func (s *Service) RefreshNodeWithMutation(ctx context.Context, mutation NodeMutation) error {
	if mutation.mutation.owner() != s || mutation.mutation.retire() {
		return nil
	}
	node, err := s.readNodeImage(ctx, mutation.mutation.id, fullNodeImage)
	if errors.Is(err, storage.ErrNotFound) {
		return s.applyReservedNodeRemoval(mutation)
	}
	if err != nil {
		s.retryRefresh(RefreshRetry{Node: mutation}, err)
		return err
	}
	return s.applyReservedNodeIndex(ctx, node, mutation)
}

// entryRefused reports whether err is only the refusal of entries a stored
// node or relationship could not be given (an index memory budget, an
// embedding of other dimensions): the refresh installed everything else, so
// it is final, not a failure to retry.
func entryRefused(err error) bool {
	return errors.Is(err, ErrIndexMemoryBudgetExceeded) || errors.Is(err, ErrDimensionMismatch)
}

// withoutRefusals is err without the entry refusals it reports.
func withoutRefusals(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var kept []error
		for _, part := range joined.Unwrap() {
			if part = withoutRefusals(part); part != nil {
				kept = append(kept, part)
			}
		}
		return errors.Join(kept...)
	}
	if entryRefused(err) {
		return nil
	}
	return err
}

// refreshAttempts bounds how often catch-up and population apply a refresh
// whose failure is transient (a read that failed) before reporting it.
const refreshAttempts = 3

// refreshUntilApplied applies refresh to id until it lands, is refused
// (final), or has failed refreshAttempts times; it returns the last outcome.
func refreshUntilApplied[ID ~string](ctx context.Context, refresh func(context.Context, ID) error, id ID) error {
	var err error
	for range refreshAttempts {
		if err = refresh(ctx, id); err == nil || entryRefused(err) {
			return err
		}
	}
	return err
}

// CatchUpPendingRefreshes applies now a refresh of every node and relationship
// that has one reserved and not yet applied, such as a write whose refresh is
// still waiting in the debounced event queue; each queued refresh, superseded,
// does nothing when it runs. A refresh whose read fails is retried a bounded
// number of times; it returns those that still failed. An entry a refresh
// refused is not a failure: the refresh installed everything else.
func (s *Service) CatchUpPendingRefreshes(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.nodeMutationMu.Lock()
	nodes := slices.Collect(maps.Keys(s.nodeMutations.pending))
	edges := slices.Collect(maps.Keys(s.edgeMutations.pending))
	s.nodeMutationMu.Unlock()
	return errors.Join(catchUp(ctx, "node", nodes, s.RefreshNode), catchUp(ctx, "relationship", edges, s.RefreshEdge))
}

// catchUp applies refresh to every identity of the kind in ids; an entry a
// refresh refused is not a failure.
func catchUp[ID ~string](ctx context.Context, kind string, ids []ID, refresh func(context.Context, ID) error) error {
	return eachFailure(kind, ids, func(id ID) error { return withoutRefusals(refreshUntilApplied(ctx, refresh, id)) })
}

// eachFailure applies apply to every identity of the kind (a node, a
// relationship) and joins the failures, each naming its identity.
func eachFailure[ID ~string](kind string, ids []ID, apply func(ID) error) error {
	var failures error
	for _, id := range ids {
		if err := apply(id); err != nil {
			failures = errors.Join(failures, fmt.Errorf("%s %s: %w", kind, id, err))
		}
	}
	return failures
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
func (s *Service) ReserveEdgeMutation(edgeID storage.EdgeID) EdgeMutation {
	lane := s.lockMutation(string(edgeID))
	defer s.unlockMutation(lane)
	return EdgeMutation{mutation: reserveIdentityMutation(s, &s.edgeMutations, edgeID)}
}

// RefreshEdge makes the relationship's index entries match storage, as
// RefreshNode does for nodes; a deleted relationship leaves them.
func (s *Service) RefreshEdge(ctx context.Context, edgeID storage.EdgeID) error {
	if s == nil || edgeID == "" {
		return nil
	}
	return s.RefreshEdgeWithMutation(ctx, s.ReserveEdgeMutation(edgeID))
}

// RefreshEdgeWithMutation applies a relationship refresh reserved earlier;
// storage is read after the reservation, as in RefreshNodeWithMutation.
func (s *Service) RefreshEdgeWithMutation(ctx context.Context, mutation EdgeMutation) error {
	if mutation.mutation.owner() != s || mutation.mutation.retire() {
		return nil
	}
	edge, err := s.readEdgeImage(ctx, mutation.mutation.id)
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
	if queued, _, _ := s.queueBuildWrite(s.LifetimeContext(), write); queued {
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
	if mutation.registry.pending[mutation.id] != mutation.generation {
		return false
	}
	mutation.complete(s)
	return true
}

// applyReservedIndexMutation completes one node or relationship reservation
// while retaining the identity lane over its index application.
func applyReservedIndexMutation[ID ~string](mutation identityMutation[ID], apply func() error) (err error) {
	s := mutation.owner()
	if s == nil || mutation.generation == 0 {
		return nil
	}
	lane := s.lockMutation(string(mutation.id))
	if mutation.registry.pending[mutation.id] != mutation.generation {
		s.unlockMutation(lane)
		return nil
	}
	s.nodeMutationMu.Unlock()
	defer func() {
		s.nodeMutationMu.Lock()
		// A transient installation failure still belongs to its retry owner.
		// Budget/dimension refusals are final outcomes, not pending work.
		if withoutRefusals(err) == nil {
			mutation.complete(s)
		}
		s.unlockMutation(lane)
	}()
	return apply()
}
