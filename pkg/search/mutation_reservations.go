package search

// mutationReservations retains links only while reservations overlap. A single
// pending reservation needs only the existing ID-to-generation entry.
type mutationReservations struct {
	next  uint64
	links map[uint64]mutationLink
}

type mutationLink struct {
	previous uint64
	next     uint64
}

// All reservation operations run under Service.nodeMutationMu.
func (r *mutationReservations) setLink(generation uint64, link mutationLink) {
	if link.previous == 0 && link.next == 0 {
		delete(r.links, generation)
		return
	}
	if r.links == nil {
		r.links = make(map[uint64]mutationLink)
	}
	r.links[generation] = link
}

func reserveMutation[K comparable](r *mutationReservations, pending map[K]uint64, id K) uint64 {
	r.next++
	generation := r.next
	if previous := pending[id]; previous != 0 {
		link := r.links[previous]
		link.next = generation
		r.setLink(previous, link)
		r.setLink(generation, mutationLink{previous: previous})
	}
	pending[id] = generation
	return generation
}

// Discard unlinks even a noncurrent reservation, so it can never be restored
// by a later discard. Isolated survivors need no link record.
func discardMutation[K comparable](r *mutationReservations, pending map[K]uint64, id K, generation uint64) {
	if generation == 0 {
		return
	}
	link, linked := r.links[generation]
	if pending[id] != generation && !linked {
		return
	}
	if link.previous != 0 {
		previous := r.links[link.previous]
		previous.next = link.next
		r.setLink(link.previous, previous)
	}
	if link.next != 0 {
		next := r.links[link.next]
		next.previous = link.previous
		r.setLink(link.next, next)
	} else if link.previous != 0 {
		pending[id] = link.previous
	} else {
		delete(pending, id)
	}
	delete(r.links, generation)
}

// Completing the current reservation retires every older one. No completed
// generation or discarded reservation leaves a tombstone.
func completeMutation[K comparable](r *mutationReservations, pending map[K]uint64, id K) {
	for generation := pending[id]; generation != 0; {
		link := r.links[generation]
		delete(r.links, generation)
		generation = link.previous
	}
	delete(pending, id)
}

func mutationRetired[K comparable](r *mutationReservations, pending map[K]uint64, id K, generation uint64) bool {
	if generation != 0 && pending[id] == generation {
		return false
	}
	_, live := r.links[generation]
	return !live
}
