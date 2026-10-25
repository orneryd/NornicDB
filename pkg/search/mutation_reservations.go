package search

// The pending entry owns both identities. An isolated reservation needs no
// link record; overlapping predecessors retain their origins in the links so
// discarding newer work restores the exact original reservation.
type mutationReservation struct{ generation, origin uint64 }
type mutationReservations struct {
	next  uint64
	links map[uint64]mutationLink
}
type mutationLink struct{ previous, next, origin uint64 }

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

// All admission/completion operations run under Service.nodeMutationMu.
func reserveMutation[K comparable](r *mutationReservations, pending map[K]mutationReservation, id K, origin uint64) uint64 {
	previous := pending[id]
	if previous.origin > origin {
		return 0
	}
	r.next++
	generation := r.next
	if previous.generation != 0 {
		link := r.links[previous.generation]
		link.next = generation
		link.origin = previous.origin
		r.setLink(previous.generation, link)
		r.setLink(generation, mutationLink{previous: previous.generation, origin: origin})
	}
	pending[id] = mutationReservation{generation, origin}
	return generation
}

func discardMutation[K comparable](r *mutationReservations, pending map[K]mutationReservation, id K, generation uint64) {
	if generation == 0 {
		return
	}
	link, linked := r.links[generation]
	if pending[id].generation != generation && !linked {
		return
	}
	var previousOrigin uint64
	if link.previous != 0 {
		previous := r.links[link.previous]
		previousOrigin = previous.origin
		previous.next = link.next
		r.setLink(link.previous, previous)
	}
	if link.next != 0 {
		next := r.links[link.next]
		next.previous = link.previous
		r.setLink(link.next, next)
	} else if link.previous != 0 {
		pending[id] = mutationReservation{link.previous, previousOrigin}
	} else {
		delete(pending, id)
	}
	delete(r.links, generation)
}

func completeMutation[K comparable](r *mutationReservations, pending map[K]mutationReservation, id K) {
	for generation := pending[id].generation; generation != 0; {
		link := r.links[generation]
		delete(r.links, generation)
		generation = link.previous
	}
	delete(pending, id)
}

func mutationRetired[K comparable](r *mutationReservations, pending map[K]mutationReservation, id K, generation uint64) bool {
	if generation != 0 && pending[id].generation == generation {
		return false
	}
	_, live := r.links[generation]
	return !live
}
