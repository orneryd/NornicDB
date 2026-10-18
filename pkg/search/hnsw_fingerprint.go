package search

import "math"

// Coordinate fingerprints let a persisted cluster graph tell which of its
// members were inserted with coordinates the canonical store no longer
// holds. A graph that tracks them (cluster graphs) records, per internal ID,
// a hash of the vector each node was inserted with; the hash is saved with
// the graph, so a restore can refresh members whose vector changed after the
// save even when they stay in the same cluster (their quantized codes and
// links describe the old position).

// vectorFingerprint is a non-zero hash of vec's coordinates; zero marks an
// unknown fingerprint.
func vectorFingerprint(vec []float32) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	h := uint64(offset)
	for _, v := range vec {
		bits := math.Float32bits(v)
		for i := 0; i < 4; i++ {
			h ^= uint64(bits & 0xff)
			h *= prime
			bits >>= 8
		}
	}
	return h | 1
}

// trackFingerprints makes the graph record the coordinates of every node
// inserted from now on; nodes it already holds have unknown fingerprints.
func (h *HNSWIndex) trackFingerprints() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fingerprints == nil {
		h.fingerprints = make([]uint64, len(h.internalToID))
	}
}

// recordFingerprintLocked records the coordinates node internalID was
// inserted (or updated in place) with; callers hold h.mu.
func (h *HNSWIndex) recordFingerprintLocked(internalID uint32, vec []float32) {
	if h.fingerprints == nil {
		return
	}
	for uint32(len(h.fingerprints)) <= internalID {
		h.fingerprints = append(h.fingerprints, 0)
	}
	h.fingerprints[internalID] = vectorFingerprint(vec)
}

// staleMembers lists the graph's live members whose recorded coordinates
// differ from lookup's (or are unknown), sorted by internal ID.
func (h *HNSWIndex) staleMembers(lookup VectorLookup) []string {
	h.mu.RLock()
	type member struct {
		id          string
		fingerprint uint64
	}
	members := make([]member, 0, h.liveCount)
	for internal, id := range h.internalToID {
		if internal < len(h.deleted) && h.deleted[internal] {
			continue
		}
		var fp uint64
		if internal < len(h.fingerprints) {
			fp = h.fingerprints[internal]
		}
		members = append(members, member{id: id, fingerprint: fp})
	}
	h.mu.RUnlock()
	var stale []string
	for _, m := range members {
		vec, ok := lookup(m.id)
		if !ok || len(vec) == 0 {
			continue
		}
		if m.fingerprint == 0 || m.fingerprint != vectorFingerprint(vec) {
			stale = append(stale, m.id)
		}
	}
	return stale
}
