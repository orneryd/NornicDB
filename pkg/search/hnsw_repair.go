package search

import (
	"fmt"
	"slices"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// Graph repair on removal.
//
// A removed vector stays in the graph as a tombstone until a rebuild. Live
// nodes that linked to it would otherwise reach their former neighbours only
// through the tombstone, so a search would either lose them or have to walk
// tombstones. Instead, removal relinks every live node that linked to the
// tombstone (found through reverseEdges) to its own live neighbours plus the
// live nodes the tombstone led to, selected with the insert-time diversity
// heuristic and topped up with the closest pruned candidates. The tombstone's
// own links are then dropped, so no live node links to a tombstone and search
// never needs to pass through one.

// hnswCheckEdges, set by tests only, verifies after every Add and Remove that
// reverseEdges mirrors the forward links and that no live node links to a
// tombstone. A violation panics.
var hnswCheckEdges bool

func (v *visitedGenState) next() uint16 {
	v.cur++
	if v.cur == 0 {
		clear(v.gen)
		v.cur = 1
	}
	return v.cur
}

// reverseSlotLocked returns the reverseEdges slot of node at level, or false
// when the node has no such level.
func (h *HNSWIndex) reverseSlotLocked(node uint32, level int) (int, bool) {
	if !validHNSWIndex(node, len(h.neighborCountsOff)) || !validHNSWIndex(node, len(h.nodeLevel)) {
		return 0, false
	}
	if level < 0 || level > int(h.nodeLevel[node]) {
		return 0, false
	}
	slot := int(h.neighborCountsOff[node]) + level
	if slot < 0 || slot >= len(h.reverseEdges) {
		return 0, false
	}
	return slot, true
}

func (h *HNSWIndex) addReverseLocked(target uint32, level int, source uint32) {
	if slot, ok := h.reverseSlotLocked(target, level); ok {
		h.reverseEdges[slot] = append(h.reverseEdges[slot], source)
	}
}

func (h *HNSWIndex) removeReverseLocked(target uint32, level int, source uint32) {
	slot, ok := h.reverseSlotLocked(target, level)
	if !ok {
		return
	}
	sources := h.reverseEdges[slot]
	if i := slices.Index(sources, source); i >= 0 {
		last := len(sources) - 1
		sources[i] = sources[last]
		h.reverseEdges[slot] = sources[:last]
	}
}

// relinkReverseLocked updates reverseEdges for node's links at level changing
// from old to next. Both are read before the forward list is overwritten.
func (h *HNSWIndex) relinkReverseLocked(node uint32, level int, old, next []uint32) {
	for i, target := range old {
		if slices.Contains(old[:i], target) {
			continue
		}
		for delta := countOf(old, target) - countOf(next, target); delta > 0; delta-- {
			h.removeReverseLocked(target, level, node)
		}
	}
	for i, target := range next {
		if slices.Contains(next[:i], target) {
			continue
		}
		for delta := countOf(next, target) - countOf(old, target); delta > 0; delta-- {
			h.addReverseLocked(target, level, node)
		}
	}
}

func countOf(ids []uint32, id uint32) int {
	n := 0
	for _, x := range ids {
		if x == id {
			n++
		}
	}
	return n
}

// rebuildReverseEdgesLocked derives reverseEdges from the forward links.
func (h *HNSWIndex) rebuildReverseEdgesLocked() {
	h.reverseEdges = make([][]uint32, len(h.neighborCountsArena))
	for node := range h.nodeLevel {
		source := uint32(node)
		for level := 0; level <= int(h.nodeLevel[node]); level++ {
			neighbors, _ := h.neighborsAtLevelLocked(source, level)
			for _, target := range neighbors {
				h.addReverseLocked(target, level, source)
			}
		}
	}
}

func (h *HNSWIndex) repairRemovedLocked(removed uint32) {
	h.repairTombstonesLocked([]uint32{removed})
}

// repairTombstonesLocked relinks the live nodes around tombs and then drops
// the tombs' own links. Links between tombs are followed while collecting
// candidates, so a chain of tombstones (a graph saved before repair existed)
// is bridged as a whole.
func (h *HNSWIndex) repairTombstonesLocked(tombs []uint32) {
	topLevel := -1
	for _, tomb := range tombs {
		if validHNSWIndex(tomb, len(h.nodeLevel)) {
			topLevel = max(topLevel, int(h.nodeLevel[tomb]))
		}
	}
	visited, _ := h.acquireVisitedLocked()
	defer h.visitedPool.Put(visited)

	var affected, exits, watch, candidates, selected []uint32
	for level := 0; level <= topLevel; level++ {
		// Live nodes linking to a tomb, and live nodes the tombs lead to.
		gen := visited.next()
		affected, exits = affected[:0], exits[:0]
		for _, tomb := range tombs {
			if slot, ok := h.reverseSlotLocked(tomb, level); ok {
				for _, source := range h.reverseEdges[slot] {
					if !h.deleted[source] && visited.gen[source] != gen {
						visited.gen[source] = gen
						affected = append(affected, source)
					}
				}
			}
		}
		visited.next()
		for _, tomb := range tombs {
			exits = h.liveThroughTombstonesLocked(tomb, level, visited, exits)
		}

		// Nodes that may have lost their last live link: the tombs' exits and
		// nodes a repaired list no longer holds.
		watch = append(watch[:0], exits...)
		capacity := h.neighborCapacity(level)
		for _, node := range affected {
			gen = visited.next()
			visited.gen[node] = gen
			candidates = h.liveThroughTombstonesLocked(node, level, visited, candidates[:0])
			if query := h.vectorAtLocked(node); len(query) == h.dimensions {
				selected = h.selectNeighbors(query, candidates, capacity, selected[:0], true)
			} else {
				selected = append(selected[:0], candidates[:min(len(candidates), capacity)]...)
			}
			previous, _ := h.neighborsAtLevelLocked(node, level)
			for _, neighbor := range previous {
				if validHNSWIndex(neighbor, len(h.nodeLevel)) && !h.deleted[neighbor] && !slices.Contains(selected, neighbor) {
					watch = append(watch, neighbor)
				}
			}
			h.setNeighborsAtLevelLocked(node, level, selected)
		}
		h.keepLinkedLocked(level, watch, affected, exits)
	}

	for _, tomb := range tombs {
		if !validHNSWIndex(tomb, len(h.nodeLevel)) {
			continue
		}
		for level := 0; level <= int(h.nodeLevel[tomb]); level++ {
			h.setNeighborsAtLevelLocked(tomb, level, nil)
		}
	}
}

// liveThroughTombstonesLocked appends to out the live nodes linked from node
// at level, following links through tombstones. visited carries the current
// generation; nodes already marked are skipped.
func (h *HNSWIndex) liveThroughTombstonesLocked(node uint32, level int, visited *visitedGenState, out []uint32) []uint32 {
	gen := visited.cur
	var stackBuf [16]uint32
	stack := append(stackBuf[:0], node)
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		neighbors, _ := h.neighborsAtLevelLocked(current, level)
		for _, neighbor := range neighbors {
			if !validHNSWIndex(neighbor, len(h.nodeLevel)) || visited.gen[neighbor] == gen {
				continue
			}
			visited.gen[neighbor] = gen
			if h.deleted[neighbor] {
				stack = append(stack, neighbor)
				continue
			}
			out = append(out, neighbor)
		}
	}
	return out
}

func (h *HNSWIndex) hasLiveInLinkLocked(node uint32, level int) bool {
	slot, ok := h.reverseSlotLocked(node, level)
	if !ok {
		return false
	}
	for _, source := range h.reverseEdges[slot] {
		if !h.deleted[source] && source != node {
			return true
		}
	}
	return false
}

// keepLinkedLocked links every queued live node that has no live link into it
// at level from its closest host that keeps the link: a node of the pools or
// one of its own neighbours. A node that link pruned away is queued in turn.
func (h *HNSWIndex) keepLinkedLocked(level int, queue []uint32, pools ...[]uint32) {
	for budget := 4*len(queue) + 16; len(queue) > 0 && budget > 0; budget-- {
		node := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if h.deleted[node] || h.hasLiveInLinkLocked(node, level) {
			continue
		}
		queue = h.attachLocked(node, level, queue, pools...)
	}
}

// attachLocked links node from its closest host that keeps the link after
// pruning, and appends to queue the live nodes that pruning dropped.
func (h *HNSWIndex) attachLocked(node uint32, level int, queue []uint32, pools ...[]uint32) []uint32 {
	query := h.vectorAtLocked(node)
	if len(query) != h.dimensions {
		return queue
	}
	own, _ := h.neighborsAtLevelLocked(node, level)
	var hosts []hnswDistItem
	for _, pool := range append(pools, own) {
		for _, host := range pool {
			if host == node || !validHNSWIndex(host, len(h.nodeLevel)) || h.deleted[host] || int(h.nodeLevel[host]) < level {
				continue
			}
			if hostVector := h.vectorAtLocked(host); len(hostVector) == h.dimensions {
				hosts = append(hosts, hnswDistItem{id: host, dist: 1 - vector.DotProductSIMD(query, hostVector)})
			}
		}
	}
	slices.SortFunc(hosts, func(a, b hnswDistItem) int {
		if a.dist != b.dist {
			if a.dist < b.dist {
				return -1
			}
			return 1
		}
		return int(a.id) - int(b.id)
	})
	hosts = slices.CompactFunc(hosts, func(a, b hnswDistItem) bool { return a.id == b.id })
	for _, host := range hosts {
		before, _ := h.neighborsAtLevelLocked(host.id, level)
		before = slices.Clone(before)
		h.insertNeighborAtLevelLocked(host.id, level, node)
		after, _ := h.neighborsAtLevelLocked(host.id, level)
		if !slices.Contains(after, node) {
			continue
		}
		for _, dropped := range before {
			if !h.deleted[dropped] && !slices.Contains(after, dropped) {
				queue = append(queue, dropped)
			}
		}
		return queue
	}
	return queue
}

func (h *HNSWIndex) debugCheckEdgesLocked() {
	if !hnswCheckEdges {
		return
	}
	if err := h.checkEdgesLocked(); err != nil {
		panic(err)
	}
}

// checkEdgesLocked reports the first mismatch between the forward links and
// reverseEdges, or a live node linking to a tombstone.
func (h *HNSWIndex) checkEdgesLocked() error {
	if len(h.reverseEdges) != len(h.neighborCountsArena) {
		return fmt.Errorf("hnsw: %d reverse slots for %d link lists", len(h.reverseEdges), len(h.neighborCountsArena))
	}
	balance := make(map[[2]uint32]int)
	for node := range h.nodeLevel {
		source := uint32(node)
		for level := 0; level <= int(h.nodeLevel[node]); level++ {
			neighbors, _ := h.neighborsAtLevelLocked(source, level)
			for _, target := range neighbors {
				if validHNSWIndex(target, len(h.deleted)) && h.deleted[target] {
					return fmt.Errorf("hnsw: node %d (deleted=%v) links tombstone %d at level %d", source, h.deleted[source], target, level)
				}
				if slot, ok := h.reverseSlotLocked(target, level); ok {
					balance[[2]uint32{uint32(slot), source}]++
				}
			}
		}
	}
	for slot, sources := range h.reverseEdges {
		for _, source := range sources {
			balance[[2]uint32{uint32(slot), source}]--
		}
	}
	for key, n := range balance {
		if n != 0 {
			return fmt.Errorf("hnsw: reverse slot %d has %d unmatched links from node %d", key[0], -n, key[1])
		}
	}
	return nil
}
