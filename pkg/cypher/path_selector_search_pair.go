package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// pathSearchBack is a step of the walk search from the end: from the
// product key (node) along edge by move.
type pathSearchBack struct {
	edge *storage.Edge
	move int
	key  pathSearchKey
	node *storage.Node
}

// backSteps lists the steps into the product of node and slot from the
// products before it. The slot a step leaves is checked by its own
// predicates only, so a walk of such steps from a product the search from
// the start reached is a walk of the pattern.
func (s *pathSearch) backSteps(node *storage.Node, slot int) []pathSearchBack {
	var steps []pathSearchBack
	for _, index := range s.a.into[slot] {
		if s.ctx.Err() != nil {
			return nil
		}
		move := s.a.moves[index]
		transition := &s.a.slots[move.from].transitions[move.transition]
		if !s.chainHolds(s.a.chains[transition.to][move.chain], node) {
			continue
		}
		direction := reverseDirection(transition.direction)
		for _, edge := range s.relationships(node.ID, direction) {
			if s.ctx.Err() != nil {
				return nil
			}
			if !s.transitionHolds(transition, edge) {
				continue
			}
			before, ok := s.e.loadTraversalEndpointNode(s.traversal, otherEnd(edge, node.ID))
			if !ok || !s.slotHolds(move.from, before) {
				continue
			}
			steps = append(steps, pathSearchBack{edge: edge, move: index, key: pathSearchKey{node: before.ID, slot: int32(move.from)}, node: before})
		}
	}
	return steps
}

// pathSearchWalks is the walk search from both ends of a pair: the length
// of the shortest walk to each product from the start (forward) and from
// each product to the end (backward), level by level.
type pathSearchWalks struct {
	forward  map[int32]int
	preds    map[int32][]pathSearchHop
	frontier []int32
	backward map[pathSearchKey]int
	nodes    map[pathSearchKey]*storage.Node
	succs    map[pathSearchKey][]pathSearchBack
	back     []pathSearchKey
	// forwardRadius and backwardRadius are how far each side has
	// searched; shortest is the length of the shortest walk (-1: none
	// found yet).
	forwardRadius, backwardRadius, shortest int
}

// expandForward adds the next level of the walk search from the start.
func (s *pathSearch) expandForward(w *pathSearchWalks) {
	if s.ctx.Err() != nil {
		return
	}
	var next []int32
	for _, p := range w.frontier {
		if s.ctx.Err() != nil {
			return
		}
		s.expand(p)
		for stepIndex, step := range s.products[p].steps {
			if s.ctx.Err() != nil {
				return
			}
			distance, seen := w.forward[step.to]
			if !seen {
				distance = w.forwardRadius + 1
				w.forward[step.to] = distance
				s.retain(searchEntryBytes + 8)
				next = append(next, step.to)
				s.meet(w, step.to)
			}
			if distance == w.forwardRadius+1 {
				w.preds[step.to] = append(w.preds[step.to], pathSearchHop{product: p, step: int32(stepIndex)})
				s.retain(16)
			}
		}
	}
	w.frontier = next
	w.forwardRadius++
}

// expandBackward adds the next level of the walk search from the end.
func (s *pathSearch) expandBackward(w *pathSearchWalks) {
	if s.ctx.Err() != nil {
		return
	}
	var next []pathSearchKey
	for _, key := range w.back {
		if s.ctx.Err() != nil {
			return
		}
		for _, step := range s.backSteps(w.nodes[key], int(key.slot)) {
			if s.ctx.Err() != nil {
				return
			}
			distance, seen := w.backward[step.key]
			if !seen {
				distance = w.backwardRadius + 1
				w.backward[step.key] = distance
				w.nodes[step.key] = step.node
				s.retain(2*searchEntryBytes + 16)
				next = append(next, step.key)
				if p, exists := s.index[step.key]; exists {
					s.meet(w, p)
				}
			}
			if distance == w.backwardRadius+1 {
				w.succs[step.key] = append(w.succs[step.key], pathSearchBack{edge: step.edge, move: step.move, key: key, node: w.nodes[key]})
			}
		}
	}
	w.back = next
	w.backwardRadius++
}

// meet records a walk through product p, which the walk search from the
// start reached (every product is a step it took), when the search from
// the end reached it too.
func (s *pathSearch) meet(w *pathSearchWalks, p int32) {
	forward := w.forward[p]
	product := s.products[p]
	backward, met := w.backward[pathSearchKey{node: product.node.ID, slot: int32(product.slot)}]
	if met && (w.shortest < 0 || forward+backward < w.shortest) {
		w.shortest = forward + backward
	}
}

// pair searches the paths from source to target. A uniform pattern's
// shortest paths come from the walk search over node ids (uniformPair).
// Otherwise a walk search from both ends finds the length of the shortest
// walk, or that there is none; when
// one path (or one group) is wanted and a shortest walk is a path, the
// shortest walks that are paths are the answer. Otherwise the search from
// source runs toward target, skipping the products that can't reach it in
// the length left.
func (s *pathSearch) pair(source, target *storage.Node, count int, groups, direct bool, bind func(pathSearchPath) (pipelineRow, bool)) []pipelineRow {
	if s.ctx.Err() != nil {
		return nil
	}
	s.target = target
	if direct && count == 1 {
		if rows, ok := s.uniformPair(source, target, groups, bind); ok {
			return rows
		}
	}
	if len(s.a.counters) > 0 {
		return s.run(source, count, groups, bind)
	}
	if s.ctx.Err() != nil {
		return nil
	}
	s.begin(source)
	if s.ctx.Err() != nil {
		return nil
	}
	w := &pathSearchWalks{forward: map[int32]int{}, preds: map[int32][]pathSearchHop{}, backward: map[pathSearchKey]int{},
		nodes: map[pathSearchKey]*storage.Node{}, succs: map[pathSearchKey][]pathSearchBack{}, shortest: -1}
	for _, entry := range s.levels[0].entries {
		w.forward[entry.product] = 0
		w.frontier = append(w.frontier, entry.product)
		s.retain(searchEntryBytes + 8)
	}
	if end := (pathSearchKey{node: target.ID, slot: int32(s.a.final)}); s.slotHolds(s.a.final, target) {
		w.backward[end] = 0
		w.nodes[end] = target
		w.back = append(w.back, end)
		if p, exists := s.index[end]; exists {
			s.meet(w, p)
		}
	}
	for w.shortest < 0 && len(w.frontier) > 0 && len(w.back) > 0 && s.ctx.Err() == nil &&
		(s.maxLength < 0 || w.forwardRadius+w.backwardRadius < s.maxLength) {
		if len(w.frontier) <= len(w.back) {
			s.expandForward(w)
		} else {
			s.expandBackward(w)
		}
	}
	if w.shortest < 0 || s.maxLength >= 0 && w.shortest > s.maxLength {
		return nil
	}
	if direct && count == 1 {
		if rows := s.shortestWalkPaths(w, groups, bind); len(rows) > 0 {
			return rows
		}
	}
	for len(w.back) > 0 && s.ctx.Err() == nil && (s.maxLength < 0 || w.backwardRadius < s.maxLength) {
		s.expandBackward(w)
	}
	s.distance = w.backward
	return s.run(source, count, groups, bind)
}

// shortestWalkPaths returns the rows of the shortest walks of a pair that
// are paths: the first one, or all of them for groups. Each shortest walk
// is a shortest walk to its product at one position (both sides reached
// it), then a shortest walk from it to the end.
func (s *pathSearch) shortestWalkPaths(w *pathSearchWalks, groups bool, bind func(pathSearchPath) (pipelineRow, bool)) []pipelineRow {
	position := max(0, w.shortest-w.backwardRadius)
	var rows []pipelineRow
	used := map[storage.EdgeID]struct{}{}
	stop := false
	take := func(id storage.EdgeID) bool {
		if s.repeatable {
			return true
		}
		if _, in := used[id]; in {
			return false
		}
		used[id] = struct{}{}
		return true
	}
	release := func(id storage.EdgeID) {
		if !s.repeatable {
			delete(used, id)
		}
	}
	var back func(key pathSearchKey, path pathSearchPath)
	back = func(key pathSearchKey, path pathSearchPath) {
		if s.ctx.Err() != nil {
			stop = true
			return
		}
		if w.backward[key] == 0 {
			if row, ok := bind(path); ok {
				rows = append(rows, row)
				stop = !groups
			}
			return
		}
		for _, step := range w.succs[key] {
			if stop || !take(step.edge.ID) {
				continue
			}
			next := path
			next.steps = append(append([]pathSearchPathStep(nil), path.steps...), pathSearchPathStep{edge: step.edge, move: step.move, node: step.node})
			back(step.key, next)
			release(step.edge.ID)
		}
	}
	var forward func(middle, p int32, hops []pathSearchHop)
	forward = func(middle, p int32, hops []pathSearchHop) {
		if s.ctx.Err() != nil {
			stop = true
			return
		}
		if w.forward[p] == 0 {
			path := s.rootPath(p)
			if len(hops) > 0 {
				path = s.pathOf(hops)
			}
			product := s.products[middle]
			back(pathSearchKey{node: product.node.ID, slot: int32(product.slot)}, path)
			return
		}
		for _, hop := range w.preds[p] {
			id := s.products[hop.product].steps[hop.step].edge.ID
			if stop || !take(id) {
				continue
			}
			forward(middle, hop.product, append(hops, hop))
			release(id)
		}
	}
	for p := range s.products {
		distance, reached := w.forward[int32(p)]
		if !reached || distance != position || stop {
			continue
		}
		product := s.products[p]
		if backward, met := w.backward[pathSearchKey{node: product.node.ID, slot: int32(product.slot)}]; met && backward == w.shortest-position {
			forward(int32(p), int32(p), nil)
		}
	}
	return rows
}
