package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// The search between two bound ends of a uniform pattern (#1212): one
// whose every relationship pattern is the same unconstrained one and whose
// inner nodes are unconstrained, as shortestPath((a)-[:T*]-(b)) is. Any
// walk of such relationships between the ends is a walk of the pattern when
// its length is one the pattern allows, and a shortest walk between two
// distinct nodes repeats no node, so it is a path. The walks are searched
// from both ends over node ids, with the relationship each node was reached
// by; the nodes of the paths found are read in one call, and each path is
// read through the automaton and bound as any other (bindSelectedPath).

// pathSearchWalkHop is how a walk from one end reached a node: from the node
// before it, along edge.
type pathSearchWalkHop struct {
	from storage.NodeID
	edge *storage.Edge
}

// pathSearchWalkReach is a node a walk search reached: the length of the
// shortest walk to it and the last hop of one such walk.
type pathSearchWalkReach struct {
	depth int
	hop   pathSearchWalkHop
}

// pathSearchWalkSide is the walk search from one end: the nodes reached,
// the last hops of their other shortest walks (when every shortest path is
// wanted), and the nodes reached last.
type pathSearchWalkSide struct {
	direction string
	reached   map[storage.NodeID]pathSearchWalkReach
	more      map[storage.NodeID][]pathSearchWalkHop
	frontier  []storage.NodeID
	next      []storage.NodeID
	radius    int
}

// uniformTransition returns the transition every relationship pattern of
// a is, when a is uniform: every transition is the same relationship
// pattern without predicates or a variable an earlier clause may bind,
// every slot but the first and the final takes any node, and a chain from
// the first slot leads to a transition. start is that chain.
func uniformTransition(a *pathSearchAutomaton) (uniform *pathSearchTransition, start int) {
	// Multiple repetitions can decompose the same walk in different ways.
	// The general automaton search already enumerates those bindings.
	if a.repetitions > 1 {
		return nil, -1
	}
	start = -1
	for index, chain := range a.chains[a.initial] {
		if len(a.slots[chain.end].transitions) > 0 {
			start = index
			break
		}
	}
	if start < 0 {
		return nil, -1
	}
	for index := range a.slots {
		slot := &a.slots[index]
		if index != a.initial && index != a.final &&
			(len(slot.labels) > 0 || len(slot.predicates) > 0 || slot.variable != "" && !slot.list) {
			return nil, -1
		}
		for t := range slot.transitions {
			transition := &slot.transitions[t]
			if len(transition.predicates) > 0 || transition.variable != "" && !transition.list {
				return nil, -1
			}
			if uniform == nil {
				uniform = transition
				continue
			}
			if transition.direction != uniform.direction || !transition.types.same(uniform.types) {
				return nil, -1
			}
		}
	}
	return uniform, start
}

// equalStrings reports whether two lists hold the same strings in order.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

// uniformPair returns the rows of the shortest path from source to target
// (every one for groups) of a uniform pattern, and ok false when the walk
// search can't answer, which leaves the pair to the general search.
func (s *pathSearch) uniformPair(source, target *storage.Node, groups bool, bind func(pathSearchPath) (pipelineRow, bool)) ([]pipelineRow, bool) {
	a := s.a
	if viewport, ok := TemporalViewportFromContext(s.ctx); a.uniform == nil || source.ID == target.ID || ok && viewport.Enabled() {
		return nil, false
	}
	if !s.chainHolds(a.chains[a.initial][a.uniformStart], source) || !s.slotHolds(a.final, target) {
		return nil, true
	}
	forward := s.newWalkSide(source.ID, a.uniform.direction, groups)
	backward := s.newWalkSide(target.ID, reverseDirection(a.uniform.direction), groups)
	shortest, meets := -1, []storage.NodeID(nil)
	for shortest < 0 && len(forward.frontier) > 0 && len(backward.frontier) > 0 && s.ctx.Err() == nil &&
		(s.maxLength < 0 || forward.radius+backward.radius < s.maxLength) {
		if len(forward.frontier) <= len(backward.frontier) {
			shortest, meets = s.walkLevel(forward, backward, groups)
		} else {
			shortest, meets = s.walkLevel(backward, forward, groups)
		}
		if getExpressionFailure(s.ctx) != nil {
			return nil, true
		}
	}
	if shortest < 0 {
		// No walk, so no path, within the length allowed.
		return nil, true
	}
	if shortest < a.minLength {
		return nil, false
	}
	var walks [][]*storage.Edge
	for _, meet := range meets {
		for _, before := range s.walkHops(forward, meet, groups) {
			for _, after := range s.walkHops(backward, meet, groups) {
				if s.ctx.Err() != nil {
					return nil, true
				}
				// The hops to meet from the start, then from meet to the end:
				// the walk's relationships in order.
				walk := make([]*storage.Edge, 0, shortest)
				for index := len(before) - 1; index >= 0; index-- {
					walk = append(walk, before[index].edge)
				}
				for _, hop := range after {
					walk = append(walk, hop.edge)
				}
				walks = append(walks, walk)
				if !groups {
					break
				}
			}
			if !groups && len(walks) > 0 {
				break
			}
		}
		if !groups && len(walks) > 0 {
			break
		}
	}
	return s.bindWalks(source, target, walks, groups, bind)
}

// newWalkSide starts the walk search from the node id.
func (s *pathSearch) newWalkSide(id storage.NodeID, direction string, groups bool) *pathSearchWalkSide {
	side := &pathSearchWalkSide{direction: direction, reached: map[storage.NodeID]pathSearchWalkReach{id: {}},
		frontier: []storage.NodeID{id}}
	if groups {
		side.more = map[storage.NodeID][]pathSearchWalkHop{}
	}
	return side
}

// walkLevel extends side by one relationship and returns the length of the
// shortest walk through a node both sides reached, and those nodes on the
// side's new level (-1 and nil when there is none yet). Every such node
// gives the same length: a node the other side reached before its last
// level would have met this side earlier. So one shortest walk ends the
// level at the first node met, and every shortest walk needs the level
// whole, for the hops into its nodes.
func (s *pathSearch) walkLevel(side, other *pathSearchWalkSide, groups bool) (int, []storage.NodeID) {
	reached := side.radius + 1
	shortest := -1
	next := side.next[:0]
	for count, id := range side.frontier {
		if count&bfsCancelCheckMask == 0 && s.ctx.Err() != nil {
			return -1, nil
		}
		outgoing, incoming, err := s.walkEdges(id, side.direction)
		if err != nil {
			recordExpressionFailure(s.ctx, err)
			return -1, nil
		}
		// A self-relationship (in both lists) reaches the node itself, which
		// the side has reached.
		for _, edges := range [2][]*storage.Edge{outgoing, incoming} {
			for index, edge := range edges {
				if index&bfsCancelCheckMask == 0 && s.ctx.Err() != nil {
					return -1, nil
				}
				if !s.a.uniform.types.matches(edge.Type) {
					continue
				}
				node := otherEnd(edge, id)
				known, seen := side.reached[node]
				hop := pathSearchWalkHop{from: id, edge: edge}
				switch {
				case !seen:
					side.reached[node] = pathSearchWalkReach{depth: reached, hop: hop}
					next = append(next, node)
					if met, both := other.reached[node]; both && shortest < 0 {
						shortest = reached + met.depth
						if !groups {
							side.frontier, side.radius = nil, reached
							return shortest, []storage.NodeID{node}
						}
					}
				case groups && known.depth == reached:
					side.more[node] = append(side.more[node], hop)
				}
			}
		}
	}
	side.next, side.frontier = side.frontier, next
	side.radius = reached
	if shortest < 0 {
		return -1, nil
	}
	var meets []storage.NodeID
	for _, node := range next {
		if met, both := other.reached[node]; both && reached+met.depth == shortest {
			meets = append(meets, node)
		}
	}
	return shortest, meets
}

// walkEdges lists node's relationships in direction, in two lists.
func (s *pathSearch) walkEdges(node storage.NodeID, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	return incidentEdgeListsContext(s.ctx, s.e.storage, node, direction)
}

// walkHops returns the shortest walks from side's end to node, each as its
// hops from node back to the end: one, or all of them for groups.
func (s *pathSearch) walkHops(side *pathSearchWalkSide, node storage.NodeID, groups bool) [][]pathSearchWalkHop {
	if s.ctx.Err() != nil {
		return nil
	}
	known := side.reached[node]
	if !groups {
		hops := make([]pathSearchWalkHop, 0, known.depth)
		for ; known.depth > 0; known = side.reached[known.hop.from] {
			if s.ctx.Err() != nil {
				return nil
			}
			hops = append(hops, known.hop)
		}
		return [][]pathSearchWalkHop{hops}
	}
	if known.depth == 0 {
		return [][]pathSearchWalkHop{nil}
	}
	hops := []pathSearchWalkHop{known.hop}
	if groups {
		hops = append(hops, side.more[node]...)
	}
	var walks [][]pathSearchWalkHop
	for _, hop := range hops {
		for _, rest := range s.walkHops(side, hop.from, groups) {
			if s.ctx.Err() != nil {
				return nil
			}
			walks = append(walks, append([]pathSearchWalkHop{hop}, rest...))
		}
	}
	return walks
}

// bindWalks reads the nodes of walks (each its relationships in order from
// source) in one call and returns the rows of the walks the pattern binds:
// the first one, or all of them for groups. ok is false when a node doesn't
// load or a walk doesn't bind, which leaves the pair to the general search.
func (s *pathSearch) bindWalks(source, target *storage.Node, walks [][]*storage.Edge, groups bool, bind func(pathSearchPath) (pipelineRow, bool)) ([]pipelineRow, bool) {
	if s.ctx.Err() != nil {
		return nil, true
	}
	nodes := map[storage.NodeID]*storage.Node{source.ID: source, target.ID: target}
	var ids []storage.NodeID
	for _, walk := range walks {
		if s.ctx.Err() != nil {
			return nil, true
		}
		for _, edge := range walk {
			if s.ctx.Err() != nil {
				return nil, true
			}
			for _, id := range [2]storage.NodeID{edge.StartNode, edge.EndNode} {
				if _, known := nodes[id]; !known {
					nodes[id] = nil
					ids = append(ids, id)
				}
			}
		}
	}
	if len(ids) > 0 {
		loaded, err := s.e.storage.BatchGetNodes(ids)
		if err != nil {
			return nil, false
		}
		for _, id := range ids {
			if nodes[id] = loaded[id]; nodes[id] == nil {
				return nil, false
			}
		}
	}
	var rows []pipelineRow
	for _, walk := range walks {
		if s.ctx.Err() != nil {
			return nil, true
		}
		steps := make([]pathSearchPathStep, len(walk))
		at := source.ID
		for index, edge := range walk {
			if s.ctx.Err() != nil {
				return nil, true
			}
			at = otherEnd(edge, at)
			steps[index] = pathSearchPathStep{edge: edge, node: nodes[at]}
		}
		path, ok := s.a.walkPathContext(s.ctx, source, steps)
		if !ok {
			return nil, false
		}
		row, holds := bind(path)
		if !holds {
			return nil, false
		}
		rows = append(rows, row)
		if !groups {
			break
		}
	}
	return rows, true
}

// walkPath reads a walk of the uniform pattern from start (steps hold the
// relationships and nodes) through the automaton: the chain it starts with
// and the move of each step. ok is false when the pattern has no such path.
func (a *pathSearchAutomaton) walkPath(start *storage.Node, steps []pathSearchPathStep) (pathSearchPath, bool) {
	return a.walkPathContext(context.Background(), start, steps)
}

func (a *pathSearchAutomaton) walkPathContext(ctx context.Context, start *storage.Node, steps []pathSearchPathStep) (pathSearchPath, bool) {
	type replayKey struct {
		slot, step int
		counts     string
	}
	var failed map[replayKey]bool
	var visit func(slot, step int, counts string) bool
	visit = func(slot, step int, counts string) bool {
		if ctx.Err() != nil {
			return false
		}
		if step == len(steps) {
			return slot == a.final
		}
		if failed[replayKey{slot, step, counts}] {
			return false
		}
		for _, index := range a.from[slot] {
			to := a.moves[index].to
			if (step == len(steps)-1) != (to == a.final) {
				continue
			}
			move := a.moves[index]
			transition := a.slots[move.from].transitions[move.transition]
			next, ok := a.chainCounts(a.chains[transition.to][move.chain], counts)
			if ok && visit(to, step+1, next) {
				steps[step].move = index
				return true
			}
		}
		if failed == nil {
			failed = map[replayKey]bool{}
		}
		failed[replayKey{slot, step, counts}] = true
		return false
	}
	for _, chain := range a.chains[a.initial] {
		counts, valid := a.chainCounts(chain, "")
		if !valid {
			continue
		}
		if len(steps) == 0 && chain.end == a.final || len(steps) > 0 && len(a.slots[chain.end].transitions) > 0 && visit(chain.end, 0, counts) {
			return pathSearchPath{start: start, chain: chain.slots, steps: steps}, true
		}
	}
	return pathSearchPath{}, false
}
