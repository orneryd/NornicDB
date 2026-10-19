package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// pathSearchKey is a pair of a data node and a slot of the automaton.
type pathSearchKey struct {
	node   storage.NodeID
	slot   int32
	counts string
}

// pathSearchProduct is a pair the search reached, with the steps from it
// once it has been expanded.
type pathSearchProduct struct {
	node     *storage.Node
	slot     int
	counts   string
	steps    []pathSearchStep
	expanded bool
}

// pathSearchStep is a move from a product along a relationship.
type pathSearchStep struct {
	edge *storage.Edge
	to   int32
	move int32
}

// pathSearchLink is a path to an entry of a level, from its last step back
// (step of product); prev is nil at the source.
type pathSearchLink struct {
	product, step int32
	prev          *pathSearchLink
}

// uses reports whether the path through link uses the relationship id.
func (link *pathSearchLink) uses(s *pathSearch, id storage.EdgeID) bool {
	for ; link != nil; link = link.prev {
		if s.products[link.product].steps[link.step].edge.ID == id {
			return true
		}
	}
	return false
}

// pathSearchEntry is a product a path of the level's length reaches: the
// steps into it from the previous level and one such path (witness).
type pathSearchEntry struct {
	product int32
	preds   []pathSearchPred
	witness *pathSearchLink
}

// pathSearchPred is a step (of the entry's product, step) from an entry of
// the previous level.
type pathSearchPred struct {
	entry, step int32
}

// pathSearchLevel is the products the paths of one length reach.
type pathSearchLevel struct {
	entries []pathSearchEntry
	index   map[int32]int32
}

// pathSearchHop is a step of a path traced back through the levels.
type pathSearchHop struct {
	product, step int32
}

// pathSearchPath is a found path: its first node with the chain of slots
// it takes, then each step.
type pathSearchPath struct {
	start *storage.Node
	chain []int
	steps []pathSearchPathStep
}

// pathSearchPathStep is a relationship of a found path, the move it takes
// and the node it reaches.
type pathSearchPathStep struct {
	edge *storage.Edge
	move int
	node *storage.Node
}

// pathSearchEdgeKey caches the relationships of a node in a direction.
type pathSearchEdgeKey struct {
	node      storage.NodeID
	direction string
}

// pathSearch is the search of one selected pattern for one row.
type pathSearch struct {
	e          *StorageExecutor
	access     *relationshipAccessExecution
	ctx        context.Context
	a          *pathSearchAutomaton
	row        pipelineRow
	scratch    map[string]interface{}
	traversal  *TraversalContext
	repeatable bool
	maxLength  int
	edges      map[pathSearchEdgeKey][]*storage.Edge
	products   []pathSearchProduct
	index      map[pathSearchKey]int32
	// unexpanded counts the products with transitions not expanded yet;
	// finals are the products of the final slot.
	unexpanded int
	finals     []int32
	roots      map[int32]int
	levels     []pathSearchLevel
	// target restricts the end to one node, with distance the length of
	// the shortest walk from a product to it; ends restricts it to a set.
	target   *storage.Node
	distance map[pathSearchKey]int
	ends     map[storage.NodeID]bool
	nogoods  map[[2]int32][][]storage.EdgeID
	// dominators tells, once every product is expanded, the relationships
	// every walk to a product takes; closed marks the end products no path
	// can reach again.
	dominators *pathSearchDominators
	closed     map[int32]bool
	// from is the node the search starts at.
	from *storage.Node
	// memory is the search's account in the statement's memory scope: its
	// frontiers, visited products and predecessors count against the limit as
	// they grow, and go back when the search ends (retain, #1343).
	memory memoryRetention
}

// newPathSearch starts a search over automaton a for row. Its state is
// made when the search begins (begin), so the walk search of a uniform
// pattern (uniformPair) doesn't pay for it.
func (e *StorageExecutor) newPathSearch(ctx context.Context, a *pathSearchAutomaton, row pipelineRow, maxLength int, from *storage.Node) *pathSearch {
	return &pathSearch{e: e, ctx: ctx, a: a, row: row, maxLength: maxLength, from: from, repeatable: repeatableElements(ctx), memory: newMemoryRetention(ctx)}
}

// slotHolds reports whether node may take slot: the node a row binds to
// the slot's variable, the slot's labels and its predicates.
func (s *pathSearch) slotHolds(slot int, node *storage.Node) bool {
	held := &s.a.slots[slot]
	if slot == s.a.final && (s.target != nil && node.ID != s.target.ID || s.ends != nil && !s.ends[node.ID]) {
		return false
	}
	if held.variable != "" && !held.list {
		if bound, exists := s.row[held.variable]; exists {
			if boundNode, ok := bound.(*storage.Node); !ok || boundNode == nil || boundNode.ID != node.ID {
				return false
			}
		}
	}
	for _, label := range held.labels {
		if !nodeHasLabel(node, label) {
			return false
		}
	}
	return s.predicatesHold(held.variable, node, held.predicates)
}

// chainHolds reports whether node may take every slot of chain.
func (s *pathSearch) chainHolds(chain pathSearchChain, node *storage.Node) bool {
	for _, slot := range chain.slots {
		if !s.slotHolds(slot, node) {
			return false
		}
	}
	return true
}

// transitionHolds reports whether edge may take transition t.
func (s *pathSearch) transitionHolds(t *pathSearchTransition, edge *storage.Edge) bool {
	if len(t.types) > 0 {
		matched := false
		for _, name := range t.types {
			matched = matched || edge.Type == name
		}
		if !matched {
			return false
		}
	}
	if t.variable != "" && !t.list {
		if bound, exists := s.row[t.variable]; exists {
			if boundEdge, ok := bound.(*storage.Edge); !ok || boundEdge == nil || boundEdge.ID != edge.ID {
				return false
			}
		}
	}
	return s.predicatesHold(t.variable, edge, t.predicates)
}

// predicatesHold evaluates predicates with variable bound to value.
func (s *pathSearch) predicatesHold(variable string, value interface{}, predicates []string) bool {
	if len(predicates) == 0 {
		return true
	}
	if s.scratch == nil {
		s.scratch = make(map[string]interface{}, len(s.row)+4)
		for name, value := range s.row {
			s.scratch[name] = value
		}
	}
	if variable != "" {
		s.scratch[variable] = value
	}
	for _, predicate := range predicates {
		if !s.e.evaluateWithWhereCondition(s.ctx, predicate, s.scratch) {
			return false
		}
	}
	return true
}

// relationships lists node's relationships in direction (in the
// automaton's direction), cached.
func (s *pathSearch) relationships(node storage.NodeID, direction string) []*storage.Edge {
	key := pathSearchEdgeKey{node: node, direction: direction}
	if edges, cached := s.edges[key]; cached {
		return edges
	}
	outgoing, incoming, err := s.walkEdges(node, direction)
	if err != nil {
		recordExpressionFailure(s.ctx, err)
		return nil
	}
	edges := outgoing
	if direction == "incoming" {
		edges = incoming
	} else if direction != "outgoing" {
		edges, err = combineIncidentEdges(s.ctx, node, outgoing, incoming)
		if err != nil {
			recordExpressionFailure(s.ctx, err)
			return nil
		}
	}
	s.edges[key] = edges
	s.retain(searchEntryBytes + sliceHeaderBytes + int64(len(edges))*8)
	return edges
}

// reverseDirection is direction seen from the other end.
func reverseDirection(direction string) string {
	switch direction {
	case "outgoing":
		return "incoming"
	case "incoming":
		return "outgoing"
	}
	return direction
}

// product returns the product of node and slot, added when new.
func (s *pathSearch) productCounts(node *storage.Node, slot int, counts string) int32 {
	key := pathSearchKey{node: node.ID, slot: int32(slot), counts: counts}
	if index, exists := s.index[key]; exists {
		return index
	}
	index := int32(len(s.products))
	s.products = append(s.products, pathSearchProduct{node: node, slot: slot, counts: counts})
	s.index[key] = index
	s.retain(searchProductBytes + int64(len(counts))*8)
	if len(s.a.slots[slot].transitions) > 0 {
		s.unexpanded++
	}
	if slot == s.a.final {
		s.finals = append(s.finals, index)
	}
	return index
}

// expand lists the steps from product p.
func (s *pathSearch) expand(p int32) {
	if s.ctx.Err() != nil || s.products[p].expanded {
		return
	}
	s.products[p].expanded = true
	node, slot := s.products[p].node, s.products[p].slot
	if len(s.a.slots[slot].transitions) > 0 {
		s.unexpanded--
	}
	var steps []pathSearchStep
	for _, index := range s.a.from[slot] {
		if s.ctx.Err() != nil {
			return
		}
		move := s.a.moves[index]
		transition := &s.a.slots[slot].transitions[move.transition]
		chain := s.a.chains[transition.to][move.chain]
		counts, valid := s.a.chainCounts(chain, s.products[p].counts)
		if !valid {
			continue
		}
		for _, edge := range s.relationships(node.ID, transition.direction) {
			if s.ctx.Err() != nil {
				return
			}
			if !s.transitionHolds(transition, edge) {
				continue
			}
			next, ok := s.e.loadTraversalEndpointNode(s.traversal, otherEnd(edge, node.ID))
			if !ok || !s.chainHolds(chain, next) {
				continue
			}
			steps = append(steps, pathSearchStep{edge: edge, to: s.productCounts(next, chain.end, counts), move: int32(index)})
		}
	}
	s.products[p].steps = steps
}

// begin makes the search's state and the first level: the products of the
// source node.
func (s *pathSearch) begin(source *storage.Node) {
	if s.ctx.Err() != nil {
		return
	}
	if s.index == nil {
		s.traversal = s.e.newTraversalContext(s.ctx, s.from, &RelationshipPattern{})
		s.edges = map[pathSearchEdgeKey][]*storage.Edge{}
		s.index = map[pathSearchKey]int32{}
		s.roots = map[int32]int{}
		s.nogoods = map[[2]int32][][]storage.EdgeID{}
	}
	level := pathSearchLevel{index: map[int32]int32{}}
	for index, chain := range s.a.chains[s.a.initial] {
		if s.ctx.Err() != nil {
			return
		}
		if !s.chainHolds(chain, source) {
			continue
		}
		// The chains from one slot end at distinct slots.
		counts, valid := s.a.chainCounts(chain, "")
		if !valid {
			continue
		}
		p := s.productCounts(source, chain.end, counts)
		s.roots[p] = index
		level.index[p] = int32(len(level.entries))
		level.entries = append(level.entries, pathSearchEntry{product: p})
	}
	if s.ctx.Err() != nil {
		return
	}
	s.levels = append(s.levels[:0], level)
}

// reachable reports whether a product at depth may still reach the target
// within the search's length.
func (s *pathSearch) reachable(p int32, depth int) bool {
	if s.distance == nil {
		return true
	}
	product := s.products[p]
	distance, known := s.distance[pathSearchKey{node: product.node.ID, slot: int32(product.slot)}]
	return known && (s.maxLength < 0 || depth+distance <= s.maxLength)
}

// nextLevel adds the level one longer than the last: the products a path
// (a walk that repeats no relationship) of that length reaches. It reports
// whether there is one.
func (s *pathSearch) nextLevel() bool {
	if s.ctx.Err() != nil {
		return false
	}
	depth := len(s.levels) - 1
	level := &s.levels[depth]
	next := pathSearchLevel{index: map[int32]int32{}}
	for entryIndex := range level.entries {
		if s.ctx.Err() != nil {
			return false
		}
		entry := level.entries[entryIndex]
		s.expand(entry.product)
		for stepIndex, step := range s.products[entry.product].steps {
			if s.ctx.Err() != nil {
				return false
			}
			if !s.reachable(step.to, depth+1) {
				continue
			}
			index, exists := next.index[step.to]
			if !exists {
				index = int32(len(next.entries))
				next.index[step.to] = index
				next.entries = append(next.entries, pathSearchEntry{product: step.to})
			}
			added := &next.entries[index]
			added.preds = append(added.preds, pathSearchPred{entry: int32(entryIndex), step: int32(stepIndex)})
			s.retain(8)
			if added.witness == nil && (s.repeatable || !entry.witness.uses(s, step.edge.ID)) {
				added.witness = &pathSearchLink{product: entry.product, step: int32(stepIndex), prev: entry.witness}
			}
		}
	}
	live := next.entries[:0]
	for _, entry := range next.entries {
		if s.ctx.Err() != nil {
			return false
		}
		for _, pred := range entry.preds {
			if s.ctx.Err() != nil {
				return false
			}
			if entry.witness != nil {
				break
			}
			from := level.entries[pred.entry]
			edge := s.products[from.product].steps[pred.step].edge
			if depth > 0 && s.unexpanded == 0 && s.closedStep(from.product, edge.ID) {
				// Every walk to the product before uses the relationship.
				continue
			}
			used := map[storage.EdgeID]struct{}{edge.ID: {}}
			s.trails(depth, pred.entry, used, nil, func(hops []pathSearchHop) bool {
				link := (*pathSearchLink)(nil)
				for index := len(hops) - 1; index >= 0; index-- {
					link = &pathSearchLink{product: hops[index].product, step: hops[index].step, prev: link}
				}
				entry.witness = &pathSearchLink{product: from.product, step: pred.step, prev: link}
				return false
			})
		}
		if entry.witness != nil {
			live = append(live, entry)
		}
	}
	if len(live) == 0 || s.ctx.Err() != nil {
		return false
	}
	next.entries = live
	next.index = make(map[int32]int32, len(live))
	for index, entry := range live {
		next.index[entry.product] = int32(index)
	}
	s.levels = append(s.levels, next)
	s.retain(searchEntryBytes)
	return true
}

// trails calls yield with each path that ends at entry of the level at
// depth and uses no relationship in used, as its hops from the last step
// back to the first; it stops when yield returns false. It returns whether
// it was not stopped, whether it found a path, and, when it found none, the
// relationships of used that blocked it.
func (s *pathSearch) trails(depth int, entry int32, used map[storage.EdgeID]struct{}, hops []pathSearchHop, yield func([]pathSearchHop) bool) (bool, bool, []storage.EdgeID) {
	if s.ctx.Err() != nil {
		return false, false, nil
	}
	if depth == 0 {
		return yield(hops), true, nil
	}
	key := [2]int32{int32(depth), entry}
	for _, nogood := range s.nogoods[key] {
		blocked := true
		for _, id := range nogood {
			if _, in := used[id]; !in {
				blocked = false
				break
			}
		}
		if blocked {
			return true, false, nogood
		}
	}
	found := false
	var conflicts []storage.EdgeID
	conflict := func(id storage.EdgeID) {
		for _, have := range conflicts {
			if have == id {
				return
			}
		}
		conflicts = append(conflicts, id)
	}
	for _, pred := range s.levels[depth].entries[entry].preds {
		from := s.levels[depth-1].entries[pred.entry]
		id := s.products[from.product].steps[pred.step].edge.ID
		if !s.repeatable {
			if _, in := used[id]; in {
				conflict(id)
				continue
			}
			used[id] = struct{}{}
		}
		keepGoing, foundBelow, below := s.trails(depth-1, pred.entry, used, append(hops, pathSearchHop{product: from.product, step: pred.step}), yield)
		if !s.repeatable {
			delete(used, id)
		}
		if !keepGoing {
			return false, true, nil
		}
		found = found || foundBelow
		for _, blocked := range below {
			if blocked != id {
				conflict(blocked)
			}
		}
	}
	if !found && len(s.nogoods[key]) < 8 {
		s.nogoods[key] = append(s.nogoods[key], conflicts)
	}
	return true, found, conflicts
}

// pathOf is the path of hops (from the last step back) ending at product
// last.
func (s *pathSearch) pathOf(hops []pathSearchHop) pathSearchPath {
	if len(hops) == 0 || s.ctx.Err() != nil {
		return pathSearchPath{}
	}
	path := s.rootPath(hops[len(hops)-1].product)
	for index := len(hops) - 1; index >= 0; index-- {
		if s.ctx.Err() != nil {
			return pathSearchPath{}
		}
		step := s.products[hops[index].product].steps[hops[index].step]
		path.steps = append(path.steps, pathSearchPathStep{edge: step.edge, move: int(step.move), node: s.products[step.to].node})
	}
	return path
}

// rootPath is the path of length 0 at a root product.
func (s *pathSearch) rootPath(p int32) pathSearchPath {
	return pathSearchPath{start: s.products[p].node, chain: s.a.chains[s.a.initial][s.roots[p]].slots}
}

// linkHops are the hops of a witness path.
func linkHops(ctx context.Context, link *pathSearchLink) []pathSearchHop {
	var hops []pathSearchHop
	for ; link != nil; link = link.prev {
		if ctx.Err() != nil {
			return nil
		}
		hops = append(hops, pathSearchHop{product: link.product, step: link.step})
	}
	return hops
}

// pathSearchTarget counts what an end node still needs: paths, or groups.
type pathSearchTarget struct {
	remaining int
}

// trace offers the paths to entry of the level at depth to take, until it
// returns false; one path (need 1) is the entry's witness first.
func (s *pathSearch) trace(depth int, entry int32, need int, take func(pathSearchPath) bool) {
	if s.ctx.Err() != nil {
		return
	}
	if depth == 0 {
		take(s.rootPath(s.levels[0].entries[entry].product))
		return
	}
	if need == 1 {
		path := s.pathOf(linkHops(s.ctx, s.levels[depth].entries[entry].witness))
		if s.ctx.Err() != nil || !take(path) {
			return
		}
	}
	s.trails(depth, entry, map[storage.EdgeID]struct{}{}, nil, func(hops []pathSearchHop) bool {
		if s.ctx.Err() != nil {
			return false
		}
		path := s.pathOf(hops)
		if s.ctx.Err() != nil {
			return false
		}
		return take(path)
	})
}

// run searches from source and returns the rows of the selected paths to
// each end node; bind makes a path's row and reports whether the path
// counts (holds the pattern's predicates that apply to whole paths).
func (s *pathSearch) run(source *storage.Node, count int, groups bool, bind func(pathSearchPath) (pipelineRow, bool)) []pipelineRow {
	if s.ctx.Err() != nil {
		return nil
	}
	s.begin(source)
	if s.ctx.Err() != nil {
		return nil
	}
	var rows []pipelineRow
	targets := map[int32]*pathSearchTarget{}
	for depth := 0; len(s.levels[0].entries) > 0; depth++ {
		for entryIndex, entry := range s.levels[depth].entries {
			if s.ctx.Err() != nil {
				return rows
			}
			if s.products[entry.product].slot != s.a.final {
				continue
			}
			target := targets[entry.product]
			if target == nil {
				target = &pathSearchTarget{remaining: count}
				targets[entry.product] = target
			}
			if target.remaining == 0 {
				continue
			}
			need, found := target.remaining, 0
			if groups {
				need = 0
			}
			s.trace(depth, int32(entryIndex), need, func(path pathSearchPath) bool {
				if s.ctx.Err() != nil {
					return false
				}
				if row, ok := bind(path); ok {
					rows = append(rows, row)
					found++
				}
				return groups || found < need
			})
			switch {
			case groups && found > 0:
				target.remaining--
			case !groups:
				target.remaining -= found
			}
		}
		if s.ctx.Err() != nil || s.done(targets) || s.maxLength >= 0 && depth >= s.maxLength || !s.nextLevel() {
			return rows
		}
	}
	return rows
}

// done reports whether no end node may get another path: the one target
// (or each of the ends) has its paths, or every product reached has been
// expanded (so no other end node is reachable) and each end node reached
// has its paths or can't be reached by another path.
func (s *pathSearch) done(targets map[int32]*pathSearchTarget) bool {
	satisfied := func(p int32) bool {
		target := targets[p]
		return target != nil && target.remaining == 0 || s.unexpanded == 0 && s.closedEnd(p)
	}
	final := int32(s.a.final)
	switch {
	case s.target != nil:
		index, exists := s.index[pathSearchKey{node: s.target.ID, slot: final}]
		return exists && satisfied(index) || !exists && s.unexpanded == 0
	case s.ends != nil:
		all := true
		for id := range s.ends {
			index, exists := s.index[pathSearchKey{node: id, slot: final}]
			all = all && exists && satisfied(index)
		}
		if all {
			return true
		}
	}
	if s.unexpanded > 0 {
		return false
	}
	for _, final := range s.finals {
		if !satisfied(final) {
			return false
		}
	}
	return true
}

// closedEnd reports whether no path can reach the end product p again:
// every step into it is closed (closedStep). It holds only once every
// product is expanded.
func (s *pathSearch) closedEnd(p int32) bool {
	if s.repeatable {
		return false
	}
	s.dominate()
	if closed, known := s.closed[p]; known {
		return closed
	}
	closed := true
	for _, hop := range s.dominators.incoming[p] {
		if !s.closedStep(hop.product, s.products[hop.product].steps[hop.step].edge.ID) {
			closed = false
			break
		}
	}
	s.closed[p] = closed
	return closed
}

// closedStep reports whether no path that took a step can take the step
// from product p along the relationship id: every walk of one step or more
// from the source to p uses it already, or none reaches p. It holds only
// once every product is expanded.
func (s *pathSearch) closedStep(p int32, id storage.EdgeID) bool {
	s.dominate()
	d := s.dominators
	node := p + 1
	if d.idom[node] < 0 {
		return true
	}
	target, known := d.relationship[id]
	for ; known && node != 0; node = d.idom[node] {
		if node == target {
			return true
		}
	}
	return false
}

// pathSearchDominators are the dominators of the walks from the source:
// over a graph whose node 0 is the source, nodes 1 to n the products
// reached by a step (product p is p+1), then a copy of each first-level
// product the source starts at (with its steps), then a node per
// relationship that every step along it passes. A relationship node that
// dominates a product is a relationship every walk to it takes.
type pathSearchDominators struct {
	idom         []int32
	relationship map[storage.EdgeID]int32
	incoming     map[int32][]pathSearchHop
}

// dominate computes the dominators once (Cooper, Harvey and Kennedy's
// iterative algorithm over the reverse postorder).
func (s *pathSearch) dominate() {
	if s.dominators != nil {
		return
	}
	d := &pathSearchDominators{relationship: map[storage.EdgeID]int32{}, incoming: map[int32][]pathSearchHop{}}
	s.dominators, s.closed = d, map[int32]bool{}
	products := int32(len(s.products))
	count := 1 + products + int32(len(s.levels[0].entries))
	var successors [][]int32
	grow := func() int32 {
		successors = append(successors, nil)
		return int32(len(successors) - 1)
	}
	for index := int32(0); index < count; index++ {
		grow()
	}
	relationshipNode := func(id storage.EdgeID) int32 {
		node, exists := d.relationship[id]
		if !exists {
			node = grow()
			d.relationship[id] = node
		}
		return node
	}
	link := func(from int32, p int32) {
		for _, step := range s.products[p].steps {
			via := relationshipNode(step.edge.ID)
			successors[from] = append(successors[from], via)
			successors[via] = append(successors[via], step.to+1)
		}
	}
	for p := int32(0); p < products; p++ {
		link(p+1, p)
		for index, step := range s.products[p].steps {
			d.incoming[step.to] = append(d.incoming[step.to], pathSearchHop{product: p, step: int32(index)})
		}
	}
	for index, entry := range s.levels[0].entries {
		start := 1 + products + int32(index)
		successors[0] = append(successors[0], start)
		link(start, entry.product)
	}
	// Reverse postorder of the nodes the source reaches.
	nodes := int32(len(successors))
	// Its order, stack, postorder and predecessors take about 40 bytes a node.
	s.retain(int64(nodes) * 40)
	order := make([]int32, nodes)
	for index := range order {
		order[index] = -1
	}
	var postorder []int32
	type frame struct{ node, next int32 }
	stack := []frame{{node: 0}}
	order[0] = 0
	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		if int(top.next) < len(successors[top.node]) {
			next := successors[top.node][top.next]
			top.next++
			if order[next] < 0 {
				order[next] = 0
				stack = append(stack, frame{node: next})
			}
			continue
		}
		postorder = append(postorder, top.node)
		stack = stack[:len(stack)-1]
	}
	predecessors := make([][]int32, nodes)
	for from := int32(0); from < nodes; from++ {
		for _, to := range successors[from] {
			predecessors[to] = append(predecessors[to], from)
		}
	}
	rank := make([]int32, nodes)
	for index := range rank {
		rank[index] = -1
	}
	for index, node := range postorder {
		rank[node] = int32(index)
	}
	d.idom = make([]int32, nodes)
	for index := range d.idom {
		d.idom[index] = -1
	}
	d.idom[0] = 0
	intersect := func(a, b int32) int32 {
		for a != b {
			for rank[a] < rank[b] {
				a = d.idom[a]
			}
			for rank[b] < rank[a] {
				b = d.idom[b]
			}
		}
		return a
	}
	for changed := true; changed; {
		changed = false
		for index := len(postorder) - 2; index >= 0; index-- {
			node := postorder[index]
			dominator := int32(-1)
			for _, pred := range predecessors[node] {
				if d.idom[pred] < 0 {
					continue
				}
				if dominator < 0 {
					dominator = pred
				} else {
					dominator = intersect(pred, dominator)
				}
			}
			if d.idom[node] != dominator {
				d.idom[node] = dominator
				changed = true
			}
		}
	}
}

// Estimated sizes of a search's state: a map entry with its key, and a
// product with its index entry.
const (
	searchEntryBytes   = 48
	searchProductBytes = 96
)

// retain counts n bytes of search state the search keeps against the
// statement's memory limit (#1343). Going over cancels the statement, and
// every loop of the search stops on cancellation.
func (s *pathSearch) retain(n int64) {
	_ = s.memory.reserveBytes(n)
}

// releaseMemory gives back what the search's state took, when the search
// ends: its result rows count where they are kept.
func (s *pathSearch) releaseMemory() {
	s.memory.release()
}
