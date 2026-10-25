package cypher

import (
	"context"
	"encoding/binary"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Selected path search (#1212).
//
// A selective selector (SHORTEST k [GROUPS], ANY SHORTEST, ALL SHORTEST,
// ANY k) takes, for each pair of start and end nodes, the k shortest paths
// of its pattern (for GROUPS, every path of each of the k shortest lengths),
// as Neo4j does; NornicDB's ANY k takes the k shortest, which are some of
// the paths ANY may take. The pattern's own predicates (label expressions,
// property maps, element and group WHEREs, a parenthesised path's WHERE)
// constrain the paths selected from; the clause's WHERE filters the selected
// paths, but for its conjuncts that read only the endpoints, which select
// the pairs and so may apply either way.
//
// The pattern is compiled to an automaton over its node positions: a node
// pattern is a state, a relationship pattern a transition between two
// states, and two juxtaposed node patterns are one node (an empty
// transition). A quantified group or a variable-length relationship is
// compiled once with a counter for its bounds. The search runs over pairs
// of a data node and a state, breadth-first by path length from the bound end: from the start
// node when it is bound (or the end is unbound), over the reversed pattern
// from the end node otherwise, and from both ends when both are bound. One
// search gives every reachable end node its paths, so an unbound end costs
// one search, not one per node.
//
// A path may not repeat a relationship (but in a MATCH REPEATABLE ELEMENTS),
// so the shortest walk to a node is not always a path. Each level of the
// search holds the pairs a path of that length reaches (a walk that is a
// path, checked with the path that first reached the pair, or by a search
// back to the start when that one already used the relationship), and the
// paths to an end node are traced back through the levels. A predicate on a
// single node or relationship is checked as the search reaches it; one that
// reads more of the path is checked on each path traced, which counts toward
// k only when it holds. A length bound in the clause's WHERE (length(p) <= n,
// length(p) < n) ends the search at that length.
//
// shortestPath() and allShortestPaths() run the same search between each
// pair of their endpoints, as ANY SHORTEST and ALL SHORTEST whose WHERE
// constrains the paths selected from (shortest_path_match.go).

// pathPatternItemKind is the kind of an element of a selected pattern.
type pathPatternItemKind int

const (
	pathPatternNode pathPatternItemKind = iota
	pathPatternRelationship
	pathPatternGroup
)

// pathPatternItem is an element of a selected pattern: a node pattern, a
// relationship pattern or a quantified group.
type pathPatternItem struct {
	kind pathPatternItemKind
	// variable is the element's variable ("" for none).
	variable string
	// text is a node pattern as written, for matching its candidates.
	text   string
	labels []string
	// predicates are checked on the element alone: its property map and
	// the conjuncts of the pattern's predicates that read only it.
	predicates []string
	types      []string
	// direction is "outgoing", "incoming" or "both".
	direction string
	// min and max are a group's or variable-length relationship's bounds
	// (max < 0: none); a variable-length relationship is a group of one
	// relationship.
	min, max  int
	varLength bool
	// items are a group's elements; where its own WHERE.
	items []pathPatternItem
	where string
	// group numbers the pattern's groups (and variable-length
	// relationships).
	group int
}

// parsePathPatternItems reads a selected pattern into its elements. ok is
// false for text that isn't a path pattern: node patterns joined by
// relationship patterns, quantified groups and juxtapositions.
func parsePathPatternItems(pattern string, nested bool) ([]pathPatternItem, bool) {
	return parsePathPatternItemsContext(context.Background(), pattern, nested)
}

func parsePathPatternItemsContext(ctx context.Context, pattern string, nested bool) ([]pathPatternItem, bool) {
	var items []pathPatternItem
	for at := skipASCIISpaces(pattern, 0, len(pattern)); at < len(pattern); at = skipASCIISpaces(pattern, at, len(pattern)) {
		if ctx.Err() != nil {
			return nil, false
		}
		switch c := pattern[at]; {
		case c == '(':
			close := findMatchingParen(pattern, at)
			if close < 0 {
				return nil, false
			}
			if parenthesisedPathAt(pattern, at, close) {
				quantifier, ok := relationshipQuantifierAt(pattern, skipASCIISpaces(pattern, close+1, len(pattern)), len(pattern))
				if nested || !ok {
					return nil, false
				}
				inner, where := pattern[at+1:close], ""
				if index := topLevelKeywordIndex(inner, "WHERE"); index >= 0 {
					inner, where = inner[:index], strings.TrimSpace(inner[index+len("WHERE"):])
				}
				members, ok := parsePathPatternItemsContext(ctx, inner, true)
				if !ok || len(members) == 0 || members[0].kind != pathPatternNode || members[len(members)-1].kind != pathPatternNode {
					return nil, false
				}
				items = append(items, pathPatternItem{kind: pathPatternGroup, items: members, where: where, min: quantifier.min, max: quantifier.max})
				at = quantifier.end
				continue
			}
			item, ok := parsePathPatternNode(pattern[at : close+1])
			if !ok || len(items) > 0 && items[len(items)-1].kind == pathPatternNode {
				// Only a quantified group juxtaposes with a node pattern.
				return nil, false
			}
			items = append(items, item)
			at = close + 1
		case c == '-' || c == '<':
			end := arrowRunEnd(pattern, at, len(pattern))
			text := pattern[at:end]
			if end < len(pattern) && pattern[end] == '[' {
				close := findMatchingBracket(pattern, end)
				if close < 0 {
					return nil, false
				}
				arrowEnd := arrowRunEnd(pattern, close+1, len(pattern))
				text = pattern[at:arrowEnd]
				end = arrowEnd
			}
			item, ok := parsePathPatternRelationship(text)
			if !ok || len(items) == 0 || items[len(items)-1].kind == pathPatternRelationship {
				return nil, false
			}
			items = append(items, item)
			at = end
		default:
			return nil, false
		}
	}
	if len(items) == 0 || items[0].kind == pathPatternRelationship || items[len(items)-1].kind == pathPatternRelationship {
		return nil, false
	}
	return items, true
}

// parsePathPatternNode reads a node pattern (text is its parentheses).
func parsePathPatternNode(text string) (pathPatternItem, bool) {
	item := pathPatternItem{kind: pathPatternNode, text: text}
	inner := text[1 : len(text)-1]
	if where := elementWhereIndex(text, 0, len(text)-1); where >= 0 {
		item.predicates = append(item.predicates, strings.TrimSpace(text[where+len("WHERE"):len(text)-1]))
		inner = text[1:where]
		item.text = "(" + inner + ")"
	}
	head, properties := splitNodePatternProperties("(" + inner + ")")
	if _, _, parameter := splitPatternParameterMap(head); parameter && properties == "" {
		// A MATCH pattern takes no parameter map (the statement rewrite
		// rejects one).
		return item, false
	}
	variable, labels, err := parseNodeHead(head)
	if err != nil {
		return item, false
	}
	item.variable, item.labels = variable, labels
	item.predicates = append(propertyMapPredicates(variable, properties), item.predicates...)
	return item, true
}

// parsePathPatternRelationship reads a relationship pattern with its
// arrows (-->, <-[r:T*1..3]-, …).
func parsePathPatternRelationship(text string) (pathPatternItem, bool) {
	item := pathPatternItem{kind: pathPatternRelationship, direction: "both", min: 1, max: 1}
	incoming, outgoing := strings.HasPrefix(text, "<"), strings.HasSuffix(text, ">")
	switch {
	case incoming && !outgoing:
		item.direction = "incoming"
	case outgoing && !incoming:
		item.direction = "outgoing"
	}
	open := strings.IndexByte(text, '[')
	if open < 0 {
		return item, text == "--" || text == "-->" || text == "<--" || text == "<-->"
	}
	close := findMatchingBracket(text, open)
	if prefix, suffix := text[:open], text[close+1:]; close < 0 || prefix != "-" && prefix != "<-" || suffix != "-" && suffix != "->" {
		return item, false
	}
	inner := text[open+1 : close]
	if where := elementWhereIndex(text, open, close); where >= 0 {
		item.predicates = append(item.predicates, strings.TrimSpace(text[where+len("WHERE"):close]))
		inner = text[open+1 : where]
	}
	declaration := relationshipDeclarationOf(inner)
	if declaration.invalidType != "" {
		return item, false
	}
	item.variable, item.types = declaration.variable, declaration.types
	for _, name := range item.types {
		if strings.HasPrefix(name, "$(") {
			// A dynamic type is read per row by the MATCH step only.
			return item, false
		}
	}
	if strings.HasPrefix(declaration.properties, "$") {
		return item, false
	}
	item.predicates = append(propertyMapPredicates(item.variable, declaration.properties), item.predicates...)
	if declaration.hasLength {
		item.varLength = true
		item.min, item.max = 1, -1
		switch spec := declaration.length; {
		case strings.Contains(spec, ".."):
			parts := strings.SplitN(spec, "..", 2)
			if parts[0] != "" {
				item.min = atoiOr(parts[0], 1)
			}
			if parts[1] != "" {
				item.max = atoiOr(parts[1], -1)
			}
		case spec != "":
			item.min = atoiOr(spec, 1)
			item.max = item.min
		}
	}
	return item, true
}

// atoiOr is the integer text holds, or fallback.
func atoiOr(text string, fallback int) int {
	number := 0
	for _, c := range text {
		if c < '0' || c > '9' || number > 1<<30 {
			return fallback
		}
		number = number*10 + int(c-'0')
	}
	return number
}

// propertyMapPredicates writes a property map ({k: v, …}) of the element
// variable as the equalities it requires: an element matches the map when
// each of its properties equals the value (a null value matches nothing).
func propertyMapPredicates(variable, properties string) []string {
	properties = strings.TrimSpace(properties)
	if len(properties) < 2 || properties[0] != '{' {
		return nil
	}
	var predicates []string
	for _, pair := range splitTopLevelComma(strings.TrimSpace(properties[1 : len(properties)-1])) {
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			continue
		}
		key := normalizePropertyKey(pair[:colon])
		predicates = append(predicates, variable+".`"+strings.ReplaceAll(key, "`", "``")+"` = ("+strings.TrimSpace(pair[colon+1:])+")")
	}
	return predicates
}

// pathSearchSlot is a state of a selected pattern's automaton: a node
// position of the (unrolled) pattern.
type pathSearchSlot struct {
	counterEnter, counterStep, counterExit int
	variable                               string
	// list marks a variable inside a group, which binds a list.
	list       bool
	labels     []string
	predicates []string
	// group is the slot's group (-1 outside one); iterationStart marks the
	// first node of each repetition.
	group          int
	iterationStart bool
	transitions    []pathSearchTransition
	empty          []int
}

// pathSearchTransition is a relationship pattern between two slots, in the
// automaton's direction.
type pathSearchTransition struct {
	variable   string
	list       bool
	types      []string
	direction  string
	predicates []string
	to         int
	group      int
}

// pathSearchChain is a run of slots joined by empty transitions that ends
// at a slot with relationship transitions or the final slot: one data node
// takes every slot of it.
type pathSearchChain struct {
	slots []int
	end   int
}

// pathSearchMove is one step of the search: a relationship transition from
// a slot and the chain of slots its node then takes.
type pathSearchMove struct {
	from, transition, chain, to int
}

// pathSearchAutomaton is a selected pattern compiled for the search.
type pathSearchCounter struct{ min, max int }

type pathSearchAutomaton struct {
	ctx            context.Context
	counters       []pathSearchCounter
	repetitions    int
	slots          []pathSearchSlot
	initial, final int
	// chains[slot] are the chains that start at slot.
	chains [][]pathSearchChain
	// moves[slot] are the moves from slot; into[slot] the indexes of the
	// moves that end at it.
	moves []pathSearchMove
	from  [][]int
	into  [][]int
	// maxLength is the longest path the pattern allows (-1: none);
	// minLength the shortest.
	maxLength, minLength int
	// uniform is the one relationship pattern of a uniform pattern
	// (uniformTransition, path_selector_search_walk.go), nil for others;
	// uniformStart is the chain from the initial slot its walks start with.
	uniform      *pathSearchTransition
	uniformStart int
}

// buildPathSearchAutomaton compiles a pattern's elements.
func buildPathSearchAutomatonContext(ctx context.Context, items []pathPatternItem) *pathSearchAutomaton {
	a := &pathSearchAutomaton{ctx: ctx}
	a.initial, a.final = a.sequence(items, -1)
	if ctx.Err() != nil {
		return nil
	}
	a.chains = make([][]pathSearchChain, len(a.slots))
	for slot := range a.slots {
		if ctx.Err() != nil {
			return nil
		}
		a.chains[slot] = a.chainsFrom(slot, nil)
	}
	a.from = make([][]int, len(a.slots))
	a.into = make([][]int, len(a.slots))
	for slot := range a.slots {
		if ctx.Err() != nil {
			return nil
		}
		for t, transition := range a.slots[slot].transitions {
			for c, chain := range a.chains[transition.to] {
				a.from[slot] = append(a.from[slot], len(a.moves))
				a.into[chain.end] = append(a.into[chain.end], len(a.moves))
				a.moves = append(a.moves, pathSearchMove{from: slot, transition: t, chain: c, to: chain.end})
			}
		}
	}
	a.maxLength = itemsMaxLength(items)
	a.minLength = itemsMinLength(items)
	a.uniform, a.uniformStart = uniformTransition(a)
	a.ctx = context.Background()
	return a
}

// itemsMaxLength is the longest path items allow (-1: none).
func itemsMaxLength(items []pathPatternItem) int {
	length := 0
	for _, item := range items {
		switch {
		case item.kind == pathPatternNode:
		case item.kind == pathPatternRelationship && !item.varLength:
			length++
		case item.max < 0:
			return -1
		default:
			inner := 1
			if item.kind == pathPatternGroup {
				inner = itemsMaxLength(item.items)
			}
			length += inner * item.max
		}
	}
	return length
}

func (a *pathSearchAutomaton) addSlot(slot pathSearchSlot) int {
	a.slots = append(a.slots, slot)
	return len(a.slots) - 1
}

// sequence adds the slots of items (inside group, -1 for none) and returns
// the first and last.
func (a *pathSearchAutomaton) sequence(items []pathPatternItem, group int) (int, int) {
	first, last := -1, -1
	join := func(entry, exit int) {
		if last >= 0 {
			a.slots[last].empty = append(a.slots[last].empty, entry)
		} else {
			first = entry
		}
		last = exit
	}
	var pending *pathPatternItem
	for index := range items {
		if a.ctx.Err() != nil {
			return first, last
		}
		item := &items[index]
		switch {
		case item.kind == pathPatternNode:
			slot := a.addSlot(pathSearchSlot{variable: item.variable, list: group >= 0, labels: item.labels, predicates: item.predicates, group: group})
			if pending != nil {
				a.slots[last].transitions = append(a.slots[last].transitions, a.transition(*pending, slot, group))
				pending = nil
				last = slot
			} else {
				join(slot, slot)
			}
		case item.kind == pathPatternRelationship && !item.varLength:
			pending = item
		default:
			// A variable-length relationship repeats a group of itself
			// between two anonymous nodes.
			members := item.items
			if item.kind == pathPatternRelationship {
				single := *item
				single.varLength, single.min, single.max = false, 1, 1
				members = []pathPatternItem{{kind: pathPatternNode}, single, {kind: pathPatternNode}}
			}
			entry, exit := a.repetition(members, item.min, item.max, item.group)
			join(entry, exit)
		}
	}
	return first, last
}

// transition is the transition for a relationship element.
func (a *pathSearchAutomaton) transition(item pathPatternItem, to, group int) pathSearchTransition {
	return pathSearchTransition{variable: item.variable, list: group >= 0, types: item.types, direction: item.direction,
		predicates: item.predicates, to: to, group: group}
}

// repetition adds one copy of a group. A counted quantifier compiles to a
// counter instead of one copy per repetition.
func (a *pathSearchAutomaton) repetition(members []pathPatternItem, min, max, group int) (int, int) {
	a.repetitions++
	entry := a.addSlot(pathSearchSlot{group: -1})
	exit := a.addSlot(pathSearchSlot{group: -1})
	if max == 0 {
		a.slots[entry].empty = append(a.slots[entry].empty, exit)
		return entry, exit
	}
	first, last := a.sequence(members, group)
	if a.ctx.Err() != nil {
		return entry, exit
	}
	a.slots[first].iterationStart = true
	a.slots[entry].empty = append(a.slots[entry].empty, first)
	a.slots[last].empty = append(a.slots[last].empty, exit, first)
	if min == 0 {
		a.slots[entry].empty = append(a.slots[entry].empty, exit)
	}
	// An unbounded 0/1 minimum needs no state beyond the loop itself.
	if max >= 0 || min > 1 {
		a.counters = append(a.counters, pathSearchCounter{min: min, max: max})
		id := len(a.counters)
		a.slots[entry].counterEnter = id
		a.slots[first].counterStep = id
		a.slots[exit].counterExit = id
	}
	return entry, exit
}

// itemsMinLength is the least number of relationships the pattern allows.
func itemsMinLength(items []pathPatternItem) int {
	length := 0
	for _, item := range items {
		switch {
		case item.kind == pathPatternNode:
		case item.kind == pathPatternGroup:
			length += item.min * itemsMinLength(item.items)
		case item.varLength:
			length += item.min
		default:
			length++
		}
	}
	return length
}

// chainCounts applies the empty chain's counter guards and updates. Completed
// groups clear their counter so final products still identify endpoint nodes.
func (a *pathSearchAutomaton) chainCounts(chain pathSearchChain, counts string) (string, bool) {
	if len(a.counters) == 0 {
		return counts, true
	}
	values := []byte(counts)
	if len(values) == 0 {
		values = make([]byte, 8*len(a.counters))
	}
	for _, index := range chain.slots {
		slot := a.slots[index]
		for _, id := range []int{slot.counterEnter, slot.counterStep, slot.counterExit} {
			if id == 0 {
				continue
			}
			value := binary.LittleEndian.Uint64(values[(id-1)*8:])
			bound := a.counters[id-1]
			switch id {
			case slot.counterEnter:
				value = 0
			case slot.counterStep:
				if bound.max >= 0 && value >= uint64(bound.max) {
					return "", false
				}
				if bound.max >= 0 || value < uint64(bound.min) {
					value++
				}
			case slot.counterExit:
				if value < uint64(bound.min) {
					return "", false
				}
				value = 0
			}
			binary.LittleEndian.PutUint64(values[(id-1)*8:], value)
		}
	}
	for _, value := range values {
		if value != 0 {
			return string(values), true
		}
	}
	return "", true
}

// chainsFrom lists the chains from slot (prefix is the run so far).
func (a *pathSearchAutomaton) chainsFrom(slot int, prefix []int) []pathSearchChain {
	if a.ctx.Err() != nil {
		return nil
	}
	for _, visited := range prefix {
		if visited == slot {
			return nil
		}
	}
	run := append(append([]int(nil), prefix...), slot)
	var chains []pathSearchChain
	if len(a.slots[slot].transitions) > 0 || slot == a.final {
		chains = append(chains, pathSearchChain{slots: run, end: slot})
	}
	for _, next := range a.slots[slot].empty {
		chains = append(chains, a.chainsFrom(next, run)...)
	}
	return chains
}

// reversePathPatternItems is items read from the end: the same paths
// written backwards.
func reversePathPatternItems(items []pathPatternItem) []pathPatternItem {
	reversed := make([]pathPatternItem, len(items))
	for index, item := range items {
		switch item.direction {
		case "outgoing":
			item.direction = "incoming"
		case "incoming":
			item.direction = "outgoing"
		}
		if item.kind == pathPatternGroup {
			item.items = reversePathPatternItems(item.items)
		}
		reversed[len(items)-1-index] = item
	}
	return reversed
}

// selectedPathPlan is a selected pattern compiled for the search: its
// automaton in both directions and its predicates by where they apply.
type selectedPathPlan struct {
	forward, backward *pathSearchAutomaton
	start, end        pathPatternItem
	// constants read no pattern variable: a row they don't hold for
	// selects nothing.
	constants []string
	// pathChecks read more of the path than one element; iterationChecks
	// read more of a repetition than one element, per group.
	pathChecks      []string
	iterationChecks map[int][]string
	// listVariables bind lists; singles bind one element each.
	listVariables []string
	// lengthBounds are the clause's length(p) <= n conjuncts as (bound,
	// strict).
	lengthBounds []pathLengthBound
	// pathVariable names the path; acyclic keeps the paths whose nodes
	// are distinct (ACYCLIC); repeated marks a variable written for two
	// elements, which must be one.
	pathVariable string
	acyclic      bool
	repeated     bool
}

// pathLengthBound is a length(p) <= bound (strict: <) conjunct.
type pathLengthBound struct {
	bound  string
	strict bool
}

// selectedPathPlans holds compiled plans by their text: a MATCH step reads
// its pattern on every execution of a statement. A plan isn't changed once
// compiled, so searches share it.
var selectedPathPlans = newBoundedCache[string, selectedPathPlanEntry](1024, boundedCacheStringShardIndex)

// selectedPathPlanEntry is a compiled plan with its filters (ok false for
// a pattern the search can't read).
type selectedPathPlanEntry struct {
	plan    *selectedPathPlan
	filters []string
	ok      bool
}

// compileSelectedPathPlan compiles a selected pattern with its predicates
// (the pattern's own) and the clause's WHERE conjuncts; it returns the
// conjuncts that filter the selected paths. ok is false for a pattern the
// search can't read. The plan is shared: it must not be changed.
func compileSelectedPathPlan(pattern, pathVariable, predicate string, terms []string) (*selectedPathPlan, []string, bool) {
	return compileSelectedPathPlanContext(context.Background(), pattern, pathVariable, predicate, terms)
}

func compileSelectedPathPlanContext(ctx context.Context, pattern, pathVariable, predicate string, terms []string) (*selectedPathPlan, []string, bool) {
	if ctx.Err() != nil {
		return nil, nil, false
	}
	key := pattern + "\x00" + pathVariable + "\x00" + predicate + "\x00" + strings.Join(terms, "\x00")
	if entry, cached := selectedPathPlans.get(key); cached {
		return entry.plan, entry.filters, entry.ok
	}
	plan, filters, ok := buildSelectedPathPlanContext(ctx, pattern, pathVariable, predicate, terms)
	if ctx.Err() != nil {
		return nil, nil, false
	}
	if selectedPathPlanBytes(plan, filters, len(key)) <= selectedPathPlanCacheEntryBytes {
		selectedPathPlans.put(key, selectedPathPlanEntry{plan: plan, filters: filters, ok: ok})
	}
	return plan, filters, ok
}

// buildSelectedPathPlan compiles a plan (compileSelectedPathPlan).
func buildSelectedPathPlan(pattern, pathVariable, predicate string, terms []string) (*selectedPathPlan, []string, bool) {
	return buildSelectedPathPlanContext(context.Background(), pattern, pathVariable, predicate, terms)
}

func buildSelectedPathPlanContext(ctx context.Context, pattern, pathVariable, predicate string, terms []string) (*selectedPathPlan, []string, bool) {
	if ctx.Err() != nil {
		return nil, nil, false
	}
	items, ok := parsePathPatternItemsContext(ctx, pattern, false)
	if !ok {
		return nil, nil, false
	}
	plan := &selectedPathPlan{iterationChecks: map[int][]string{}, pathVariable: pathVariable}
	generated := 0
	name := func() string {
		generated++
		return generatedVariablePrefix + "ps" + strconv.Itoa(generated)
	}
	// Every node element gets a variable, so its predicates can read it.
	singles := map[string]bool{}
	groups := 0
	for index := range items {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		item := &items[index]
		switch item.kind {
		case pathPatternNode:
			if item.variable == "" {
				item.variable = name()
				item.text = "(" + item.variable + item.text[1:]
				item.predicates = propertyMapPredicatesRenamed(item.predicates, item.variable)
			}
			plan.repeated = plan.repeated || singles[item.variable]
			singles[item.variable] = true
		case pathPatternRelationship:
			if item.varLength {
				item.group = groups
				groups++
				if item.variable != "" {
					plan.listVariables = append(plan.listVariables, item.variable)
				}
			} else if item.variable != "" {
				plan.repeated = plan.repeated || singles[item.variable]
				singles[item.variable] = true
			}
		case pathPatternGroup:
			item.group = groups
			groups++
			plan.compileGroup(item, name)
		}
	}
	// A relationship without a variable but with a property map needs one.
	for index := range items {
		if item := &items[index]; item.kind == pathPatternRelationship && item.variable == "" && len(item.predicates) > 0 {
			item.variable = name()
			item.predicates = propertyMapPredicatesRenamed(item.predicates, item.variable)
			if !item.varLength {
				singles[item.variable] = true
			}
		}
	}
	plan.start, plan.end = items[0], items[len(items)-1]
	patternVariables := func(term string) []string {
		var read []string
		for variable := range singles {
			if readsPatternVariable(term, variable) {
				read = append(read, variable)
			}
		}
		for _, variable := range plan.listVariables {
			if readsPatternVariable(term, variable) {
				read = append(read, variable+"[]")
			}
		}
		if readsPatternVariable(term, pathVariable) {
			read = append(read, pathVariable+"[]")
		}
		return read
	}
	attach := func(term string) {
		read := patternVariables(term)
		switch {
		case len(read) == 0:
			plan.constants = append(plan.constants, term)
		case len(read) == 1 && singles[read[0]]:
			for index := range items {
				if items[index].kind != pathPatternGroup && !items[index].varLength && items[index].variable == read[0] {
					items[index].predicates = append(items[index].predicates, term)
				}
			}
		default:
			plan.pathChecks = append(plan.pathChecks, term)
		}
	}
	// A length bound among the pattern's predicates (shortestPath's WHERE)
	// bounds the search too; each path is still checked against it.
	for _, term := range splitTopLevelAndConjuncts(predicate) {
		if term = strings.TrimSpace(term); term != "" {
			if bound, ok := pathLengthBoundOf(term, pathVariable); ok {
				plan.lengthBounds = append(plan.lengthBounds, bound)
			}
			attach(term)
		}
	}
	// The clause's conjuncts that read only the endpoints select the pairs:
	// they apply as well before the selection as after it.
	var filters []string
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		read := patternVariables(term)
		endpoints := len(read) > 0
		for _, variable := range read {
			endpoints = endpoints && (variable == items[0].variable || variable == items[len(items)-1].variable)
		}
		if endpoints {
			attach(term)
			continue
		}
		if bound, ok := pathLengthBoundOf(term, pathVariable); ok {
			plan.lengthBounds = append(plan.lengthBounds, bound)
		}
		filters = append(filters, term)
	}
	plan.start, plan.end = items[0], items[len(items)-1]
	plan.forward = buildPathSearchAutomatonContext(ctx, items)
	plan.backward = buildPathSearchAutomatonContext(ctx, reversePathPatternItems(items))
	if ctx.Err() != nil {
		return nil, nil, false
	}
	return plan, filters, true
}

// compileGroup names a group's anonymous nodes and sorts the conjuncts of
// its WHERE: one that reads a single element of the group is checked on it
// in each repetition, one that reads none of them on each of its
// relationships, any other on each repetition of a path traced.
func (plan *selectedPathPlan) compileGroup(group *pathPatternItem, name func() string) {
	var elements []string
	for index := range group.items {
		item := &group.items[index]
		if item.variable == "" && len(item.predicates) > 0 {
			item.variable = name()
			item.predicates = propertyMapPredicatesRenamed(item.predicates, item.variable)
		}
		if item.variable != "" {
			elements = append(elements, item.variable)
			plan.listVariables = append(plan.listVariables, item.variable)
		}
	}
	for _, term := range splitTopLevelAndConjuncts(group.where) {
		if term = strings.TrimSpace(term); term == "" {
			continue
		}
		var read []string
		for _, variable := range elements {
			if readsPatternVariable(term, variable) {
				read = append(read, variable)
			}
		}
		switch len(read) {
		case 0:
			for index := range group.items {
				if group.items[index].kind == pathPatternRelationship {
					group.items[index].predicates = append(group.items[index].predicates, term)
				}
			}
		case 1:
			for index := range group.items {
				if group.items[index].variable == read[0] {
					group.items[index].predicates = append(group.items[index].predicates, term)
				}
			}
		default:
			plan.iterationChecks[group.group] = append(plan.iterationChecks[group.group], term)
		}
	}
}

// readsPatternVariable reports whether term reads the variable name: the
// name outside quotes, not a property key (n.name) and not a map key
// ({name: …}); a label test (name:Label) reads it.
func readsPatternVariable(term, name string) bool {
	if name == "" {
		return false
	}
	var brackets []byte
	for i := 0; i < len(term); i++ {
		switch c := term[i]; {
		case isCypherQuoteByte(c):
			i = skipCypherQuotedText(term, i, c) - 1
		case c == '(' || c == '[' || c == '{':
			brackets = append(brackets, c)
		case c == ')' || c == ']' || c == '}':
			if len(brackets) > 0 {
				brackets = brackets[:len(brackets)-1]
			}
		case isIdentByte(c):
			end := i + 1
			for end < len(term) && isIdentByte(term[end]) {
				end++
			}
			if term[i:end] == name && (i == 0 || !isIdentByte(term[i-1]) && term[i-1] != '$') && prevNonSpaceByte(term, i) != '.' &&
				!(nextNonSpaceByte(term, end) == ':' && len(brackets) > 0 && brackets[len(brackets)-1] == '{') {
				return true
			}
			i = end - 1
		}
	}
	return false
}

// propertyMapPredicatesRenamed rewrites property-map predicates written for
// an element without a variable (".`k` = (v)") for variable.
func propertyMapPredicatesRenamed(predicates []string, variable string) []string {
	renamed := make([]string, len(predicates))
	for index, predicate := range predicates {
		if strings.HasPrefix(predicate, ".`") {
			predicate = variable + predicate
		}
		renamed[index] = predicate
	}
	return renamed
}

// pathLengthBoundOf reads a conjunct length(p) <= n, length(p) < n,
// n >= length(p) or n > length(p).
func pathLengthBoundOf(term, pathVariable string) (pathLengthBound, bool) {
	if pathVariable == "" {
		return pathLengthBound{}, false
	}
	for _, operator := range []string{"<=", ">=", "<", ">"} {
		index := topLevelOperatorIndex(term, operator)
		if index < 0 {
			continue
		}
		left, right := strings.TrimSpace(term[:index]), strings.TrimSpace(term[index+len(operator):])
		isLength := func(text string) bool {
			name, inner, ok := parseFunctionCallWS(text)
			return ok && strings.EqualFold(name, "length") && trimQueryTrivia(inner) == pathVariable
		}
		switch {
		case isLength(left) && (operator == "<=" || operator == "<"):
			return pathLengthBound{bound: right, strict: operator == "<"}, !readsPatternVariable(right, pathVariable)
		case isLength(right) && (operator == ">=" || operator == ">"):
			return pathLengthBound{bound: left, strict: operator == ">"}, !readsPatternVariable(left, pathVariable)
		}
		return pathLengthBound{}, false
	}
	return pathLengthBound{}, false
}

// topLevelOperatorIndex is the index of a comparison operator in term
// outside brackets and quotes, -1 when there is none or more than one.
func topLevelOperatorIndex(term, operator string) int {
	found := -1
	depth := 0
	for i := 0; i < len(term); i++ {
		switch c := term[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(term, i, c) - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '<', '>', '=':
			if depth != 0 {
				continue
			}
			end := i + 1
			for end < len(term) && strings.IndexByte("<>=", term[end]) >= 0 {
				end++
			}
			if term[i:end] == operator {
				if found >= 0 {
					return -1
				}
				found = i
			} else if strings.ContainsAny(term[i:end], "<>") {
				return -1
			}
			i = end - 1
		}
	}
	return found
}

// errSelectedPathPattern is the error for a selected pattern the search
// can't read.
func errSelectedPathPattern(pattern string) error {
	return localizedError(localization.CypherMatchingPathPatternInvalid(pattern), nil)
}

// selectPaths returns the rows of a selected pattern's paths for one row,
// before the clause's filters.
func (e *StorageExecutor) selectPaths(ctx context.Context, m *shortestPathMatch, row pipelineRow, count int) ([]pipelineRow, error) {
	plan := m.plan
	maxLength, ok := e.selectedPathLimit(ctx, plan, row)
	if !ok {
		return nil, getExpressionFailure(ctx)
	}
	start, startBound, startOK := boundSelectedEndpoint(row, plan.start.variable)
	end, endBound, endOK := boundSelectedEndpoint(row, plan.end.variable)
	if !startOK || !endOK {
		return nil, nil
	}
	var rows []pipelineRow
	search := func(a *pathSearchAutomaton, reversed bool, source, target *storage.Node, ends []*storage.Node) {
		rows = append(rows, e.searchSelectedPaths(ctx, plan, a, reversed, row, maxLength, source, target, ends, count, m.selector.groups)...)
	}
	switch {
	case startBound && endBound:
		search(plan.forward, false, start, end, nil)
	case startBound:
		search(plan.forward, false, start, nil, nil)
	case endBound:
		search(plan.backward, true, end, nil, nil)
	default:
		// Neither end is bound: the search runs from each node of the
		// narrower end pattern (from both ends when each matches one node).
		var starts, ends []*storage.Node
		var err error
		if selectedEndpointConstrained(plan.start) || !selectedEndpointConstrained(plan.end) {
			if starts, err = e.selectedPathEndpoints(ctx, row, plan.start); err != nil {
				return nil, err
			}
		}
		if selectedEndpointConstrained(plan.end) {
			if ends, err = e.selectedPathEndpoints(ctx, row, plan.end); err != nil {
				return nil, err
			}
		}
		switch {
		case plan.start.variable == plan.end.variable:
			// One node is both ends.
			if starts == nil {
				starts = ends
			}
			for _, source := range starts {
				search(plan.forward, false, source, source, nil)
			}
		case len(starts) == 1 && len(ends) == 1:
			search(plan.forward, false, starts[0], ends[0], nil)
		case ends != nil && (starts == nil || len(ends) < len(starts)):
			for _, source := range ends {
				search(plan.backward, true, source, nil, starts)
			}
		default:
			for _, source := range starts {
				search(plan.forward, false, source, nil, ends)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return rows, getExpressionFailure(ctx)
}

// selectedPathLimit checks a plan's constant conjuncts for row and returns
// the longest path its length bounds allow (-1: none); ok is false when the
// row selects nothing.
func (e *StorageExecutor) selectedPathLimit(ctx context.Context, plan *selectedPathPlan, row pipelineRow) (int, bool) {
	for _, term := range plan.constants {
		if !e.evaluateWithWhereCondition(ctx, term, map[string]interface{}(row)) {
			return 0, false
		}
	}
	maxLength := plan.forward.maxLength
	for _, bound := range plan.lengthBounds {
		value, _ := e.evaluateRowExpressionWithContext(ctx, bound.bound, row)
		if limit, known := pathLengthLimit(value, bound.strict); known {
			if limit < 0 {
				return 0, false
			}
			if maxLength < 0 || limit < maxLength {
				maxLength = limit
			}
		}
	}
	return maxLength, true
}

// searchSelectedPaths runs one search of a plan's automaton a (the
// reversed pattern when reversed) from source for row and returns the rows
// of the count shortest paths (count lengths for groups) to each end node:
// to target only when it is given, to the nodes of ends only when they are.
func (e *StorageExecutor) searchSelectedPaths(ctx context.Context, plan *selectedPathPlan, a *pathSearchAutomaton, reversed bool, row pipelineRow, maxLength int, source, target *storage.Node, ends []*storage.Node, count int, groups bool) []pipelineRow {
	// The search's ID-keyed state uses the same identity view as native graph
	// algorithms. Expression evaluation and output keep the original entities.
	searchExecutor := e
	var view *graphIdentityStorage
	if storage.CompositeDatabaseOf(e.getStorage(ctx)) != "" {
		searchExecutor = e.graphIdentityExecutor(ctx)
		view = searchExecutor.storage.(*graphIdentityStorage)
		source, target = view.node(source), view.node(target)
		if ends != nil {
			selected := make([]*storage.Node, len(ends))
			for i, node := range ends {
				selected[i] = view.node(node)
			}
			ends = selected
		}
	}
	bind := func(path pathSearchPath) (pipelineRow, bool) {
		if view != nil {
			path.start = graphIdentityNodeValue(path.start)
			steps := make([]pathSearchPathStep, len(path.steps))
			for i, step := range path.steps {
				step.node = graphIdentityNodeValue(step.node)
				step.edge = graphIdentityEdgeValue(step.edge)
				steps[i] = step
			}
			path.steps = steps
		}
		return e.bindSelectedPath(ctx, plan, a, reversed, row, path)
	}
	s := searchExecutor.newPathSearch(ctx, a, row, maxLength, source)
	if view != nil {
		s.native = e
	}
	if target != nil {
		direct := len(plan.pathChecks) == 0 && len(plan.iterationChecks) == 0 && !plan.acyclic && !plan.repeated
		for _, variable := range plan.listVariables {
			if _, bound := row[variable]; bound {
				direct = false
			}
		}
		return s.pair(source, target, count, groups, direct, bind)
	}
	if ends != nil {
		s.ends = make(map[storage.NodeID]bool, len(ends))
		for _, node := range ends {
			s.ends[node.ID] = true
		}
	}
	return s.run(source, count, groups, bind)
}

// pathLengthLimit is the longest length a length(p) <= value (strict: <)
// conjunct allows; known is false for a value that isn't a number.
func pathLengthLimit(value interface{}, strict bool) (int, bool) {
	limit, numeric := valueconvert.NumericFloat64(value)
	if !numeric || math.IsNaN(limit) {
		return 0, false
	}
	if strict {
		limit = math.Ceil(limit) - 1
	} else {
		limit = math.Floor(limit)
	}
	if limit > float64(VarLengthUnboundedMaxHops) {
		return -1, false
	}
	return valueconvert.ToInt(limit, valueconvert.IntegerFloat)
}

// boundSelectedEndpoint returns the node a row binds to an endpoint's
// variable; ok is false when the row binds it to something else (null
// included), which selects nothing.
func boundSelectedEndpoint(row pipelineRow, variable string) (*storage.Node, bool, bool) {
	value, exists := row[variable]
	if !exists {
		return nil, false, true
	}
	node, isNode := value.(*storage.Node)
	if !isNode || node == nil {
		return nil, false, false
	}
	return node, true, true
}

// selectedEndpointConstrained reports whether an endpoint's node pattern
// narrows its nodes (labels, properties or predicates).
func selectedEndpointConstrained(item pathPatternItem) bool {
	return len(item.labels) > 0 || len(item.predicates) > 0
}

// selectedPathEndpoints returns the nodes an endpoint's node pattern
// matches for row, with its predicates.
func (e *StorageExecutor) selectedPathEndpoints(ctx context.Context, row pipelineRow, item pathPatternItem) ([]*storage.Node, error) {
	clause := "MATCH " + item.text
	if len(item.predicates) > 0 {
		clause += " WHERE " + strings.Join(item.predicates, " AND ")
	}
	matched, err := e.pipelineMatchRows(ctx, row, clause)
	if err != nil {
		return nil, err
	}
	nodes := make([]*storage.Node, 0, len(matched))
	for _, bound := range matched {
		if node, ok := bound[item.variable].(*storage.Node); ok && node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

// bindSelectedPath returns row with a found path's variables bound: the
// path, its nodes and relationships, and a list of values per repetition
// for the variables inside a group or a variable-length relationship. ok
// is false when the path doesn't hold the pattern's predicates that read
// more than one element. A path found over the reversed pattern (reversed)
// is read backwards.
func (e *StorageExecutor) bindSelectedPath(ctx context.Context, plan *selectedPathPlan, a *pathSearchAutomaton, reversed bool, row pipelineRow, path pathSearchPath) (pipelineRow, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	bound := copyPipelineRow(row)
	lists := make(map[string][]interface{}, len(plan.listVariables))
	for _, variable := range plan.listVariables {
		lists[variable] = []interface{}{}
	}
	iterations := map[int][]pipelineRow{}
	consistent := true
	bind := func(variable string, list bool, group int, value interface{}) {
		switch {
		case variable == "":
		case list:
			lists[variable] = append(lists[variable], value)
			if records := iterations[group]; len(records) > 0 {
				records[len(records)-1][variable] = value
			}
		default:
			if existing, exists := bound[variable]; exists && !sameSelectedEntity(existing, value) {
				consistent = false
			}
			bound[variable] = value
		}
	}
	visit := func(slots []int, node *storage.Node) {
		for index, slot := range slots {
			if index&bfsCancelCheckMask == 0 && ctx.Err() != nil {
				consistent = false
				return
			}
			held := &a.slots[slot]
			if held.iterationStart {
				iterations[held.group] = append(iterations[held.group], pipelineRow{})
			}
			bind(held.variable, held.list, held.group, node)
		}
	}
	nodes := []*storage.Node{path.start}
	relationships := make([]*storage.Edge, 0, len(path.steps))
	visit(path.chain, path.start)
	for _, step := range path.steps {
		if ctx.Err() != nil {
			return nil, false
		}
		move := a.moves[step.move]
		transition := &a.slots[move.from].transitions[move.transition]
		bind(transition.variable, transition.list, transition.group, step.edge)
		visit(a.chains[transition.to][move.chain].slots, step.node)
		nodes = append(nodes, step.node)
		relationships = append(relationships, step.edge)
	}
	if !consistent {
		return nil, false
	}
	if reversed {
		if !reverseSliceContext(ctx, nodes) {
			return nil, false
		}
		if !reverseSliceContext(ctx, relationships) {
			return nil, false
		}
		for _, values := range lists {
			if !reverseSliceContext(ctx, values) {
				return nil, false
			}
		}
		for _, records := range iterations {
			if !reverseSliceContext(ctx, records) {
				return nil, false
			}
		}
	}
	for variable, values := range lists {
		if existing, exists := row[variable]; exists {
			items, ok := toInterfaceSlice(existing)
			if !ok || len(items) != len(values) {
				return nil, false
			}
			for index, value := range values {
				if ctx.Err() != nil || !sameSelectedEntity(items[index], value) {
					return nil, false
				}
			}
			continue
		}
		bound[variable] = values
	}
	bound[plan.pathVariable] = e.pathToMapContext(ctx, PathResult{Nodes: nodes, Relationships: relationships, Length: len(relationships)})
	if plan.acyclic && !distinctNodeIDs(pathResultNodeIDs(&PathResult{Nodes: nodes})) {
		return nil, false
	}
	for group, checks := range plan.iterationChecks {
		for _, record := range iterations[group] {
			if ctx.Err() != nil {
				return nil, false
			}
			iteration := copyPipelineRow(bound)
			for variable, value := range record {
				iteration[variable] = value
			}
			for _, check := range checks {
				if !e.evaluateWithWhereCondition(ctx, check, map[string]interface{}(iteration)) {
					return nil, false
				}
			}
		}
	}
	for _, check := range plan.pathChecks {
		if !e.evaluateWithWhereCondition(ctx, check, map[string]interface{}(bound)) {
			return nil, false
		}
	}
	return bound, ctx.Err() == nil
}

// sameSelectedEntity reports whether two values are the same node or the
// same relationship.
func sameSelectedEntity(a, b interface{}) bool {
	switch left := a.(type) {
	case *storage.Node:
		right, ok := b.(*storage.Node)
		return ok && left != nil && right != nil && left.Identity() == right.Identity()
	case *storage.Edge:
		right, ok := b.(*storage.Edge)
		return ok && left != nil && right != nil && left.Identity() == right.Identity()
	}
	return false
}

// reverseSlice reverses values in place.
func reverseSlice[T any](values []T) {
	reverseSliceContext(context.Background(), values)
}

func reverseSliceContext[T any](ctx context.Context, values []T) bool {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		if i&bfsCancelCheckMask == 0 && ctx.Err() != nil {
			return false
		}
		values[i], values[j] = values[j], values[i]
	}
	return ctx.Err() == nil
}

// Every cached entry has a byte limit as well as the cache's entry limit.
// This upper estimate includes slice capacities and retained query strings.
const selectedPathPlanCacheEntryBytes = 64 << 10

func selectedPathPlanBytes(plan *selectedPathPlan, filters []string, keyBytes int) int {
	size := keyBytes + int(unsafe.Sizeof(selectedPathPlanEntry{}))
	stringsBytes := func(values []string) int {
		n := cap(values) * int(unsafe.Sizeof(""))
		for _, v := range values {
			n += len(v)
		}
		return n
	}
	size += stringsBytes(filters)
	if plan == nil {
		return size
	}
	size += 1024 + 1024*len(plan.iterationChecks) + int(unsafe.Sizeof(*plan)) + len(plan.pathVariable) + stringsBytes(plan.constants) + stringsBytes(plan.pathChecks) + stringsBytes(plan.listVariables)
	for _, checks := range plan.iterationChecks {
		size += 64 + stringsBytes(checks)
	}
	for _, item := range []pathPatternItem{plan.start, plan.end} {
		size += len(item.text) + len(item.variable) + stringsBytes(item.labels) + stringsBytes(item.predicates)
	}
	size += cap(plan.lengthBounds) * int(unsafe.Sizeof(pathLengthBound{}))
	for _, bound := range plan.lengthBounds {
		size += len(bound.bound)
	}
	for _, a := range []*pathSearchAutomaton{plan.forward, plan.backward} {
		size += int(unsafe.Sizeof(*a)) + cap(a.slots)*int(unsafe.Sizeof(pathSearchSlot{})) + cap(a.moves)*int(unsafe.Sizeof(pathSearchMove{})) + cap(a.counters)*int(unsafe.Sizeof(pathSearchCounter{}))
		for _, slot := range a.slots {
			size += len(slot.variable) + stringsBytes(slot.labels) + stringsBytes(slot.predicates) + cap(slot.empty)*int(unsafe.Sizeof(int(0))) + cap(slot.transitions)*int(unsafe.Sizeof(pathSearchTransition{}))
			for _, t := range slot.transitions {
				size += len(t.variable) + len(t.direction) + stringsBytes(t.types) + stringsBytes(t.predicates)
			}
		}
		size += (cap(a.chains) + cap(a.from) + cap(a.into)) * int(unsafe.Sizeof([]int{}))
		for _, chains := range a.chains {
			size += cap(chains) * int(unsafe.Sizeof(pathSearchChain{}))
			for _, chain := range chains {
				size += cap(chain.slots) * int(unsafe.Sizeof(int(0)))
			}
		}
		for _, indexes := range append(append([][]int(nil), a.from...), a.into...) {
			size += cap(indexes) * int(unsafe.Sizeof(int(0)))
		}
	}
	return size
}
