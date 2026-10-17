package cypher

import (
	"context"
	"math"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nornicdb.graph.expandReferences(seeds, config) :: (node, relationship,
// category) returns the closure of the seed nodes and relationships under
// three rules, applied until nothing new is found (a fixed point, so the
// result does not depend on the order things are found in):
//
//   - references: a found node or relationship names nodes by the values of
//     its references properties (a STRING or INTEGER, or a list of them);
//     each name finds every node of each domain label whose idProperty
//     equals it, so one id may find a node in several domains. A name is
//     looked up once.
//   - endpoints: a found relationship brings both its ends.
//   - branches: from each found node (only nodes reached rather than seeded,
//     for a branch with fromSeeds false) the relationships an expand branch
//     follows are found with their other end; a relationship a between
//     branch follows is found once both its ends are, and adds no node.
//     A branch is APOC's relationshipFilter ("TYPE>|<TYPE|TYPE") and a where
//     predicate on relationship, start and end, read as db.retrieve reads
//     its where for relationship candidates.
//
// Cypher can't state the reference rule (no recursion over a property join)
// and apoc.path.* follows relationships by type and label only, so a closure
// such as an export's (the selected items and all they name) took a client
// round trip per round. Seeds are nodes, relationships or seed groups
// {category, nodes, relationships}; a bare node or relationship is in
// category "seed", and an element seeded twice keeps its first category.
// Seed nodes are taken before seed relationships, so a seeded node keeps its
// category when a seeded relationship also reaches it. Everything else is in
// config.category ("reached"). Each element is one row, node or relationship
// null: the seeds in their order, then the rest in the order found. The
// reads are the caller's: an explicit transaction sees its own writes. No
// depth or size limit. A NornicDB extension.

// expandReferencesConfigKeys are the configuration keys
// nornicdb.graph.expandReferences takes.
var expandReferencesConfigKeys = []string{"references", "domains", "idProperty", "expand", "between", "category"}

// referenceBranch is one expand or between entry.
type referenceBranch struct {
	// steps are the relationshipFilter's; none follows every relationship,
	// in both directions.
	steps []apocRelationshipStep
	// where is the compiled where predicate; nil admits every relationship.
	where func(*storage.Edge, *storage.Node, *storage.Node) bool
	// ends: where reads start or end, so they are read for it.
	ends bool
	// fromSeeds: an expand branch is followed from seed nodes too.
	fromSeeds bool
}

// referenceWaiting is a relationship a between branch follows, read from
// its found start, waiting for its end to be found.
type referenceWaiting struct {
	edge   *storage.Edge
	branch *referenceBranch
}

// referenceClosure is one nornicdb.graph.expandReferences call.
type referenceClosure struct {
	e          *StorageExecutor
	references []string
	domains    []string
	idProperty string
	expand     []referenceBranch
	between    []referenceBranch
	category   string

	nodes    map[storage.NodeID]*storage.Node // found
	seeds    map[storage.NodeID]bool          // found as seed nodes
	edges    map[storage.EdgeID]bool          // found
	asked    map[interface{}]bool             // names looked up or pending
	pending  []interface{}                    // names found since the last lookup
	frontier []*storage.Node                  // nodes found since the last branch step

	// byID maps each domain no index serves from id key to its nodes.
	byID map[string]map[interface{}][]*storage.Node
	rows [][]interface{}

	// waiting holds the relationships between branches read from a found
	// start, by the end not found yet.
	waiting map[storage.NodeID][]referenceWaiting
}

// callNornicGraphExpandReferences implements
// nornicdb.graph.expandReferences(seeds, config).
func (e *StorageExecutor) callNornicGraphExpandReferences(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "nornicdb.graph.expandReferences"
	options, err := optionalProcedureMap(procedure, args, 1, "config")
	if err != nil {
		return nil, err
	}
	closure, err := e.newReferenceClosure(ctx, options)
	if err != nil {
		return nil, err
	}
	groups, err := readReferenceSeeds(procedure, procedureArgument(args, 0))
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		for _, node := range group.nodes {
			closure.addNode(node, group.category, true)
		}
	}
	for _, group := range groups {
		for _, edge := range group.relationships {
			closure.addRelationship(edge, group.category)
		}
	}
	if err := closure.run(ctx); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{"node", "relationship", "category"}, Rows: closure.rows}, nil
}

// referenceSeedGroup is seeds of one category.
type referenceSeedGroup struct {
	category      string
	nodes         []*storage.Node
	relationships []*storage.Edge
}

// readReferenceSeeds reads the seeds argument: null (none), a node, a
// relationship, a seed group map, or a list of them; null entries are none.
func readReferenceSeeds(procedure string, value interface{}) ([]referenceSeedGroup, error) {
	items, isList := cypherListValue(value)
	if !isList {
		items = []interface{}{value}
	}
	var groups []referenceSeedGroup
	bare := func() *referenceSeedGroup {
		if len(groups) == 0 || groups[len(groups)-1].category != "seed" {
			groups = append(groups, referenceSeedGroup{category: "seed"})
		}
		return &groups[len(groups)-1]
	}
	for _, item := range items {
		switch typed := item.(type) {
		case nil:
		case *storage.Node:
			if typed != nil {
				group := bare()
				group.nodes = append(group.nodes, typed)
			}
		case *storage.Edge:
			if typed != nil {
				group := bare()
				group.relationships = append(group.relationships, typed)
			}
		case map[string]interface{}:
			group, err := readReferenceSeedGroup(typed)
			if err != nil {
				return nil, err
			}
			groups = append(groups, group)
		default:
			return nil, procedureArgumentTypeError(procedure, "seeds", "LIST<NODE | RELATIONSHIP | MAP>", value)
		}
	}
	return groups, nil
}

// readReferenceSeedGroup reads {category, nodes, relationships}.
func readReferenceSeedGroup(entries map[string]interface{}) (referenceSeedGroup, error) {
	group := referenceSeedGroup{category: "seed"}
	c, err := newGDSConfig(entries, "category", "nodes", "relationships")
	if err != nil {
		return group, err
	}
	if category, given, err := c.text("category"); err != nil {
		return group, err
	} else if given {
		group.category = category
	}
	for _, key := range []string{"nodes", "relationships"} {
		value := c.value(key)
		if value == nil {
			continue
		}
		items, isList := cypherListValue(value)
		if !isList {
			return group, c.typeError(key, "List")
		}
		for _, item := range items {
			switch typed := item.(type) {
			case nil:
			case *storage.Node:
				if key != "nodes" {
					return group, c.typeError(key, "List of Relationship")
				}
				if typed != nil {
					group.nodes = append(group.nodes, typed)
				}
			case *storage.Edge:
				if key != "relationships" {
					return group, c.typeError(key, "List of Node")
				}
				if typed != nil {
					group.relationships = append(group.relationships, typed)
				}
			default:
				if key == "nodes" {
					return group, c.typeError(key, "List of Node")
				}
				return group, c.typeError(key, "List of Relationship")
			}
		}
	}
	return group, nil
}

// newReferenceClosure reads the configuration.
func (e *StorageExecutor) newReferenceClosure(ctx context.Context, options map[string]interface{}) (*referenceClosure, error) {
	closure := &referenceClosure{
		e: e, idProperty: "id", category: "reached",
		nodes: map[storage.NodeID]*storage.Node{}, seeds: map[storage.NodeID]bool{}, edges: map[storage.EdgeID]bool{},
		asked: map[interface{}]bool{}, byID: map[string]map[interface{}][]*storage.Node{},
		waiting: map[storage.NodeID][]referenceWaiting{},
	}
	c, err := newGDSConfig(options, expandReferencesConfigKeys...)
	if err != nil {
		return nil, err
	}
	if closure.references, err = c.stringNames("references", true, false); err != nil {
		return nil, err
	}
	if closure.domains, err = c.stringNames("domains", true, false); err != nil {
		return nil, err
	}
	if property, given, err := c.text("idProperty"); err != nil {
		return nil, err
	} else if given {
		closure.idProperty = property
	}
	if category, given, err := c.text("category"); err != nil {
		return nil, err
	} else if given {
		closure.category = category
	}
	if closure.expand, err = e.readReferenceBranches(ctx, c, "expand"); err != nil {
		return nil, err
	}
	if closure.between, err = e.readReferenceBranches(ctx, c, "between"); err != nil {
		return nil, err
	}
	return closure, nil
}

// readReferenceBranches reads the expand or between list of branch maps.
func (e *StorageExecutor) readReferenceBranches(ctx context.Context, c *gdsConfig, key string) ([]referenceBranch, error) {
	value := c.value(key)
	if value == nil {
		return nil, nil
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, c.typeError(key, "List")
	}
	allowed := []string{"relationshipFilter", "where"}
	if key == "expand" {
		allowed = append(allowed, "fromSeeds")
	}
	branches := make([]referenceBranch, 0, len(items))
	for _, item := range items {
		entries, isMap := item.(map[string]interface{})
		if !isMap {
			return nil, c.typeError(key, "List of Map")
		}
		b, err := newGDSConfig(entries, allowed...)
		if err != nil {
			return nil, err
		}
		branch := referenceBranch{fromSeeds: true}
		filter, _, err := b.text("relationshipFilter")
		if err != nil {
			return nil, err
		}
		branch.steps = parseApocRelationshipFilter(filter)
		where, _, err := b.text("where")
		if err != nil {
			return nil, err
		}
		if where != "" {
			if where, err = desugarRetrievalWhere(ctx, where); err != nil {
				return nil, err
			}
			branch.where = e.compileRelationshipWhereFilter(ctx, where)
			branch.ends = containsIdentifierToken(where, "start") || containsIdentifierToken(where, "end")
		}
		if fromSeeds, given, err := b.boolean("fromSeeds"); err != nil {
			return nil, err
		} else if given {
			branch.fromSeeds = fromSeeds
		}
		branches = append(branches, branch)
	}
	return branches, nil
}

// addNode records node, once, with its row and its names, and admits the
// between relationships that waited for it.
func (r *referenceClosure) addNode(node *storage.Node, category string, seed bool) {
	if r.nodes[node.ID] != nil {
		return
	}
	r.nodes[node.ID] = node
	if seed {
		r.seeds[node.ID] = true
	}
	r.rows = append(r.rows, []interface{}{node, nil, category})
	r.frontier = append(r.frontier, node)
	r.name(node.Properties)
	if waiting := r.waiting[node.ID]; len(waiting) > 0 {
		delete(r.waiting, node.ID)
		for _, entry := range waiting {
			r.admitBetween(entry.edge, entry.branch)
		}
	}
}

// addRelationship records edge, once, after its ends, with its names.
func (r *referenceClosure) addRelationship(edge *storage.Edge, category string) {
	if r.edges[edge.ID] {
		return
	}
	r.edges[edge.ID] = true
	for _, end := range []storage.NodeID{edge.StartNode, edge.EndNode} {
		if r.nodes[end] != nil {
			continue
		}
		if node, err := r.e.getQueryNode(r.e.storage, end); err == nil && node != nil {
			r.addNode(node, r.category, false)
		}
	}
	r.rows = append(r.rows, []interface{}{nil, edge, category})
	r.name(edge.Properties)
}

// name queues the names properties hold under the references properties.
func (r *referenceClosure) name(properties map[string]interface{}) {
	for _, property := range r.references {
		value := properties[property]
		items, isList := cypherListValue(value)
		if !isList {
			items = []interface{}{value}
		}
		for _, item := range items {
			key, ok := referenceKey(item, false)
			if ok && !r.asked[key] {
				r.asked[key] = true
				r.pending = append(r.pending, key)
			}
		}
	}
}

// referenceKey is a value's key for id equality: a string as itself, an
// integer as int64; with numbers, an integral float as the integer it
// equals in Cypher and another float as itself. A name is a STRING or an
// INTEGER (numbers false); an id value may be any of them.
func referenceKey(value interface{}, numbers bool) (interface{}, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case float64:
		if !numbers {
			return nil, false
		}
		if typed == math.Trunc(typed) && math.Abs(typed) < 1<<63 {
			return int64(typed), true
		}
		return typed, true
	case float32:
		return referenceKey(float64(typed), numbers)
	}
	if integer, ok := coerceInt64(value); ok {
		return integer, true
	}
	return nil, false
}

// run applies the rules until nothing new is found.
func (r *referenceClosure) run(ctx context.Context) error {
	for len(r.frontier) > 0 || len(r.pending) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		names := r.pending
		r.pending = nil
		sort.Slice(names, func(i, j int) bool { return referenceKeyLess(names[i], names[j]) })
		for _, name := range names {
			for _, domain := range r.domains {
				nodes, err := r.lookup(domain, name)
				if err != nil {
					return err
				}
				for _, node := range nodes {
					r.addNode(node, r.category, false)
				}
			}
		}
		round := r.frontier
		r.frontier = nil
		for _, node := range round {
			if err := ctx.Err(); err != nil {
				return err
			}
			r.followAll(node)
		}
	}
	return nil
}

// followAll applies node's branches, reading its relationships once in
// each direction one follows. A between branch reads outgoing relationships
// only: whatever its filter's direction, it follows a relationship from one
// end exactly when it follows it from the other (one end sees it outgoing,
// the other incoming), so each is read once, from its start; one whose end
// is not found yet waits for it (addNode).
func (r *referenceClosure) followAll(node *storage.Node) {
	var expand []*referenceBranch
	outgoing, incoming := len(r.between) > 0, false
	for index := range r.expand {
		if branch := &r.expand[index]; branch.fromSeeds || !r.seeds[node.ID] {
			expand = append(expand, branch)
			out, in := apocStepDirections(branch.steps)
			outgoing, incoming = outgoing || out, incoming || in
		}
	}
	var out, in []*storage.Edge
	if outgoing {
		out, _ = r.e.storage.GetOutgoingEdges(node.ID)
	}
	if incoming {
		in, _ = r.e.storage.GetIncomingEdges(node.ID)
	}
	for _, branch := range expand {
		r.expandFrom(node, branch, out, in)
	}
	for index := range r.between {
		branch := &r.between[index]
		for _, edge := range out {
			if r.edges[edge.ID] || !apocStepsMatchType(branch.steps, edge) {
				continue
			}
			if r.nodes[edge.EndNode] == nil {
				r.waiting[edge.EndNode] = append(r.waiting[edge.EndNode], referenceWaiting{edge: edge, branch: branch})
				continue
			}
			r.admitBetween(edge, branch)
		}
	}
}

// apocStepsMatchType reports whether a filter step names edge's type (or
// any type), whatever its direction.
func apocStepsMatchType(steps []apocRelationshipStep, edge *storage.Edge) bool {
	if len(steps) == 0 {
		return true
	}
	for _, step := range steps {
		if step.relType == "" || step.relType == edge.Type {
			return true
		}
	}
	return false
}

// admitBetween finds a between branch's relationship whose ends are both
// found, when its where holds.
func (r *referenceClosure) admitBetween(edge *storage.Edge, branch *referenceBranch) {
	if r.edges[edge.ID] {
		return
	}
	if branch.where != nil && !branch.where(edge, r.nodes[edge.StartNode], r.nodes[edge.EndNode]) {
		return
	}
	r.addRelationship(edge, r.category)
}

// expandFrom finds the relationships an expand branch follows from node
// among its relationships out and in, with their other end. The ends are
// read for where only when it reads start or end.
func (r *referenceClosure) expandFrom(node *storage.Node, branch *referenceBranch, out, in []*storage.Edge) {
	for _, edge := range apocStepsAmong(node.ID, branch.steps, out, in) {
		if r.edges[edge.ID] {
			continue
		}
		if branch.where != nil {
			var start, end *storage.Node
			if branch.ends {
				other := r.nodes[otherEnd(edge, node.ID)]
				if other == nil {
					loaded, err := r.e.getQueryNode(r.e.storage, otherEnd(edge, node.ID))
					if err != nil || loaded == nil {
						continue
					}
					other = loaded
				}
				start, end = node, other
				if edge.StartNode != node.ID {
					start, end = other, node
				}
			}
			if !branch.where(edge, start, end) {
				continue
			}
		}
		r.addRelationship(edge, r.category)
	}
}

// lookup returns the nodes of domain whose idProperty equals name, by node
// id: from an index on (domain, idProperty) when one serves it (MATCH's
// inline-property seek), else from one scan of the domain's nodes.
func (r *referenceClosure) lookup(domain string, name interface{}) ([]*storage.Node, error) {
	props := map[string]interface{}{r.idProperty: name}
	nodes, indexed := r.e.lookupPatternCandidatesUsingPropertyIndex(nodePatternInfo{labels: []string{domain}, properties: props}, r.e.storage)
	if !indexed {
		byID, ok := r.byID[domain]
		if !ok {
			all, err := r.e.storage.GetNodesByLabel(domain)
			if err != nil {
				return nil, err
			}
			byID = make(map[interface{}][]*storage.Node, len(all))
			for _, node := range all {
				if key, ok := referenceKey(node.Properties[r.idProperty], true); ok {
					byID[key] = append(byID[key], node)
				}
			}
			r.byID[domain] = byID
		}
		nodes = byID[name]
	}
	matched := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if mergeNodeHasLabels(node, []string{domain}) && r.e.nodeMatchesProps(node, props) {
			matched = append(matched, node)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })
	return matched, nil
}

// referenceKeyLess orders names: integers before strings, each ascending.
func referenceKeyLess(a, b interface{}) bool {
	as, aText := a.(string)
	bs, bText := b.(string)
	switch {
	case aText && bText:
		return as < bs
	case aText != bText:
		return !aText
	}
	return a.(int64) < b.(int64)
}
