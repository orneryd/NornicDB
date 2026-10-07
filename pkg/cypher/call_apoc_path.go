package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// APOC path expansion (apoc.path.*)
// ========================================
//
// apoc.path.expand, expandConfig, subgraphNodes, subgraphAll and
// spanningTree share one expander (runApocExpansion), configured from the
// procedure's evaluated arguments: the start node(s) bound by MATCH, an
// element id, a $parameter or a list of them, and the config map, inline or
// as a parameter (#907). The rules follow APOC 5.26 and its documentation
// (https://neo4j.com/docs/apoc/current/graph-querying/expand-paths-config/):
//
//   - relationshipFilter "TYPE>|<TYPE|TYPE|>|<": per type a direction
//     (outgoing, incoming, both); a lone > or < is any type in that direction.
//   - labelFilter "+Allow|-Deny|/Terminate|>End": a node with a denylisted
//     label is never part of a path; a termination node ends a path and stops
//     expansion; an end node ends a path and expansion continues past it;
//     when termination or end labels are given, only paths ending at such a
//     node are returned; otherwise every node on a path must carry an
//     allowlisted label (termination and end nodes are exempt). A label
//     without an operator is allowlisted. Precedence: deny, terminate, end,
//     allow. Below minLevel, termination and end nodes neither end nor stop
//     a path (deny and allow still apply).
//   - sequence "Label,RelFilter,Label,…": alternating label and relationship
//     filters that repeat along the path, replacing labelFilter and
//     relationshipFilter. With beginSequenceAtStart (default true) the first
//     label filter is the start node's and the sequence repeats whole; with
//     it false the first relationship filter is the first step's only, and
//     the remaining relationship filters repeat. A step the sequence has no
//     relationship filter for fails, as in APOC.
//   - endNodes / terminatorNodes / allowlistNodes / denylistNodes: the same
//     roles given as node lists.
//   - minLevel / maxLevel (-1: none), limit (-1: none), bfs, filterStartNode
//     (the label and node filters apply to the start node too), optional (a
//     start with no result yields a null).
//   - uniqueness: RELATIONSHIP_PATH for expand / expandConfig (configurable
//     for expandConfig), NODE_GLOBAL for subgraphNodes, subgraphAll and
//     spanningTree, which accept a minLevel of 0 or 1 only.
//
// Relationships are followed in NornicDB's storage order. Neo4j follows its
// own store's order (newest first on a node with few relationships), which it
// doesn't specify; where only one of several paths is kept (spanningTree,
// NODE_GLOBAL, limit, bfs: false), the path kept can differ from Neo4j's.

// apocRelationshipStep is one relationshipFilter entry: a type ("" for any)
// and the direction it is followed in.
type apocRelationshipStep struct {
	relType   string
	direction string // "out", "in" or "both"
}

// apocLabelFilter is one labelFilter: the labels of each role.
type apocLabelFilter struct {
	allow, deny, terminate, end map[string]bool
}

// endsPaths reports whether the filter names termination or end labels, so
// that only paths ending at such a node are returned.
func (f apocLabelFilter) endsPaths() bool {
	return f.terminate != nil || f.end != nil
}

// apocSequence is a sequence config: label filters and relationship filters
// in the order they repeat (see the comment above).
type apocSequence struct {
	labels        []apocLabelFilter
	relationships [][]apocRelationshipStep
	beginAtStart  bool
}

// apocExpansion is one apoc.path.* request.
type apocExpansion struct {
	starts          []*storage.Node
	minLevel        int
	maxLevel        int // -1: no maximum; below -1: no path
	relationships   []apocRelationshipStep
	labels          apocLabelFilter
	sequence        *apocSequence
	allowNodes      map[storage.NodeID]bool
	denyNodes       map[storage.NodeID]bool
	terminatorNodes map[storage.NodeID]bool
	endNodes        map[storage.NodeID]bool
	endsOnly        bool // only paths ending at a termination or end node
	uniqueness      string
	bfs             bool
	filterStartNode bool
	limit           int // -1: no limit
	optional        bool
}

func newApocExpansion(uniqueness string) *apocExpansion {
	return &apocExpansion{minLevel: 0, maxLevel: -1, uniqueness: uniqueness, bfs: true, limit: -1}
}

// apocNodes reads an apoc.path node argument (the start, or a config node
// list): a node, an element id, or a list of them; null is none. An id that
// matches no node fails, as in APOC.
func (e *StorageExecutor) apocNodes(argument string, value interface{}) ([]*storage.Node, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case []interface{}:
		var nodes []*storage.Node
		for _, item := range typed {
			more, err := e.apocNodes(argument, item)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, more...)
		}
		return nodes, nil
	case *storage.Node:
		if typed == nil {
			return nil, nil
		}
		return []*storage.Node{typed}, nil
	case string:
		node, err := e.storage.GetNode(storage.NodeID(normalizeNodeIDValue(typed).(string)))
		if err != nil || node == nil {
			return nil, localizedError(localization.CypherCoreApocPathNodeNotFound(argument, typed), nil)
		}
		return []*storage.Node{node}, nil
	}
	return nil, localizedError(localization.CypherCoreApocPathNodeArgument(argument, neo4jProvidedValue(value)), nil)
}

// apocInteger reads an apoc.path integer setting as APOC does: a number,
// truncated, or a string holding one; anything else, null included, fails.
func apocInteger(key string, value interface{}) (int, error) {
	if integer, ok := coerceInt64(value); ok {
		return int(integer), nil
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), nil
	case string:
		if number, err := strconv.ParseFloat(typed, 64); err == nil {
			return int(number), nil
		}
	}
	return 0, localizedError(localization.CypherCoreApocPathConfigNumber(key, neo4jProvidedValue(value)), nil)
}

// apocString reads an apoc.path string setting; a value of another type
// fails, as in APOC.
func apocString(key string, value interface{}) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", localizedError(localization.CypherCoreApocPathConfigString(key, neo4jProvidedValue(value)), nil)
	}
	return text, nil
}

// apocBoolean reads an apoc.path boolean setting as APOC does: null, false,
// 0 and the strings "", "false", "no" and "0" are false, anything else true.
func apocBoolean(value interface{}) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		switch strings.ToLower(typed) {
		case "", "false", "no", "0":
			return false
		}
		return true
	}
	if number, ok := coerceInt64(value); ok {
		return number != 0
	}
	if number, ok := value.(float64); ok {
		return int64(number) != 0
	}
	return true
}

// configure applies an apoc.path config map. As in APOC, null is false for
// a boolean setting, fails for minLevel, maxLevel and limit, and leaves any
// other setting at its default; uniqueness is read only when readUniqueness
// (expandConfig), and a limit below -1 fails.
func (x *apocExpansion) configure(e *StorageExecutor, config map[string]interface{}, readUniqueness bool) error {
	sequence, beginAtStart := "", true
	for key, value := range config {
		switch key {
		case "minLevel", "maxLevel", "limit":
			number, err := apocInteger(key, value)
			if err != nil {
				return err
			}
			switch key {
			case "minLevel":
				x.minLevel = max(number, 0)
			case "maxLevel":
				x.maxLevel = number
			default:
				if number < -1 {
					return localizedError(localization.CypherCoreApocPathLimit(number), nil)
				}
				x.limit = number
			}
			continue
		case "beginSequenceAtStart":
			beginAtStart = apocBoolean(value)
			continue
		case "bfs":
			x.bfs = apocBoolean(value)
			continue
		case "filterStartNode":
			x.filterStartNode = apocBoolean(value)
			continue
		case "optional":
			x.optional = apocBoolean(value)
			continue
		}
		if value == nil || (key == "uniqueness" && !readUniqueness) {
			continue
		}
		switch key {
		case "relationshipFilter", "labelFilter", "sequence", "uniqueness":
			text, err := apocString(key, value)
			if err != nil {
				return err
			}
			switch key {
			case "relationshipFilter":
				x.relationships = parseApocRelationshipFilter(text)
			case "labelFilter":
				labels, err := parseApocLabelFilter(text)
				if err != nil {
					return err
				}
				x.labels = labels
			case "sequence":
				sequence = text
			default:
				x.uniqueness = strings.ToUpper(text)
			}
		case "endNodes", "terminatorNodes", "allowlistNodes", "whitelistNodes", "denylistNodes", "blacklistNodes":
			nodes, err := e.apocNodes(key, value)
			if err != nil {
				return err
			}
			set := make(map[storage.NodeID]bool, len(nodes))
			for _, node := range nodes {
				set[node.ID] = true
			}
			switch key {
			case "endNodes":
				x.endNodes = set
			case "terminatorNodes":
				x.terminatorNodes = set
			case "allowlistNodes", "whitelistNodes":
				x.allowNodes = set
			default:
				x.denyNodes = set
			}
		}
	}
	x.endsOnly = x.endNodes != nil || x.terminatorNodes != nil
	if sequence != "" {
		parsed, err := parseApocSequence(sequence, beginAtStart)
		if err != nil {
			return err
		}
		x.sequence = parsed
		for _, labels := range x.sequence.labels {
			x.endsOnly = x.endsOnly || labels.endsPaths()
		}
	} else {
		x.endsOnly = x.endsOnly || x.labels.endsPaths()
	}
	return nil
}

// parseApocRelationshipFilter reads "TYPE>|<TYPE|TYPE|>|<". As in APOC,
// the direction mark may stand on either side of the type (">TYPE" is
// outgoing, "TYPE<" incoming); with marks on both sides the leading one
// decides ("<TYPE>" is incoming).
func parseApocRelationshipFilter(filter string) []apocRelationshipStep {
	var steps []apocRelationshipStep
	for _, part := range strings.Split(filter, "|") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		step := apocRelationshipStep{direction: "both"}
		marks := map[byte]string{'<': "in", '>': "out"}
		if direction, ok := marks[part[len(part)-1]]; ok {
			step.direction, part = direction, part[:len(part)-1]
		}
		if part != "" {
			if direction, ok := marks[part[0]]; ok {
				step.direction, part = direction, part[1:]
			}
		}
		step.relType = strings.TrimSpace(part)
		steps = append(steps, step)
	}
	return steps
}

// parseApocLabelFilter reads "+Allow|-Deny|/Terminate|>End". An operator
// without a label fails, as in APOC.
func parseApocLabelFilter(filter string) (apocLabelFilter, error) {
	var labels apocLabelFilter
	add := func(set *map[string]bool, label string) {
		if *set == nil {
			*set = make(map[string]bool)
		}
		(*set)[label] = true
	}
	for _, part := range strings.Split(filter, "|") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if len(part) == 1 && strings.ContainsRune("+-/>", rune(part[0])) {
			return apocLabelFilter{}, localizedError(localization.CypherCoreApocPathLabelFilterEmpty(part), nil)
		}
		switch part[0] {
		case '-':
			add(&labels.deny, part[1:])
		case '/':
			add(&labels.terminate, part[1:])
		case '>':
			add(&labels.end, part[1:])
		case '+':
			add(&labels.allow, part[1:])
		default:
			add(&labels.allow, part)
		}
	}
	return labels, nil
}

// parseApocSequence reads a sequence config: comma-separated filters that
// alternate between labels and relationships, starting with labels when
// beginAtStart. As APOC splits it, trailing empty entries are dropped
// ("A,R>," is one label filter and one relationship filter).
func parseApocSequence(sequence string, beginAtStart bool) (*apocSequence, error) {
	parsed := &apocSequence{beginAtStart: beginAtStart}
	parts := strings.Split(sequence, ",")
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	for index, part := range parts {
		if (index%2 == 0) != beginAtStart {
			parsed.relationships = append(parsed.relationships, parseApocRelationshipFilter(part))
			continue
		}
		labels, err := parseApocLabelFilter(part)
		if err != nil {
			return nil, err
		}
		parsed.labels = append(parsed.labels, labels)
	}
	return parsed, nil
}

// labelsAt returns the label filter for a node at depth, or an error when
// the sequence has none for it (one with only relationship filters).
func (x *apocExpansion) labelsAt(depth int) (apocLabelFilter, error) {
	if x.sequence == nil {
		return x.labels, nil
	}
	labels := x.sequence.labels
	if !x.sequence.beginAtStart {
		if depth == 0 {
			return apocLabelFilter{}, nil
		}
		depth--
	}
	if len(labels) == 0 {
		return apocLabelFilter{}, localizedError(localization.CypherCoreApocPathSequenceLabel(depth+1), nil)
	}
	return labels[depth%len(labels)], nil
}

// relationshipsAt returns the relationship filter for step (1 for the
// start node's relationships), or an error when the sequence has none for it.
func (x *apocExpansion) relationshipsAt(step int) ([]apocRelationshipStep, error) {
	if x.sequence == nil {
		return x.relationships, nil
	}
	relationships := x.sequence.relationships
	if !x.sequence.beginAtStart {
		if step == 1 && len(relationships) > 0 {
			return relationships[0], nil
		}
		relationships, step = relationships[min(1, len(relationships)):], step-1
	}
	if len(relationships) == 0 {
		return nil, localizedError(localization.CypherCoreApocPathSequenceRelationship(step), nil)
	}
	return relationships[(step-1)%len(relationships)], nil
}

func nodeHasLabelIn(node *storage.Node, labels map[string]bool) bool {
	for _, label := range node.Labels {
		if labels[label] {
			return true
		}
	}
	return false
}

// verdict is how the filters treat a node reached at depth: whether a path
// ending there is returned, and whether expansion continues past it.
func (x *apocExpansion) verdict(node *storage.Node, depth int) (include, expand bool, err error) {
	if x.maxLevel < -1 {
		return false, false, nil
	}
	include, expand = depth >= x.minLevel, x.maxLevel < 0 || depth < x.maxLevel
	if depth == 0 && !x.filterStartNode {
		return include && !x.endsOnly, expand, nil
	}
	labels, err := x.labelsAt(depth)
	if err != nil {
		return false, false, err
	}
	if x.denyNodes[node.ID] || nodeHasLabelIn(node, labels.deny) {
		return false, false, nil
	}
	if x.terminatorNodes[node.ID] || nodeHasLabelIn(node, labels.terminate) {
		if depth < x.minLevel {
			return false, expand, nil
		}
		return include, false, nil
	}
	if x.endNodes[node.ID] || nodeHasLabelIn(node, labels.end) {
		return include, expand, nil
	}
	if (labels.allow != nil && !nodeHasLabelIn(node, labels.allow)) || (x.allowNodes != nil && !x.allowNodes[node.ID]) {
		return false, false, nil
	}
	return include && !x.endsOnly, expand, nil
}

// apocSteps lists the relationships the filter steps follow from node.
func (e *StorageExecutor) apocSteps(node *storage.Node, steps []apocRelationshipStep) []*storage.Edge {
	if len(steps) == 0 {
		steps = []apocRelationshipStep{{direction: "both"}}
	}
	out, _ := e.storage.GetOutgoingEdges(node.ID)
	in, _ := e.storage.GetIncomingEdges(node.ID)
	var edges []*storage.Edge
	seen := make(map[storage.EdgeID]bool)
	take := func(edge *storage.Edge, step apocRelationshipStep) {
		if step.follows(edge, node.ID) && !seen[edge.ID] {
			seen[edge.ID] = true
			edges = append(edges, edge)
		}
	}
	for _, step := range steps {
		if step.direction != "in" {
			for _, edge := range out {
				take(edge, step)
			}
		}
		if step.direction != "out" {
			for _, edge := range in {
				take(edge, step)
			}
		}
	}
	return edges
}

// follows reports whether this filter entry follows edge from the node
// from: its type matches (any, for "") and, for a direction, from is the
// edge's start (out) or end (in).
func (step apocRelationshipStep) follows(edge *storage.Edge, from storage.NodeID) bool {
	if step.relType != "" && edge.Type != step.relType {
		return false
	}
	switch step.direction {
	case "out":
		return edge.StartNode == from
	case "in":
		return edge.EndNode == from
	}
	return true
}

// apocStep is the end of a path the expander walks: its last node and the
// relationship it arrived by, linked to the path one step shorter. The
// frontier shares each path's prefix; only a returned path is copied out
// (path).
type apocStep struct {
	node   *storage.Node
	edge   *storage.Edge // nil at the start node
	parent *apocStep
	depth  int
}

// path copies the steps out as a PathResult, start node first.
func (step *apocStep) path() PathResult {
	path := PathResult{Nodes: make([]*storage.Node, step.depth+1), Relationships: make([]*storage.Edge, step.depth), Length: step.depth}
	for at := step; at != nil; at = at.parent {
		path.Nodes[at.depth] = at.node
		if at.edge != nil {
			path.Relationships[at.depth-1] = at.edge
		}
	}
	return path
}

// runApocExpansion expands every start node and returns the ends of the
// paths the filters include, in BFS or DFS order, at most limit of them.
func (e *StorageExecutor) runApocExpansion(ctx context.Context, x *apocExpansion) ([]*apocStep, error) {
	var results []*apocStep
	if x.limit == 0 {
		return results, nil
	}
	globalNodes := make(map[storage.NodeID]bool)
	globalRelationships := make(map[storage.EdgeID]bool)
	for _, start := range x.starts {
		// Every start node starts its own path, even one already reached.
		frontier := []*apocStep{{node: start}}
		globalNodes[start.ID] = true
		for len(frontier) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var step *apocStep
			if x.bfs {
				step, frontier = frontier[0], frontier[1:]
			} else {
				step, frontier = frontier[len(frontier)-1], frontier[:len(frontier)-1]
			}
			include, expand, err := x.verdict(step.node, step.depth)
			if err != nil {
				return nil, err
			}
			if include {
				results = append(results, step)
				if x.limit >= 0 && len(results) >= x.limit {
					return results, nil
				}
			}
			if !expand {
				continue
			}
			filter, err := x.relationshipsAt(step.depth + 1)
			if err != nil {
				return nil, err
			}
			edges := e.apocSteps(step.node, filter)
			next := make([]*apocStep, 0, len(edges))
			for _, edge := range edges {
				// A path never turns straight back along the relationship
				// it arrived by, whatever the uniqueness, as in Neo4j.
				if step.edge != nil && step.edge.ID == edge.ID {
					continue
				}
				otherID := edge.EndNode
				if otherID == step.node.ID {
					otherID = edge.StartNode
				}
				if !apocUnique(x.uniqueness, step, edge, otherID, globalNodes, globalRelationships) {
					continue
				}
				other, err := e.storage.GetNode(otherID)
				if err != nil || other == nil {
					continue
				}
				switch x.uniqueness {
				case "NODE_GLOBAL":
					globalNodes[otherID] = true
				case "RELATIONSHIP_GLOBAL":
					globalRelationships[edge.ID] = true
				}
				next = append(next, &apocStep{node: other, edge: edge, parent: step, depth: step.depth + 1})
			}
			if x.bfs {
				frontier = append(frontier, next...)
			} else {
				for index := len(next) - 1; index >= 0; index-- {
					frontier = append(frontier, next[index])
				}
			}
		}
	}
	return results, nil
}

// apocUnique applies the uniqueness rule to following edge from the path
// ending at step to otherID.
func apocUnique(uniqueness string, step *apocStep, edge *storage.Edge, otherID storage.NodeID, globalNodes map[storage.NodeID]bool, globalRelationships map[storage.EdgeID]bool) bool {
	switch uniqueness {
	case "NODE_GLOBAL":
		return !globalNodes[otherID]
	case "RELATIONSHIP_GLOBAL":
		return !globalRelationships[edge.ID]
	case "NODE_PATH":
		for at := step; at != nil; at = at.parent {
			if at.node.ID == otherID {
				return false
			}
		}
		return true
	case "NONE":
		return true
	default: // RELATIONSHIP_PATH
		for at := step; at.edge != nil; at = at.parent {
			if at.edge.ID == edge.ID {
				return false
			}
		}
		return true
	}
}

// apocPathProcedure describes how an apoc.path procedure reads its config.
type apocPathProcedure struct {
	name string
	// uniqueness is the procedure's uniqueness rule; configurable lets the
	// config's uniqueness replace it (expandConfig).
	uniqueness   string
	configurable bool
	// oneVisitPerNode procedures (subgraphNodes, subgraphAll, spanningTree)
	// accept a minLevel of 0 or 1 only.
	oneVisitPerNode bool
}

var (
	apocPathExpandProcedure        = apocPathProcedure{name: "expand", uniqueness: "RELATIONSHIP_PATH"}
	apocPathExpandConfigProcedure  = apocPathProcedure{name: "expandConfig", uniqueness: "RELATIONSHIP_PATH", configurable: true}
	apocPathSubgraphNodesProcedure = apocPathProcedure{name: "subgraphNodes", uniqueness: "NODE_GLOBAL", oneVisitPerNode: true}
	apocPathSubgraphAllProcedure   = apocPathProcedure{name: "subgraphAll", uniqueness: "NODE_GLOBAL", oneVisitPerNode: true}
	apocPathSpanningTreeProcedure  = apocPathProcedure{name: "spanningTree", uniqueness: "NODE_GLOBAL", oneVisitPerNode: true}
)

// apocExpansionFromArguments builds the expansion of a (start, config)
// procedure call from its evaluated arguments.
func (e *StorageExecutor) apocExpansionFromArguments(args []interface{}, procedure apocPathProcedure) (*apocExpansion, error) {
	x := newApocExpansion(procedure.uniqueness)
	if len(args) > 0 {
		starts, err := e.apocNodes("the start node", args[0])
		if err != nil {
			return nil, err
		}
		x.starts = starts
	}
	if len(args) > 1 && args[1] != nil {
		config, ok := args[1].(map[string]interface{})
		if !ok {
			return nil, localizedError(localization.CypherCoreApocPathConfigNotMap(neo4jProvidedValue(args[1])), nil)
		}
		// These take an integer minLevel of 0 or 1 only: 1.0 and '1' fail.
		if value, given := config["minLevel"]; given && procedure.oneVisitPerNode {
			if level, ok := coerceInt64(value); !ok || (level != 0 && level != 1) {
				return nil, localizedError(localization.CypherCoreApocPathMinLevel(procedure.name), nil)
			}
		}
		if err := x.configure(e, config, procedure.configurable); err != nil {
			return nil, err
		}
	}
	return x, nil
}

func (e *StorageExecutor) apocPathRows(ctx context.Context, x *apocExpansion, row func(*apocStep) []interface{}, columns ...string) (*ExecuteResult, error) {
	ends, err := e.runApocExpansion(ctx, x)
	if err != nil {
		return nil, err
	}
	result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, len(ends))}
	for _, end := range ends {
		result.Rows = append(result.Rows, row(end))
	}
	if len(result.Rows) == 0 && x.optional {
		nulls := make([]interface{}, len(columns))
		result.Rows = append(result.Rows, nulls)
	}
	return result, nil
}

// callApocPathExpandConfig implements apoc.path.expandConfig(start, config)
// :: (path).
func (e *StorageExecutor) callApocPathExpandConfig(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	x, err := e.apocExpansionFromArguments(args, apocPathExpandConfigProcedure)
	if err != nil {
		return nil, err
	}
	return e.apocPathRows(ctx, x, func(end *apocStep) []interface{} { return []interface{}{e.pathToMap(end.path())} }, "path")
}

// callApocPathExpand implements apoc.path.expand(start, relationshipFilter,
// labelFilter, minLevel, maxLevel) :: (path).
func (e *StorageExecutor) callApocPathExpand(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	// Null filters are none; null levels fail, as in APOC.
	config := map[string]interface{}{}
	for index, key := range []string{"", "relationshipFilter", "labelFilter", "minLevel", "maxLevel"} {
		if index > 0 && index < len(args) && (args[index] != nil || index > 2) {
			config[key] = args[index]
		}
	}
	start := []interface{}{nil, config}
	if len(args) > 0 {
		start[0] = args[0]
	}
	x, err := e.apocExpansionFromArguments(start, apocPathExpandProcedure)
	if err != nil {
		return nil, err
	}
	return e.apocPathRows(ctx, x, func(end *apocStep) []interface{} { return []interface{}{e.pathToMap(end.path())} }, "path")
}

// callApocPathSubgraphNodes implements apoc.path.subgraphNodes(start,
// config) :: (node).
func (e *StorageExecutor) callApocPathSubgraphNodes(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	x, err := e.apocExpansionFromArguments(args, apocPathSubgraphNodesProcedure)
	if err != nil {
		return nil, err
	}
	return e.apocPathRows(ctx, x, func(end *apocStep) []interface{} { return []interface{}{end.node} }, "node")
}

// callApocPathSpanningTree implements apoc.path.spanningTree(start, config)
// :: (path): a path from the start to every node reached once.
func (e *StorageExecutor) callApocPathSpanningTree(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	x, err := e.apocExpansionFromArguments(args, apocPathSpanningTreeProcedure)
	if err != nil {
		return nil, err
	}
	return e.apocPathRows(ctx, x, func(end *apocStep) []interface{} { return []interface{}{e.pathToMap(end.path())} }, "path")
}

// callApocPathSubgraphAll implements apoc.path.subgraphAll(start, config) ::
// (nodes, relationships): the nodes subgraphNodes returns and every
// relationship between two of them.
func (e *StorageExecutor) callApocPathSubgraphAll(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	x, err := e.apocExpansionFromArguments(args, apocPathSubgraphAllProcedure)
	if err != nil {
		return nil, err
	}
	ends, err := e.runApocExpansion(ctx, x)
	if err != nil {
		return nil, err
	}
	nodes := make([]interface{}, 0, len(ends))
	members := make(map[storage.NodeID]bool, len(ends))
	for _, end := range ends {
		node := end.node
		if !members[node.ID] {
			members[node.ID] = true
			nodes = append(nodes, node)
		}
	}
	relationships := make([]interface{}, 0)
	for _, value := range nodes {
		edges, _ := e.storage.GetOutgoingEdges(value.(*storage.Node).ID)
		for _, edge := range edges {
			if members[edge.EndNode] {
				relationships = append(relationships, edge)
			}
		}
	}
	// One row, with empty lists when nothing is reached, optional or not,
	// as in APOC.
	return &ExecuteResult{Columns: []string{"nodes", "relationships"}, Rows: [][]interface{}{{nodes, relationships}}}, nil
}
