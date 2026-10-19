package cypher

import (
	"fmt"
	"math"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GDS's functions (#1161): the topological link-prediction functions
// (gds.alpha.linkprediction.*), the gds.util helpers and gds.version(), with
// GDS 2.13's names, arguments and values. NornicDB's own
// gds.linkPrediction.*.stream procedures stay as extensions.
func init() {
	// One scoring for these functions and the gds.linkPrediction.*.stream
	// procedures: linkpredict.PairScore (#1192).
	for name, algorithm := range map[string]string{
		"adamicadar":             "adamic_adar",
		"commonneighbors":        "common_neighbors",
		"preferentialattachment": "preferential_attachment",
		"resourceallocation":     "resource_allocation",
		"totalneighbors":         "total_neighbors",
	} {
		cypherfn.Register("gds.alpha.linkprediction."+name, gdsLinkPrediction("gds.alpha.linkprediction."+name, algorithm))
	}
	cypherfn.Register("gds.alpha.linkprediction.samecommunity", fnGDSSameCommunity)
	cypherfn.Register("gds.util.asnode", fnGDSAsNode)
	cypherfn.Register("gds.util.asnodes", fnGDSAsNodes)
	cypherfn.Register("gds.util.nan", gdsConstant("gds.util.NaN", math.NaN()))
	cypherfn.Register("gds.util.infinity", gdsConstant("gds.util.infinity", math.Inf(1)))
	cypherfn.Register("gds.util.isfinite", fnGDSIsFinite(true))
	cypherfn.Register("gds.util.isinfinite", fnGDSIsFinite(false))
	cypherfn.Register("gds.version", gdsConstant("gds.version", gdsVersion))
	cypherfn.Register("gds.graph.exists", fnGDSGraphExists)
	cypherfn.Register("gds.util.nodeproperty", fnGDSNodeProperty)
}

// fnGDSGraphExists is gds.graph.exists(graphName): whether this database's
// catalog holds the graph (#1161).
func fnGDSGraphExists(ctx cypherfn.Context, args []string) (interface{}, error) {
	if err := gdsArity("gds.graph.exists", args, 1, 1); err != nil {
		return nil, err
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	name, ok := values[0].(string)
	if !ok {
		return nil, fmt.Errorf("gds.graph.exists expects a graph name, got %s", cypherTypeSystemName(values[0]))
	}
	_, missing := lookupGraphProjection(graphFunctionDatabase(ctx), name)
	return missing == nil, nil
}

// fnGDSNodeProperty is gds.util.nodeProperty(graphName, nodeId, propertyKey,
// nodeLabel = '*'): the node's value of a projected property in the graph,
// NaN when it has none, as in GDS (#1161). A node the graph doesn't hold
// gives null.
func fnGDSNodeProperty(ctx cypherfn.Context, args []string) (interface{}, error) {
	const name = "gds.util.nodeProperty"
	if err := gdsArity(name, args, 3, 4); err != nil {
		return nil, err
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	graph, ok := values[0].(string)
	if !ok {
		return nil, fmt.Errorf("%s expects a graph name as argument 1, got %s", name, cypherTypeSystemName(values[0]))
	}
	key, ok := values[2].(string)
	if !ok {
		return nil, fmt.Errorf("%s expects a property key as argument 3, got %s", name, cypherTypeSystemName(values[2]))
	}
	label := "*"
	if len(values) == 4 {
		if label, ok = values[3].(string); !ok {
			return nil, fmt.Errorf("%s expects a node label as argument 4, got %s", name, cypherTypeSystemName(values[3]))
		}
	}
	projection, err := lookupGraphProjection(graphFunctionDatabase(ctx), graph)
	if err != nil {
		return nil, err
	}
	projected := false
	for projectedLabel, keys := range projection.NodePropertyKeys {
		if (label == "*" || label == projectedLabel) && containsString(keys, key) {
			projected = true
		}
	}
	if !projected {
		return nil, localizedError(localization.CypherGraphProceduresNodePropertyNotInGraph(key), nil)
	}
	id, ok := projection.nodeReference(values[1])
	if !ok {
		return nil, fmt.Errorf("%s expects a node or node id as argument 2, got %s", name, cypherTypeSystemName(values[1]))
	}
	if _, inGraph := projection.nodeIndex[string(id)]; !inGraph || !projection.matchesNodeLabels(string(id), []string{label}) {
		return nil, nil
	}
	// GDS's utility reads scalars as Double and arrays in their projected
	// element type. Every value comes from the projection's typed reader.
	kind := projection.NodePropertyTypes[key]
	if !strings.HasPrefix(kind, "List") {
		kind = "Float"
	}
	value, err := projection.nodePropertyValue(string(id), key, kind)
	if err != nil {
		return nil, err
	}
	if list, ok := cypherListValue(value); ok {
		return list, nil
	}
	return value, nil
}

// gdsConstant is a GDS function of no arguments that returns value.
func gdsConstant(name string, value interface{}) cypherfn.Func {
	return func(_ cypherfn.Context, args []string) (interface{}, error) {
		if err := gdsArity(name, args, 0, 0); err != nil {
			return nil, err
		}
		return value, nil
	}
}

// gdsArity is Neo4j's parameter-count error for a GDS function called with
// fewer than minimum or more than maximum arguments. An empty argument list
// may arrive as one empty argument.
func gdsArity(name string, args []string, minimum, maximum int) error {
	count := len(args)
	if count == 1 && strings.TrimSpace(args[0]) == "" {
		count = 0
	}
	if count < minimum || count > maximum {
		return &cypherfn.ParameterCountError{Function: name, TooMany: count > maximum}
	}
	return nil
}

// isGDSFunctionName reports whether a lower-cased name is one of GDS's
// functions, which the function registry evaluates for both evaluators.
func isGDSFunctionName(name string) bool {
	return strings.HasPrefix(name, "gds.alpha.linkprediction.") || strings.HasPrefix(name, "gds.util.") || name == "gds.version"
}

// gdsVersion is what gds.version() reports, the function and the procedure.
const gdsVersion = "2.6.0-nornicdb"

// gdsNeighborhood is a node's neighbours for link prediction, as GDS counts
// them (checked against GDS 2.13.12): each distinct other node joined to it by
// a relationship of the configured type and direction, and its degree, the
// number of those relationships (a parallel relationship counts again, a
// self-loop once).
type gdsNeighborhood struct {
	nodes  map[storage.NodeID]struct{}
	degree int
	// same marks a pair of one node with itself.
	same bool
}

// gdsLinkConfig is a link-prediction function's config map: relationshipQuery
// (one relationship type; every type when absent) and direction (BOTH,
// OUTGOING or INCOMING; BOTH when absent). As in GDS, other keys are ignored,
// and a direction other than BOTH or INCOMING reads outgoing relationships.
type gdsLinkConfig struct {
	relationshipType string
	direction        string
}

func gdsLinkPrediction(name, algorithm string) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if err := gdsArity(name, args, 2, 3); err != nil {
			return nil, err
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		first, second, err := gdsNodePair(name, values)
		if err != nil || first == nil || second == nil {
			return nil, err
		}
		config, err := gdsLinkPredictionConfig(name, values)
		if err != nil {
			return nil, err
		}
		store,err := ctx.GraphStorage()
 if err != nil { return nil,err }
		if store != nil {
			store = graphIdentityView(store)
		}
		firstID, secondID := storage.NodeID(first.IdentityKey()), storage.NodeID(second.IdentityKey())
		left, err := gdsNeighbors(store, firstID, config)
		if err != nil {
			return nil, err
		}
		right, err := gdsNeighbors(store, secondID, config)
		if err != nil {
			return nil, err
		}
		// A node and itself share no neighbours, as GDS scores them.
		left.same = firstID == secondID
		// The common neighbours' degrees, for Adamic-Adar and resource
		// allocation; a failed read is the statement's error, not an empty
		// neighbourhood.
		degrees := map[storage.NodeID]int{}
		for _, id := range gdsCommon(left, right) {
			neighbors, err := gdsNeighbors(store, id, config)
			if err != nil {
				return nil, err
			}
			degrees[id] = neighbors.degree
		}
		degree := func(id storage.NodeID) int {
			switch id {
			case firstID:
				return left.degree
			case secondID:
				return right.degree
			}
			return degrees[id]
		}
		return linkpredict.PairScore(algorithm, firstID, secondID, linkpredict.NodeSet(left.nodes), linkpredict.NodeSet(right.nodes), degree), nil
	}
}

// gdsNodePair reads a link-prediction function's two node arguments; a null
// node gives null.
func gdsNodePair(name string, values []interface{}) (*storage.Node, *storage.Node, error) {
	nodes := make([]*storage.Node, 2)
	for index := range nodes {
		switch value := values[index].(type) {
		case nil:
		case *storage.Node:
			nodes[index] = value
		default:
			return nil, nil, fmt.Errorf("%s expects a node as argument %d, got %s", name, index+1, cypherTypeSystemName(value))
		}
	}
	return nodes[0], nodes[1], nil
}

func gdsLinkPredictionConfig(name string, values []interface{}) (gdsLinkConfig, error) {
	config := gdsLinkConfig{direction: "BOTH"}
	if len(values) < 3 || values[2] == nil {
		return config, nil
	}
	entries, ok := values[2].(map[string]interface{})
	if !ok {
		return config, fmt.Errorf("%s expects a config map as argument 3, got %s", name, cypherTypeSystemName(values[2]))
	}
	for key, value := range entries {
		text, isText := value.(string)
		switch key {
		case "relationshipQuery":
			if !isText {
				return config, fmt.Errorf("%s: relationshipQuery must be a relationship type", name)
			}
			config.relationshipType = text
		case "direction":
			if !isText {
				return config, fmt.Errorf("%s: direction must be BOTH, OUTGOING or INCOMING", name)
			}
			config.direction = strings.ToUpper(text)
		}
	}
	return config, nil
}

// gdsNeighbors returns a node's neighbourhood under config, or the error a
// read of its relationships gives.
func gdsNeighbors(store storage.Engine, id storage.NodeID, config gdsLinkConfig) (gdsNeighborhood, error) {
	neighbors := gdsNeighborhood{nodes: map[storage.NodeID]struct{}{}}
	if store == nil {
		return neighbors, nil
	}
	add := func(edges []*storage.Edge, outgoing bool) {
		for _, edge := range edges {
			if config.relationshipType != "" && edge.Type != config.relationshipType {
				continue
			}
			other := edge.EndNode
			if !outgoing {
				other = edge.StartNode
			}
			if other == id {
				// A self-loop is one relationship: counted once, from its
				// outgoing side when both directions are read.
				if outgoing || config.direction == "INCOMING" {
					neighbors.degree++
				}
				continue
			}
			neighbors.degree++
			neighbors.nodes[other] = struct{}{}
		}
	}
	if config.direction != "INCOMING" {
		edges, err := store.GetOutgoingEdges(id)
		if err != nil {
			return neighbors, err
		}
		add(edges, true)
	}
	if config.direction == "BOTH" || config.direction == "INCOMING" {
		edges, err := store.GetIncomingEdges(id)
		if err != nil {
			return neighbors, err
		}
		add(edges, false)
	}
	return neighbors, nil
}

func gdsCommon(a, b gdsNeighborhood) []storage.NodeID {
	common := make([]storage.NodeID, 0)
	if a.same {
		return common
	}
	for id := range a.nodes {
		if _, shared := b.nodes[id]; shared {
			common = append(common, id)
		}
	}
	return common
}

// fnGDSSameCommunity is gds.alpha.linkprediction.sameCommunity(node1, node2,
// communityProperty = 'community'): 1.0 when both nodes have the property
// with the same value, else 0.0.
func fnGDSSameCommunity(ctx cypherfn.Context, args []string) (interface{}, error) {
	const name = "gds.alpha.linkprediction.sameCommunity"
	if err := gdsArity(name, args, 2, 3); err != nil {
		return nil, err
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	first, second, err := gdsNodePair(name, values)
	if err != nil || first == nil || second == nil {
		return nil, err
	}
	property := "community"
	if len(values) == 3 {
		text, ok := values[2].(string)
		if !ok {
			return nil, fmt.Errorf("%s expects a property name as argument 3, got %s", name, cypherTypeSystemName(values[2]))
		}
		property = text
	}
	left, leftOK := first.Properties[property]
	right, rightOK := second.Properties[property]
	if equal, _ := cypherEquality(left, right).(bool); leftOK && rightOK && equal {
		return 1.0, nil
	}
	return 0.0, nil
}

// fnGDSAsNode is gds.util.asNode(nodeId): the node with that id, as the
// gds.* procedures return it, or null.
func fnGDSAsNode(ctx cypherfn.Context, args []string) (interface{}, error) {
	if err := gdsArity("gds.util.asNode", args, 1, 1); err != nil {
		return nil, err
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	store,err := ctx.GraphStorage()
 if err != nil { return nil,err }
 return gdsNodeByID(store, values[0], graphFunctionIdentityDomain(ctx,store)), nil
}

// fnGDSAsNodes is gds.util.asNodes(nodeIds): the nodes with those ids, in
// order, without the ids no node has.
func fnGDSAsNodes(ctx cypherfn.Context, args []string) (interface{}, error) {
	if err := gdsArity("gds.util.asNodes", args, 1, 1); err != nil {
		return nil, err
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil {
		return nil, err
	}
	ids, ok := cypherListValue(values[0])
	if !ok {
		return nil, fmt.Errorf("gds.util.asNodes expects a list of node ids, got %s", cypherTypeSystemName(values[0]))
	}
	store,err := ctx.GraphStorage()
 if err != nil { return nil,err }
 nodes := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		if node := gdsNodeByID(store, id, graphFunctionIdentityDomain(ctx,store)); node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func gdsNodeByID(store storage.Engine, value interface{}, domains ...graphIdentityDomain) interface{} {
 if store == nil || value == nil { return nil }
 id, ok := graphEntityLookupID(store,value,domains...)
 if !ok { return nil }
 // Storage's transaction owner decides visibility; preserve getQueryNode.
 var noExecutor *StorageExecutor
 if node, err := noExecutor.getQueryNode(store,id); err == nil && node != nil { return node }
 return nil
}

// fnGDSIsFinite is gds.util.isFinite / isInfinite: isFinite is true for a
// finite number, isInfinite for anything else (±Infinity, NaN, null), as in
// GDS.
func fnGDSIsFinite(finite bool) cypherfn.Func {
	name := "gds.util.isInfinite"
	if finite {
		name = "gds.util.isFinite"
	}
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if err := gdsArity(name, args, 1, 1); err != nil {
			return nil, err
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		isFinite := false
		if values[0] != nil {
			number, ok := toFloat64(values[0])
			if !ok {
				return nil, fmt.Errorf("expected a number, got %s", cypherTypeSystemName(values[0]))
			}
			isFinite = !math.IsInf(number, 0) && !math.IsNaN(number)
		}
		return isFinite == finite, nil
	}
}
