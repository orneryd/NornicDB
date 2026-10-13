package cypher

import (
	"fmt"
	"math"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GDS's functions (#1161): the topological link-prediction functions
// (gds.alpha.linkprediction.*), the gds.util helpers and gds.version(), with
// GDS 2.13's names, arguments and values. NornicDB's own
// gds.linkPrediction.*.stream procedures stay as extensions.
func init() {
	for name, score := range map[string]func(a, b gdsNeighborhood, degree func(storage.NodeID) int) float64{
		"adamicadar":             gdsAdamicAdar,
		"commonneighbors":        gdsCommonNeighbors,
		"preferentialattachment": gdsPreferentialAttachment,
		"resourceallocation":     gdsResourceAllocation,
		"totalneighbors":         gdsTotalNeighbors,
	} {
		cypherfn.Register("gds.alpha.linkprediction."+name, gdsLinkPrediction("gds.alpha.linkprediction."+name, score))
	}
	cypherfn.Register("gds.alpha.linkprediction.samecommunity", fnGDSSameCommunity)
	cypherfn.Register("gds.util.asnode", fnGDSAsNode)
	cypherfn.Register("gds.util.asnodes", fnGDSAsNodes)
	cypherfn.Register("gds.util.nan", func(cypherfn.Context, []string) (interface{}, error) { return math.NaN(), nil })
	cypherfn.Register("gds.util.infinity", func(cypherfn.Context, []string) (interface{}, error) { return math.Inf(1), nil })
	cypherfn.Register("gds.util.isfinite", fnGDSIsFinite(true))
	cypherfn.Register("gds.util.isinfinite", fnGDSIsFinite(false))
	cypherfn.Register("gds.version", func(cypherfn.Context, []string) (interface{}, error) { return gdsVersion, nil })
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

func gdsLinkPrediction(name string, score func(a, b gdsNeighborhood, degree func(storage.NodeID) int) float64) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) < 2 || len(args) > 3 {
			return nil, fmt.Errorf("%s takes two nodes and an optional config map", name)
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
		degree := func(id storage.NodeID) int { return gdsNeighbors(ctx.Storage, id, config).degree }
		left, right := gdsNeighbors(ctx.Storage, first.ID, config), gdsNeighbors(ctx.Storage, second.ID, config)
		// A node and itself share no neighbours, as GDS scores them.
		left.same = first.ID == second.ID
		value := score(left, right, degree)
		if value == 0 {
			value = 0 // not -0, from 1/log(0)
		}
		return value, nil
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

// gdsNeighbors returns a node's neighbourhood under config.
func gdsNeighbors(store storage.Engine, id storage.NodeID, config gdsLinkConfig) gdsNeighborhood {
	neighbors := gdsNeighborhood{nodes: map[storage.NodeID]struct{}{}}
	if store == nil {
		return neighbors
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
		edges, _ := store.GetOutgoingEdges(id)
		add(edges, true)
	}
	if config.direction == "BOTH" || config.direction == "INCOMING" {
		edges, _ := store.GetIncomingEdges(id)
		add(edges, false)
	}
	return neighbors
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

func gdsCommonNeighbors(a, b gdsNeighborhood, _ func(storage.NodeID) int) float64 {
	return float64(len(gdsCommon(a, b)))
}

func gdsTotalNeighbors(a, b gdsNeighborhood, _ func(storage.NodeID) int) float64 {
	union := len(a.nodes)
	for id := range b.nodes {
		if _, counted := a.nodes[id]; !counted {
			union++
		}
	}
	return float64(union)
}

func gdsPreferentialAttachment(a, b gdsNeighborhood, _ func(storage.NodeID) int) float64 {
	return float64(a.degree * b.degree)
}

func gdsAdamicAdar(a, b gdsNeighborhood, degree func(storage.NodeID) int) float64 {
	score := 0.0
	for _, id := range gdsCommon(a, b) {
		score += 1 / math.Log(float64(degree(id)))
	}
	return score
}

func gdsResourceAllocation(a, b gdsNeighborhood, degree func(storage.NodeID) int) float64 {
	score := 0.0
	for _, id := range gdsCommon(a, b) {
		score += 1 / float64(degree(id))
	}
	return score
}

// fnGDSSameCommunity is gds.alpha.linkprediction.sameCommunity(node1, node2,
// communityProperty = 'community'): 1.0 when both nodes have the property
// with the same value, else 0.0.
func fnGDSSameCommunity(ctx cypherfn.Context, args []string) (interface{}, error) {
	const name = "gds.alpha.linkprediction.sameCommunity"
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("%s takes two nodes and an optional community property", name)
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
	if len(args) != 1 {
		return nil, fmt.Errorf("gds.util.asNode takes one node id")
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	return gdsNodeByID(ctx.Storage, values[0]), nil
}

// fnGDSAsNodes is gds.util.asNodes(nodeIds): the nodes with those ids, in
// order, without the ids no node has.
func fnGDSAsNodes(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("gds.util.asNodes takes a list of node ids")
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil {
		return nil, err
	}
	ids, ok := cypherListValue(values[0])
	if !ok {
		return nil, fmt.Errorf("gds.util.asNodes expects a list of node ids, got %s", cypherTypeSystemName(values[0]))
	}
	nodes := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		if node := gdsNodeByID(ctx.Storage, id); node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func gdsNodeByID(store storage.Engine, value interface{}) interface{} {
	if store == nil || value == nil {
		return nil
	}
	id := fmt.Sprint(value)
	if node, err := store.GetNode(storage.NodeID(id)); err == nil && node != nil {
		return node
	}
	return nil
}

// fnGDSIsFinite is gds.util.isFinite / isInfinite: isFinite is true for a
// finite number, isInfinite for anything else (±Infinity, NaN, null), as in
// GDS.
func fnGDSIsFinite(finite bool) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("gds.util.isFinite and gds.util.isInfinite take one number")
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
