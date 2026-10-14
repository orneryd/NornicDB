package cypher

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeSimilarityConfigKeys are the configuration keys gds.nodeSimilarity.stream
// takes; the filtered form adds sourceNodeFilter and targetNodeFilter.
var nodeSimilarityConfigKeys = []string{
	"similarityCutoff", "degreeCutoff", "upperDegreeCutoff", "topK", "bottomK", "topN", "bottomN",
	"relationshipWeightProperty", "similarityMetric", "nodeLabels", "relationshipTypes",
	"concurrency", "jobId", "logProgress", "sudo", "useComponents",
}

// nodeSimilarityConfig is gds.nodeSimilarity's configuration (#1195).
type nodeSimilarityConfig struct {
	metric            string // JACCARD, OVERLAP or COSINE
	cutoff            float64
	degreeCutoff      int
	upperDegreeCutoff int
	topK, topN        int  // per node, and overall (0: none)
	bottomK, bottomN  bool // the least similar per node, or overall
	weightProperty    string
	nodeLabels        []string
	relationshipTypes []string
	includeZero       bool   // an explicit similarityCutoff of 0: every eligible pair
	componentProperty string // useComponents' property: pairs within one component
	connectedOnly     bool   // useComponents: true: pairs within one connected component
	sourceFilter      *gdsNodeFilter
	targetFilter      *gdsNodeFilter
}

// gdsNodeFilter is a filtered algorithm's node filter: a label, or nodes.
type gdsNodeFilter struct {
	label string
	nodes map[string]bool
}

func (f *gdsNodeFilter) admits(proj *GraphProjection, id string) bool {
	if f == nil {
		return true
	}
	if f.label != "" {
		return containsString(proj.NodeLabels[id], f.label)
	}
	return f.nodes[id]
}

// callGdsNodeSimilarityStream implements gds.nodeSimilarity.stream and, with
// filtered, gds.nodeSimilarity.filtered.stream, as GDS 2.13 does (#1195):
// every pair of nodes that share a neighbour in the projection, by JACCARD
// (weighted: Σmin / Σmax), OVERLAP or COSINE similarity, the topK (or
// bottomK) per node and the topN (or bottomN) overall. Only pairs that
// share a neighbour are scored, so a few source nodes cost their
// neighbourhoods, not the whole graph. Rows are in node order, then most
// similar first; the shared column, a NornicDB extension, lists the
// neighbours each pair shares with their weight in the score.
func (e *StorageExecutor) callGdsNodeSimilarityStream(args []interface{}, filtered bool) (*ExecuteResult, error) {
	procedure := "gds.nodeSimilarity.stream"
	if filtered {
		procedure = "gds.nodeSimilarity.filtered.stream"
	}
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	options, err := optionalProcedureMap(procedure, args, 1, "configuration")
	if err != nil {
		return nil, err
	}
	projection, err := e.graphProjection(graphName)
	if err != nil {
		return nil, err
	}
	config, err := readNodeSimilarityConfig(projection, options, filtered)
	if err != nil {
		return nil, err
	}
	graph, err := selectGDSSubgraph(projection, config.nodeLabels, config.relationshipTypes, config.weightProperty)
	if err != nil {
		return nil, err
	}
	rows := nodeSimilarityRows(projection, graph, config)
	return &ExecuteResult{Columns: []string{"node1", "node2", "similarity", "shared"}, Rows: rows}, nil
}

func readNodeSimilarityConfig(proj *GraphProjection, options map[string]interface{}, filtered bool) (nodeSimilarityConfig, error) {
	config := nodeSimilarityConfig{metric: "JACCARD", cutoff: 1e-42}
	keys := nodeSimilarityConfigKeys
	if filtered {
		keys = append(append([]string(nil), keys...), "sourceNodeFilter", "targetNodeFilter")
	}
	c, err := newGDSConfig(options, keys...)
	if err != nil {
		return config, err
	}
	if config.cutoff, err = c.number("similarityCutoff", "Double", config.cutoff); err != nil {
		return config, err
	}
	if config.cutoff < 0 || config.cutoff > 1 || math.IsNaN(config.cutoff) {
		return config, localizedError(localization.CypherGraphProceduresConfigValueOutOfRange("similarityCutoff", javaDoubleText(config.cutoff), "[0.00, 1.00]"), nil)
	}
	// An explicit cutoff of 0 keeps the pairs that share nothing, as GDS
	// does (#1197 review); the default keeps only pairs sharing a neighbour.
	config.includeZero = c.has("similarityCutoff") && config.cutoff == 0
	degreeCutoff, err := c.integer("degreeCutoff", "Integer", 1, 1<<31-1, 1, false)
	if err != nil {
		return config, err
	}
	upperDegreeCutoff, err := c.integer("upperDegreeCutoff", "Integer", 1, 1<<31-1, 1<<31-1, false)
	if err != nil {
		return config, err
	}
	if upperDegreeCutoff < degreeCutoff {
		return config, localizedError(localization.CypherGraphProceduresUpperDegreeBelowDegree(), nil)
	}
	config.degreeCutoff, config.upperDegreeCutoff = int(degreeCutoff), int(upperDegreeCutoff)
	// topK and bottomK (topN and bottomN) clash only when both differ from
	// their defaults, as in GDS; the bottom one applies when it does.
	limits := make(map[string]int64)
	for _, limit := range []struct {
		key           string
		min, fallback int64
	}{{"topK", 1, 10}, {"bottomK", 1, 10}, {"topN", 0, 0}, {"bottomN", 0, 0}} {
		value, err := c.integer(limit.key, "Integer", limit.min, 1<<31-1, limit.fallback, false)
		if err != nil {
			return config, err
		}
		limits[limit.key] = value
	}
	if limits["topK"] != 10 && limits["bottomK"] != 10 {
		return config, localizedError(localization.CypherGraphProceduresInvalidParameterCombination("topK", "bottomK"), nil)
	}
	if limits["topN"] != 0 && limits["bottomN"] != 0 {
		return config, localizedError(localization.CypherGraphProceduresInvalidParameterCombination("topN", "bottomN"), nil)
	}
	config.bottomK, config.bottomN = limits["bottomK"] != 10, limits["bottomN"] != 0
	config.topK, config.topN = int(limits["topK"]), int(limits["topN"])
	if config.bottomK {
		config.topK = int(limits["bottomK"])
	}
	if config.bottomN {
		config.topN = int(limits["bottomN"])
	}
	if metric, ok, err := c.text("similarityMetric"); err != nil {
		return config, err
	} else if ok {
		config.metric = strings.ToUpper(metric)
		switch config.metric {
		case "JACCARD", "OVERLAP", "COSINE":
		default:
			return config, localizedError(localization.CypherGraphProceduresInvalidSimilarityMetric(metric), nil)
		}
	}
	if property, ok, err := c.text("relationshipWeightProperty"); err != nil {
		return config, err
	} else if ok {
		config.weightProperty = property
	}
	if config.nodeLabels, err = c.names("nodeLabels"); err != nil {
		return config, err
	}
	if config.relationshipTypes, err = c.names("relationshipTypes"); err != nil {
		return config, err
	}
	if _, err := c.integer("concurrency", "Integer", 1, 1<<31-1, 4, false); err != nil {
		return config, err
	}
	if _, err := c.jobID(); err != nil {
		return config, err
	}
	for _, key := range []string{"logProgress", "sudo"} {
		if _, _, err := c.boolean(key); err != nil {
			return config, err
		}
	}
	// useComponents compares nodes within one component only: by a
	// projected property's values, or (true) by connected components, as
	// GDS does (#1197 review).
	switch value := c.value("useComponents").(type) {
	case nil:
	case bool:
		config.connectedOnly = value
	case string:
		var available []string
		for _, keys := range proj.NodePropertyKeys {
			for _, key := range keys {
				available = appendUnique(available, key)
			}
		}
		sort.Strings(available)
		if !containsString(available, value) {
			return config, localizedError(localization.CypherGraphProceduresComponentPropertyNotFound(value, "["+strings.Join(available, ", ")+"]"), nil)
		}
		config.componentProperty = value
	default:
		return config, c.typeError("useComponents", "Boolean")
	}
	if filtered {
		if config.sourceFilter, err = readGDSNodeFilter(proj, c.value("sourceNodeFilter")); err != nil {
			return config, err
		}
		if config.targetFilter, err = readGDSNodeFilter(proj, c.value("targetNodeFilter")); err != nil {
			return config, err
		}
	}
	return config, nil
}

// readGDSNodeFilter reads a node filter: a label, a node, an id, or a list
// of nodes or ids; nil or an empty list admits every node, as in GDS. A
// string is a label, or, as NornicDB's ids are strings (id(n)), the id of a
// projected node; a label the graph doesn't hold is refused, as in GDS.
// Ids that match no node admit none.
func readGDSNodeFilter(proj *GraphProjection, value interface{}) (*gdsNodeFilter, error) {
	if value == nil {
		return nil, nil
	}
	if label, ok := value.(string); ok {
		if !containsString(proj.Labels, label) {
			if _, projected := proj.nodeIndex[label]; projected {
				return &gdsNodeFilter{nodes: map[string]bool{label: true}}, nil
			}
			return nil, localizedError(localization.CypherGraphProceduresLabelNotInGraph(label), nil)
		}
		return &gdsNodeFilter{label: label}, nil
	}
	items, isList := cypherListValue(value)
	if isList && len(items) == 0 {
		return nil, nil
	}
	filter := &gdsNodeFilter{nodes: make(map[string]bool, len(items))}
	var invalid []string
	for _, item := range items {
		if id, ok := gdsNodeFilterID(proj, item); ok {
			filter.nodes[id] = true
		} else {
			invalid = appendUnique(invalid, gdsJavaType(item))
		}
	}
	if !isList {
		if id, ok := gdsNodeFilterID(proj, value); ok {
			filter.nodes[id] = true
		} else {
			return nil, localizedError(localization.CypherGraphProceduresInvalidScalarType(gdsJavaType(value)), nil)
		}
	}
	if len(invalid) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresInvalidListTypes("["+strings.Join(invalid, ", ")+"]"), nil)
	}
	return filter, nil
}

// gdsNodeFilterID is a node filter entry's node id: a node's, an integer
// id, or a projected node's string id.
func gdsNodeFilterID(proj *GraphProjection, item interface{}) (string, bool) {
	switch node := item.(type) {
	case *storage.Node:
		if node == nil {
			return "", false
		}
		return string(node.ID), true
	case string:
		_, projected := proj.nodeIndex[node]
		return node, projected
	}
	if !isIntegerProcedureValue(item) {
		return "", false
	}
	return strconv.FormatInt(toInt64(item), 10), true
}

// nodeSimilarityPair is one scored pair.
type nodeSimilarityPair struct {
	source, target int
	similarity     float64
	shared         []interface{}
}

func nodeSimilarityRows(proj *GraphProjection, graph *gdsSubgraph, config nodeSimilarityConfig) [][]interface{} {
	size := len(graph.nodes)
	// Each node's neighbours, with the first relationship's weight to each
	// and no self-loops, as GDS reads them, and who points at each node (to
	// find the pairs sharing a neighbour).
	neighbors := make([]map[int]float64, size)
	incoming := make([][]int, size)
	for source, edges := range graph.outgoing {
		neighbors[source] = make(map[int]float64, len(edges))
		for _, edge := range edges {
			if _, seen := neighbors[source][edge.target]; seen || edge.target == source {
				continue
			}
			incoming[edge.target] = append(incoming[edge.target], source)
			neighbors[source][edge.target] = edge.weight
		}
	}
	eligible := func(node int) bool {
		degree := len(neighbors[node])
		return degree >= config.degreeCutoff && degree <= config.upperDegreeCutoff
	}
	weighted := config.weightProperty != ""
	component := nodeSimilarityComponents(proj, graph, config)

	var pairs []nodeSimilarityPair
	for source := 0; source < size; source++ {
		if !eligible(source) || !config.sourceFilter.admits(proj, graph.nodes[source]) {
			continue
		}
		candidates := make(map[int]bool)
		if config.includeZero {
			for other := 0; other < size; other++ {
				if other != source {
					candidates[other] = true
				}
			}
		} else {
			for neighbor := range neighbors[source] {
				for _, other := range incoming[neighbor] {
					if other != source {
						candidates[other] = true
					}
				}
			}
		}
		var scored []nodeSimilarityPair
		for target := range candidates {
			if !eligible(target) || !config.targetFilter.admits(proj, graph.nodes[target]) {
				continue
			}
			if component != nil && component[source] != component[target] {
				continue
			}
			similarity, shared := nodeSimilarity(neighbors[source], neighbors[target], config.metric, weighted, graph.nodes)
			// An undefined score (0/0) is dropped, as GDS drops it.
			if math.IsNaN(similarity) || math.IsInf(similarity, 0) || similarity < config.cutoff || (similarity == 0 && !config.includeZero) {
				continue
			}
			scored = append(scored, nodeSimilarityPair{source: source, target: target, similarity: similarity, shared: shared})
		}
		sort.Slice(scored, func(i, j int) bool { return nodeSimilarityBefore(scored[i], scored[j], config.bottomK) })
		if len(scored) > config.topK {
			scored = scored[:config.topK]
		}
		pairs = append(pairs, scored...)
	}
	if config.topN > 0 {
		sort.SliceStable(pairs, func(i, j int) bool {
			if pairs[i].similarity != pairs[j].similarity {
				return (pairs[i].similarity > pairs[j].similarity) != config.bottomN
			}
			if pairs[i].source != pairs[j].source {
				return pairs[i].source < pairs[j].source
			}
			return pairs[i].target < pairs[j].target
		})
		if len(pairs) > config.topN {
			pairs = pairs[:config.topN]
		}
	}
	rows := make([][]interface{}, len(pairs))
	for i, pair := range pairs {
		rows[i] = []interface{}{graph.nodes[pair.source], graph.nodes[pair.target], pair.similarity, pair.shared}
	}
	return rows
}

// nodeSimilarityComponents is each node's component for useComponents: the
// projected property's value, or its connected component (the
// relationships read both ways); nil without useComponents. Nodes without
// the property share one component, the property's default, as in GDS.
func nodeSimilarityComponents(proj *GraphProjection, graph *gdsSubgraph, config nodeSimilarityConfig) []string {
	size := len(graph.nodes)
	component := make([]string, size)
	switch {
	case config.componentProperty != "":
		for i, id := range graph.nodes {
			if value, ok := proj.NodeProperties[id][config.componentProperty]; ok {
				component[i] = strconv.FormatFloat(value, 'g', -1, 64)
			} else {
				component[i] = "default"
			}
		}
	case config.connectedOnly:
		parent := make([]int, size)
		for i := range parent {
			parent[i] = i
		}
		find := func(i int) int {
			for parent[i] != i {
				parent[i] = parent[parent[i]]
				i = parent[i]
			}
			return i
		}
		for source, edges := range graph.outgoing {
			for _, edge := range edges {
				if a, b := find(source), find(edge.target); a != b {
					parent[a] = b
				}
			}
		}
		for i := range component {
			component[i] = strconv.Itoa(find(i))
		}
	default:
		return nil
	}
	return component
}

// nodeSimilarityBefore orders a node's pairs: most similar first (least for
// bottomK), then by the other node.
func nodeSimilarityBefore(a, b nodeSimilarityPair, bottom bool) bool {
	if a.similarity != b.similarity {
		return (a.similarity > b.similarity) != bottom
	}
	return a.target < b.target
}

// nodeSimilarity is the similarity of two neighbour weight maps by metric,
// and the neighbours they share (nodeId, and weight: the smaller of the two
// weights, 1 unweighted), most weight first.
func nodeSimilarity(a, b map[int]float64, metric string, weighted bool, nodes []string) (float64, []interface{}) {
	weightOf := func(weights map[int]float64, node int) float64 {
		if weighted {
			return weights[node]
		}
		return 1
	}
	var intersection, union, dot, normA, normB, sumA, sumB float64
	type sharedNeighbor struct {
		node   int
		weight float64
	}
	var shared []sharedNeighbor
	for node := range a {
		wa := weightOf(a, node)
		normA += wa * wa
		sumA += wa
		if _, ok := b[node]; ok {
			wb := weightOf(b, node)
			intersection += math.Min(wa, wb)
			union += math.Max(wa, wb)
			dot += wa * wb
			shared = append(shared, sharedNeighbor{node, math.Min(wa, wb)})
		} else {
			union += wa
		}
	}
	for node := range b {
		wb := weightOf(b, node)
		normB += wb * wb
		sumB += wb
		if _, ok := a[node]; !ok {
			union += wb
		}
	}
	var similarity float64
	switch metric {
	case "OVERLAP":
		similarity = intersection / math.Min(sumA, sumB)
	case "COSINE":
		similarity = dot / (math.Sqrt(normA) * math.Sqrt(normB))
	default:
		similarity = intersection / union
	}
	sort.Slice(shared, func(i, j int) bool {
		if shared[i].weight != shared[j].weight {
			return shared[i].weight > shared[j].weight
		}
		return nodes[shared[i].node] < nodes[shared[j].node]
	})
	out := make([]interface{}, len(shared))
	for i, neighbor := range shared {
		out[i] = map[string]interface{}{"nodeId": nodes[neighbor.node], "weight": neighbor.weight}
	}
	return similarity, out
}

// javaDoubleText is a number as GDS's messages print a Double: 2.0, 0.5.
func javaDoubleText(value float64) string {
	if value == math.Trunc(value) && math.Abs(value) < 1e15 {
		return fmt.Sprintf("%.1f", value)
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}
