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
	if config.cutoff < 0 || config.cutoff > 1 {
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
	for _, pair := range [][2]string{{"topK", "bottomK"}, {"topN", "bottomN"}} {
		if c.has(pair[0]) && c.has(pair[1]) {
			return config, localizedError(localization.CypherGraphProceduresInvalidParameterCombination(pair[0], pair[1]), nil)
		}
	}
	config.bottomK, config.bottomN = c.has("bottomK"), c.has("bottomN")
	perNode, overall := "topK", "topN"
	if c.has("bottomK") {
		perNode = "bottomK"
	}
	if c.has("bottomN") {
		overall = "bottomN"
	}
	topK, err := c.integer(perNode, "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return config, err
	}
	topN, err := c.integer(overall, "Integer", 0, 1<<31-1, 0, false)
	if err != nil {
		return config, err
	}
	config.topK, config.topN = int(topK), int(topN)
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
// of nodes or ids; nil admits every node. A label the graph doesn't hold is
// refused, as in GDS; ids that match no node admit none.
func readGDSNodeFilter(proj *GraphProjection, value interface{}) (*gdsNodeFilter, error) {
	if value == nil {
		return nil, nil
	}
	if label, ok := value.(string); ok {
		if !containsString(proj.Labels, label) {
			return nil, localizedError(localization.CypherGraphProceduresLabelNotInGraph(label), nil)
		}
		return &gdsNodeFilter{label: label}, nil
	}
	items, isList := cypherListValue(value)
	if !isList {
		items = []interface{}{value}
	}
	filter := &gdsNodeFilter{nodes: make(map[string]bool, len(items))}
	for _, item := range items {
		switch node := item.(type) {
		case *storage.Node:
			if node != nil {
				filter.nodes[string(node.ID)] = true
			}
		default:
			if !isIntegerProcedureValue(item) {
				return nil, localizedError(localization.CypherGraphProceduresInvalidScalarType(gdsJavaType(item)), nil)
			}
			filter.nodes[strconv.FormatInt(toInt64(item), 10)] = true
		}
	}
	return filter, nil
}

// nodeSimilarityPair is one scored pair.
type nodeSimilarityPair struct {
	source, target int
	similarity     float64
	shared         []interface{}
}

func nodeSimilarityRows(proj *GraphProjection, graph *gdsSubgraph, config nodeSimilarityConfig) [][]interface{} {
	size := len(graph.nodes)
	// Each node's neighbours with their summed weights, and who points at
	// each node (to find the pairs sharing a neighbour).
	neighbors := make([]map[int]float64, size)
	incoming := make([][]int, size)
	for source, edges := range graph.outgoing {
		neighbors[source] = make(map[int]float64, len(edges))
		for _, edge := range edges {
			if _, seen := neighbors[source][edge.target]; !seen {
				incoming[edge.target] = append(incoming[edge.target], source)
			}
			neighbors[source][edge.target] += edge.weight
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
			if similarity < config.cutoff || (similarity == 0 && !config.includeZero) {
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
// relationships read both ways); nil without useComponents. A node without
// the property is a component of its own.
func nodeSimilarityComponents(proj *GraphProjection, graph *gdsSubgraph, config nodeSimilarityConfig) []string {
	size := len(graph.nodes)
	component := make([]string, size)
	switch {
	case config.componentProperty != "":
		for i, id := range graph.nodes {
			if value, ok := proj.NodeProperties[id][config.componentProperty]; ok {
				component[i] = strconv.FormatFloat(value, 'g', -1, 64)
			} else {
				component[i] = "node:" + id
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
		if denominator := math.Min(sumA, sumB); denominator > 0 {
			similarity = intersection / denominator
		}
	case "COSINE":
		if normA > 0 && normB > 0 {
			similarity = dot / (math.Sqrt(normA) * math.Sqrt(normB))
		}
	default:
		if union > 0 {
			similarity = intersection / union
		}
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
