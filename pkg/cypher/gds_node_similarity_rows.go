package cypher

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// nornicdb.similarity.stream scores node similarity for one source over
// incidence rows the statement supplies, {source, thing, weight?}, instead of
// a catalog projection (B9). The rows come from the statement's own reads, so
// the scores follow the graph as the statement sees it (its snapshot, its own
// writes, and a restricted login's view) with nothing to project, maintain or
// drop. Nothing is read from storage: node1, node2 and shared.nodeId are the
// ids the rows carry (a remote constituent's node by its elementId), and a
// tieBreakProperty is read from the node values the rows carry. A NornicDB extension; GDS has no row-input similarity.

// similarityRowsConfigKeys are the configuration keys nornicdb.similarity.stream
// takes; the shared ones keep GDS's names.
var similarityRowsConfigKeys = []string{"sourceNode", "similarityMetric", "similarityCutoff", "topK", "maxHolders", "tieBreakProperty"}

// similarityRowsConfig is nornicdb.similarity.stream's configuration.
type similarityRowsConfig struct {
	sources     []gdsNodeRef // the source: one node, or the nodes of a list, scored as one
	group       bool         // sourceNode was a list: node1 is its ids
	metric      string       // JACCARD, OVERLAP, COSINE or RESOURCE_ALLOCATION
	cutoff      float64
	includeZero bool // an explicit similarityCutoff of 0: every other source in the rows
	topK        int
	maxHolders  int    // things held by more sources are ignored; 0: no limit
	tieBreak    string // property ordering equal scores and equal shared weights before the id; "": the id
}

// callNornicSimilarityStream implements
// nornicdb.similarity.stream(rows, configuration).
func callNornicSimilarityStream(args []interface{}) (*ExecuteResult, error) {
	const procedure = "nornicdb.similarity.stream"
	value := procedureArgument(args, 0)
	if value == nil {
		return nil, procedureArgumentNullError(procedure, "rows")
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, procedureArgumentTypeError(procedure, "rows", "LIST<MAP>", value)
	}
	options, err := optionalProcedureMap(procedure, args, 1, "configuration")
	if err != nil {
		return nil, err
	}
	config, err := readSimilarityRowsConfig(options)
	if err != nil {
		return nil, err
	}
	incidence, err := readSimilarityIncidence(items)
	if err != nil {
		return nil, err
	}
	result := &ExecuteResult{Columns: []string{"node1", "node2", "similarity", "shared"}, Rows: similarityRowsScores(incidence, config)}
	renderGDSRowKeys(result, gdsNodeProcedures["nornicdb.similarity.stream"].columns)
	return result, nil
}

func readSimilarityRowsConfig(options map[string]interface{}) (similarityRowsConfig, error) {
	config := similarityRowsConfig{metric: "JACCARD", cutoff: 1e-42}
	c, err := newGDSConfig(options, similarityRowsConfigKeys...)
	if err != nil {
		return config, err
	}
	// sourceNode: a node or id, or a list of them (a start with several
	// nodes, e.g. a seed's documents): their things are one source's, and
	// none of them is scored as a candidate.
	value := c.value("sourceNode")
	if items, isList := cypherListValue(value); isList && value != nil {
		config.group = true
		for _, item := range items {
			source, ok := readGDSNode(item, gdsRowKinds)
			if !ok {
				return config, c.typeError("sourceNode", "Node")
			}
			config.sources = append(config.sources, source)
		}
	} else if value != nil {
		source, ok := readGDSNode(value, gdsRowKinds)
		if !ok {
			return config, c.typeError("sourceNode", "Node")
		}
		config.sources = []gdsNodeRef{source}
	}
	if len(config.sources) == 0 {
		return config, localizedError(localization.CypherGraphProceduresSourceNodeRequired(), nil)
	}
	if metric, ok, err := c.text("similarityMetric"); err != nil {
		return config, err
	} else if ok {
		switch upper := strings.ToUpper(metric); upper {
		case "JACCARD", "OVERLAP", "COSINE", "RESOURCE_ALLOCATION":
			config.metric = upper
		default:
			return config, localizedError(localization.CypherGraphProceduresInvalidRowSimilarityMetric(metric), nil)
		}
	}
	if config.cutoff, err = c.number("similarityCutoff", "Double", config.cutoff); err != nil {
		return config, err
	}
	// Resource allocation sums weights, so its scores have no upper bound.
	upper, upperText := 1.0, "1.00"
	if config.metric == "RESOURCE_ALLOCATION" {
		upper, upperText = math.Inf(1), "Infinity"
	}
	if config.cutoff < 0 || config.cutoff > upper || math.IsNaN(config.cutoff) {
		return config, localizedError(localization.CypherGraphProceduresConfigValueOutOfRange("similarityCutoff", javaDoubleText(config.cutoff), "[0.00, "+upperText+"]"), nil)
	}
	config.includeZero = c.has("similarityCutoff") && config.cutoff == 0
	topK, err := c.integer("topK", "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return config, err
	}
	config.topK = int(topK)
	maxHolders, err := c.integer("maxHolders", "Integer", 0, 1<<31-1, 0, false)
	if err != nil {
		return config, err
	}
	config.maxHolders = int(maxHolders)
	if config.tieBreak, _, err = c.text("tieBreakProperty"); err != nil {
		return config, err
	}
	return config, nil
}

// similarityIncidence is the rows' bipartite graph: each source's things.
type similarityIncidence struct {
	sources []string                                // sorted ids
	things  map[string]map[string]*similarityWeight // source → thing → weight given
	nodes   map[string]interface{}                  // id → the first node value a row gave for it
}

// similarityWeight is what repeated rows gave one (source, thing): the
// largest explicit weight, and whether a row left the weight to its default.
type similarityWeight struct {
	explicit    float64
	hasExplicit bool
	hasDefault  bool
}

// resolve is the pair's weight given the thing's default weight: the
// largest weight its rows gave.
func (w *similarityWeight) resolve(fallback float64) float64 {
	switch {
	case !w.hasExplicit:
		return fallback
	case w.hasDefault:
		return math.Max(w.explicit, fallback)
	}
	return w.explicit
}

// readSimilarityIncidence reads the rows: maps with source (a node or id,
// required), thing (a node or id; null for a source with no thing) and
// weight (a number of at least 0; no weight key for the metric's default).
// A weight that is null, NaN or infinite (1.0 / 0 for a thing nobody holds)
// drops its row's thing; its source stays a source. Repeated (source, thing)
// rows are one, with the largest weight given.
func readSimilarityIncidence(items []interface{}) (*similarityIncidence, error) {
	incidence := &similarityIncidence{things: make(map[string]map[string]*similarityWeight), nodes: make(map[string]interface{})}
	remember := func(ref gdsNodeRef, value interface{}) {
		if ref.isNode() {
			if _, seen := incidence.nodes[ref.key()]; !seen {
				incidence.nodes[ref.key()] = value
			}
		}
	}
	for index, item := range items {
		position := strconv.Itoa(index)
		row, isMap := item.(map[string]interface{})
		if !isMap || cypherValueKindOf(item) != valueKindMap {
			return nil, localizedError(localization.CypherGraphProceduresSimilarityRowNotMap(position, gdsJavaType(item)), nil)
		}
		for key := range row {
			if key != "source" && key != "thing" && key != "weight" {
				return nil, localizedError(localization.CypherGraphProceduresSimilarityRowKey(position, key), nil)
			}
		}
		sourceRef, ok := readGDSNode(row["source"], gdsRowKinds)
		if !ok {
			return nil, localizedError(localization.CypherGraphProceduresSimilarityRowNode(position, "source", gdsJavaType(row["source"])), nil)
		}
		remember(sourceRef, row["source"])
		source := sourceRef.key()
		things, seen := incidence.things[source]
		if !seen {
			things = make(map[string]*similarityWeight)
			incidence.things[source] = things
			incidence.sources = append(incidence.sources, source)
		}
		value, hasWeight := row["weight"]
		if row["thing"] == nil {
			// A source with no thing, as OPTIONAL MATCH gives it; a
			// weight then weighs nothing.
			if value != nil {
				return nil, localizedError(localization.CypherGraphProceduresSimilarityRowNode(position, "thing", gdsJavaType(nil)), nil)
			}
			continue
		}
		thingRef, ok := readGDSNode(row["thing"], gdsRowKinds)
		if !ok {
			return nil, localizedError(localization.CypherGraphProceduresSimilarityRowNode(position, "thing", gdsJavaType(row["thing"])), nil)
		}
		remember(thingRef, row["thing"])
		thing := thingRef.key()
		number, isNumber := gdsScalarNumber(value)
		switch {
		case !hasWeight:
		case value == nil || isNumber && (math.IsNaN(number) || math.IsInf(number, 0)):
			continue
		case !isNumber || number < 0:
			text := gdsJavaType(value)
			if isNumber {
				text = javaDoubleText(number)
			}
			return nil, localizedError(localization.CypherGraphProceduresSimilarityRowWeight(position, text), nil)
		}
		weight := things[thing]
		if weight == nil {
			weight = &similarityWeight{}
			things[thing] = weight
		}
		if !hasWeight {
			weight.hasDefault = true
		} else if !weight.hasExplicit || number > weight.explicit {
			weight.explicit, weight.hasExplicit = number, true
		}
	}
	sort.Strings(incidence.sources)
	return incidence, nil
}

// tieKey is id's tieBreakProperty value, from the node a row gave for it;
// null for an id given only as an id or without the property.
func (incidence *similarityIncidence) tieKey(id, property string) interface{} {
	value, ok := incidence.nodes[id]
	if !ok {
		return nil
	}
	key, _ := rowPropertyValue(value, property)
	return key
}

// similarityVectors builds each source's neighbour vector over the things
// left after the maxHolders cutoff, sorted by thing, and each thing's
// holders (the distinct sources whose rows give it). A thing's default weight
// is 1/holders for RESOURCE_ALLOCATION and 1 for the GDS metrics.
func similarityVectors(incidence *similarityIncidence, config similarityRowsConfig) ([][]gdsEdge, [][]int, []string) {
	holders := make(map[string]int)
	for _, things := range incidence.things {
		for thing := range things {
			holders[thing]++
		}
	}
	kept := make([]string, 0, len(holders))
	for thing, count := range holders {
		if config.maxHolders == 0 || count <= config.maxHolders {
			kept = append(kept, thing)
		}
	}
	sort.Strings(kept)
	thingIndex := make(map[string]int, len(kept))
	for i, thing := range kept {
		thingIndex[thing] = i
	}
	neighbors := make([][]gdsEdge, len(incidence.sources))
	incoming := make([][]int, len(kept))
	for s, source := range incidence.sources {
		for thing, given := range incidence.things[source] {
			t, ok := thingIndex[thing]
			if !ok {
				continue
			}
			fallback := 1.0
			if config.metric == "RESOURCE_ALLOCATION" {
				fallback = 1 / float64(holders[thing])
			}
			neighbors[s] = append(neighbors[s], gdsEdge{target: t, weight: given.resolve(fallback)})
			incoming[t] = append(incoming[t], s)
		}
		sort.Slice(neighbors[s], func(i, j int) bool { return neighbors[s][i].target < neighbors[s][j].target })
	}
	return neighbors, incoming, kept
}

// similaritySourceVector is the source's neighbour vector: the union of its
// nodes' vectors, a thing two of them have once with the larger weight.
func similaritySourceVector(neighbors [][]gdsEdge, members []int) []gdsEdge {
	if len(members) == 1 {
		return neighbors[members[0]]
	}
	weights := make(map[int]float64)
	for _, member := range members {
		for _, edge := range neighbors[member] {
			if weight, seen := weights[edge.target]; !seen || edge.weight > weight {
				weights[edge.target] = edge.weight
			}
		}
	}
	vector := make([]gdsEdge, 0, len(weights))
	for target, weight := range weights {
		vector = append(vector, gdsEdge{target: target, weight: weight})
	}
	sort.Slice(vector, func(i, j int) bool { return vector[i].target < vector[j].target })
	return vector
}

// similarityRowsScores is the source's topK most similar other sources, by
// the node-similarity kernel gds.nodeSimilarity uses: most similar first,
// ties by the tieBreakProperty, then by id; zero rows when no source node is
// in the rows. Only the returned rows' shared lists are built.
func similarityRowsScores(incidence *similarityIncidence, config similarityRowsConfig) [][]interface{} {
	excluded := make(map[int]bool, len(config.sources))
	var members []int
	has := func(key string) bool {
		index := sort.SearchStrings(incidence.sources, key)
		return index < len(incidence.sources) && incidence.sources[index] == key
	}
	for _, ref := range config.sources {
		key, ok := ref.find(has)
		if !ok {
			continue
		}
		if index := sort.SearchStrings(incidence.sources, key); !excluded[index] {
			excluded[index] = true
			members = append(members, index)
		}
	}
	if len(members) == 0 {
		return [][]interface{}{}
	}
	neighbors, incoming, things := similarityVectors(incidence, config)
	source := similaritySourceVector(neighbors, members)
	candidates := make(map[int]bool)
	if config.includeZero {
		for other := range incidence.sources {
			candidates[other] = true
		}
	} else {
		for _, neighbor := range source {
			for _, other := range incoming[neighbor.target] {
				candidates[other] = true
			}
		}
	}
	var scored []nodeSimilarityPair
	for target := range candidates {
		if excluded[target] {
			continue
		}
		similarity, _ := nodeSimilarity(source, neighbors[target], config.metric, true, things, false)
		// An undefined score (0/0) is dropped, as gds.nodeSimilarity drops it.
		if math.IsNaN(similarity) || similarity < config.cutoff || (similarity == 0 && !config.includeZero) {
			continue
		}
		scored = append(scored, nodeSimilarityPair{target: target, similarity: similarity})
	}
	tieBefore := func(a, b string) bool {
		if config.tieBreak != "" {
			if order := compareValuesForSort(incidence.tieKey(a, config.tieBreak), incidence.tieKey(b, config.tieBreak)); order != 0 {
				return order < 0
			}
		}
		return gdsShownBefore(a, b)
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].similarity != scored[j].similarity {
			return scored[i].similarity > scored[j].similarity
		}
		return tieBefore(incidence.sources[scored[i].target], incidence.sources[scored[j].target])
	})
	if len(scored) > config.topK {
		scored = scored[:config.topK]
	}
	// Keys here; renderGDSRowKeys shows them.
	var node1 interface{} = config.sources[0].key()
	if config.group {
		ids := make([]string, len(config.sources))
		for i, ref := range config.sources {
			ids[i] = ref.key()
		}
		sort.Slice(ids, func(i, j int) bool { return gdsShownBefore(ids[i], ids[j]) })
		list := make([]interface{}, len(ids))
		for i, id := range ids {
			list[i] = id
		}
		node1 = list
	}
	rows := make([][]interface{}, len(scored))
	for i, pair := range scored {
		_, shared := nodeSimilarity(source, neighbors[pair.target], config.metric, true, things, true)
		// Most weight first, as the kernel orders them; equal weights
		// by the tieBreakProperty, then by id.
		sort.Slice(shared, func(i, j int) bool {
			left, right := shared[i].(map[string]interface{}), shared[j].(map[string]interface{})
			if lw, rw := left["weight"].(float64), right["weight"].(float64); lw != rw {
				return lw > rw
			}
			return tieBefore(left["nodeId"].(string), right["nodeId"].(string))
		})
		rows[i] = []interface{}{node1, incidence.sources[pair.target], pair.similarity, shared}
	}
	return rows
}
