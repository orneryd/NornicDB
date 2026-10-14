package cypher

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// louvainConfigKeys are the configuration keys gds.louvain.stream takes.
var louvainConfigKeys = []string{
	"relationshipWeightProperty", "maxLevels", "maxIterations", "tolerance", "includeIntermediateCommunities",
	"consecutiveIds", "seedProperty", "minCommunitySize", "nodeLabels", "relationshipTypes",
	"concurrency", "jobId", "logProgress", "sudo",
}

// callGdsLouvainStream implements gds.louvain.stream(graphName, config) as
// GDS 2.13 does (#1198): Louvain over the projection's nodes and
// relationships as projected, their weights from relationshipWeightProperty,
// with GDS's maxLevels (10),
// maxIterations (10) and tolerance (0.0001), seedProperty for the initial
// communities, minCommunitySize, and includeIntermediateCommunities for
// each level's community. Rows are in the projection's node order. As in
// GDS, a community is named by the node it grew from: its position in the
// projection, or with seedProperty, that node's seed (non-negative
// integers only; Long.MIN_VALUE means missing), or for an unseeded node,
// the largest seed + its numeric original ID (string IDs use 1 + their
// projection position). This can collide with a seed, as in GDS.
// consecutiveIds numbers them 0, 1, … by first node.
func (e *StorageExecutor) callGdsLouvainStream(args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.louvain.stream"
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
	c, err := newGDSConfig(options, louvainConfigKeys...)
	if err != nil {
		return nil, err
	}
	run := louvainOptions{}
	maxLevels, err := c.integer("maxLevels", "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return nil, err
	}
	maxIterations, err := c.integer("maxIterations", "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return nil, err
	}
	run.maxLevels, run.maxIterations = int(maxLevels), int(maxIterations)
	if run.tolerance, err = c.number("tolerance", "Double", 0.0001); err != nil {
		return nil, err
	}
	if run.tolerance < 0 || math.IsNaN(run.tolerance) || math.IsInf(run.tolerance, 1) {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueOutOfRange("tolerance", javaDoubleText(run.tolerance), "[0.00, "+javaPlainDouble(math.MaxFloat64)+".00]"), nil)
	}
	intermediate, _, err := c.boolean("includeIntermediateCommunities")
	if err != nil {
		return nil, err
	}
	consecutive, _, err := c.boolean("consecutiveIds")
	if err != nil {
		return nil, err
	}
	seedProperty, _, err := c.text("seedProperty")
	if err != nil {
		return nil, err
	}
	if intermediate && consecutive {
		return nil, localizedError(localization.CypherGraphProceduresLouvainIntermediateConsecutive(), nil)
	}
	if seedProperty != "" && consecutive {
		return nil, localizedError(localization.CypherGraphProceduresLouvainSeedingConsecutive(), nil)
	}
	minSize, err := c.integer("minCommunitySize", "Integer", 1, math.MaxInt64, 1, false)
	if err != nil {
		return nil, err
	}
	weightProperty, _, err := c.text("relationshipWeightProperty")
	if err != nil {
		return nil, err
	}
	nodeLabels, err := c.names("nodeLabels")
	if err != nil {
		return nil, err
	}
	relationshipTypes, err := c.names("relationshipTypes")
	if err != nil {
		return nil, err
	}
	if _, err := c.integer("concurrency", "Integer", 1, 1<<31-1, 4, false); err != nil {
		return nil, err
	}
	if _, err := c.jobID(); err != nil {
		return nil, err
	}
	for _, key := range []string{"logProgress", "sudo"} {
		if _, _, err := c.boolean(key); err != nil {
			return nil, err
		}
	}
	if seedProperty != "" {
		var available []string
		for _, keys := range projection.NodePropertyKeys {
			for _, key := range keys {
				available = appendUnique(available, key)
			}
		}
		sort.Strings(available)
		if !containsString(available, seedProperty) {
			return nil, localizedError(localization.CypherGraphProceduresSeedPropertyNotFound(seedProperty, "["+strings.Join(available, ", ")+"]"), nil)
		}
		if gdsNodePropertyNotLong(projection, seedProperty) {
			return nil, localizedError(localization.CypherGraphProceduresSeedPropertyNotLong(seedProperty), nil)
		}
	}

	graph, err := selectGDSSubgraph(projection, nodeLabels, relationshipTypes, weightProperty, true)
	if err != nil {
		return nil, err
	}
	size := len(graph.nodes)
	// The relationships as projected, as GDS reads them: outgoing (an
	// UNDIRECTED projection holds each both ways), a missing weight NaN.
	louvain := newLouvainGraph(size)
	for source, edges := range graph.outgoing {
		for _, edge := range edges {
			louvain.arc(source, edge.target, edge.weight)
		}
	}

	// Seeds: nodes with equal seeds start in one community.
	name := func(node int) int64 { return int64(node) }
	if seedProperty != "" {
		run.seed = make([]int, size)
		seeds := make(map[int]int64)
		largest := int64(0)
		for i, id := range graph.nodes {
			run.seed[i] = i
			// The exact value: a large seed doesn't round through a float.
			value, ok := projection.NodeValues[id][seedProperty]
			if !ok {
				continue
			}
			seed, integer := value.(int64)
			if !integer {
				return nil, localizedError(localization.CypherGraphProceduresSeedPropertyNotLong(seedProperty), nil)
			}
			if seed == math.MinInt64 {
				continue // GDS's missing Integer property sentinel.
			}
			if seed < 0 {
				return nil, localizedError(localization.CypherGraphProceduresSeedNegative(), nil)
			}
			seeds[i], largest = seed, max(largest, seed)
		}
		name = func(node int) int64 {
			if seed, seeded := seeds[node]; seeded {
				return seed
			}
			// GDS adds the largest seed to the original numeric node ID.
			// String IDs retain NornicDB's existing projection-position names.
			original, err := strconv.ParseInt(graph.nodes[node], 10, 64)
			if err != nil {
				original = 1 + int64(projection.nodeIndex[graph.nodes[node]])
			}
			return largest + original
		}
		groups := make(map[int64]int)
		for i := range graph.nodes {
			seed := name(i)
			if first, seen := groups[seed]; seen {
				run.seed[i] = first
			} else {
				groups[seed] = i
			}
		}
	}
	result := runLouvain(louvain, run)
	last := len(result.levels) - 1
	final := result.levels[last]
	communityID := func(level, community int) int64 {
		if consecutive {
			return int64(community)
		}
		return name(result.representatives[level][community])
	}

	sizes := make(map[int]int)
	for _, community := range final {
		sizes[community]++
	}
	rows := make([][]interface{}, 0, size)
	for i, id := range graph.nodes {
		if int64(sizes[final[i]]) < minSize {
			continue
		}
		var levels interface{}
		if intermediate {
			list := make([]interface{}, len(result.levels))
			for level, membership := range result.levels {
				list[level] = communityID(level, membership[i])
			}
			levels = list
		}
		rows = append(rows, []interface{}{id, communityID(last, final[i]), levels})
	}
	return &ExecuteResult{Columns: []string{"nodeId", "communityId", "intermediateCommunityIds"}, Rows: rows}, nil
}

// gdsNodePropertyNotLong rejects any non-Integer schema for key, including
// lists, under any projected label, before applying nodeLabels filtering.
func gdsNodePropertyNotLong(projection *GraphProjection, key string) bool {
	labels, _ := projection.schema["nodes"].(map[string]interface{})
	for _, properties := range labels {
		properties, _ := properties.(map[string]interface{})
		if text, _ := properties[key].(string); text != "" && !strings.HasPrefix(text, "Integer ") {
			return true
		}
	}
	return false
}

// javaPlainDouble is a Double as GDS's range messages print it: its
// shortest digits, padded with zeros to its full integer part.
func javaPlainDouble(value float64) string {
	text := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(text, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	power, _ := strconv.Atoi(exponent)
	if power+1 > len(digits) {
		digits += strings.Repeat("0", power+1-len(digits))
	}
	return digits
}
