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
// relationships (one undirected relationship each, its weight the
// relationshipWeightProperty's value), with GDS's maxLevels (10),
// maxIterations (10) and tolerance (0.0001), seedProperty for the initial
// communities, minCommunitySize, and includeIntermediateCommunities for
// each level's community. Rows are in the projection's node order; the
// communities are numbered by their first node, as consecutiveIds numbers
// them in GDS (GDS's own default ids are internal node ids). A seeded
// community keeps its first seeded node's seed, others are numbered after
// the largest seed.
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
	if run.tolerance < 0 {
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
	if intermediate && consecutive {
		return nil, localizedError(localization.CypherGraphProceduresLouvainIntermediateConsecutive(), nil)
	}
	minSize, err := c.integer("minCommunitySize", "Integer", 0, 1<<31-1, 0, false)
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
	seedProperty, _, err := c.text("seedProperty")
	if err != nil {
		return nil, err
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
	}

	graph, err := selectGDSSubgraph(projection, nodeLabels, relationshipTypes, weightProperty)
	if err != nil {
		return nil, err
	}
	size := len(graph.nodes)
	louvain := newLouvainGraph(size)
	linked := make(map[string]bool)
	for source, edges := range graph.outgoing {
		for _, edge := range edges {
			// One undirected relationship each: an UNDIRECTED projection
			// holds it both ways.
			if linked[edge.id] {
				continue
			}
			linked[edge.id] = true
			louvain.link(source, edge.target, edge.weight)
		}
	}

	// Seeds: nodes with equal seeds start in one community.
	var seeds []float64
	if seedProperty != "" {
		run.seed = make([]int, size)
		seeds = make([]float64, size)
		groups := make(map[float64]int)
		for i, id := range graph.nodes {
			value, ok := projection.NodeProperties[id][seedProperty]
			seeds[i] = math.NaN()
			if !ok {
				run.seed[i] = i
				continue
			}
			seeds[i] = value
			first, seen := groups[value]
			if !seen {
				first = i
				groups[value] = i
			}
			run.seed[i] = first
		}
	}
	result := runLouvain(louvain, run)
	final := louvainFinal(result, size)
	ids := louvainCommunityIDs(final, seeds)

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
				list[level] = int64(membership[i])
			}
			levels = list
		}
		rows = append(rows, []interface{}{id, ids[final[i]], levels})
	}
	return &ExecuteResult{Columns: []string{"nodeId", "communityId", "intermediateCommunityIds"}, Rows: rows}, nil
}

// louvainCommunityIDs is each community's id: its number (by first node),
// or with seeds, its first seeded node's seed, the unseeded communities
// numbered after the largest seed.
func louvainCommunityIDs(final []int, seeds []float64) map[int]int64 {
	ids := make(map[int]int64)
	if seeds == nil {
		for _, community := range final {
			ids[community] = int64(community)
		}
		return ids
	}
	next := int64(0)
	for _, seed := range seeds {
		if !math.IsNaN(seed) && int64(seed) >= next {
			next = int64(seed) + 1
		}
	}
	for i, community := range final {
		if _, numbered := ids[community]; numbered || math.IsNaN(seeds[i]) {
			continue
		}
		ids[community] = int64(seeds[i])
	}
	for _, community := range final {
		if _, numbered := ids[community]; !numbered {
			ids[community] = next
			next++
		}
	}
	return ids
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
