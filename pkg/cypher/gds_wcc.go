package cypher

import (
	"math"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/wcc"
)

// wccConfigKeys are the configuration keys gds.wcc.stats takes; the stream
// also takes minComponentSize, as in GDS.
var wccConfigKeys = []string{
	"relationshipWeightProperty", "threshold", "seedProperty", "consecutiveIds",
	"nodeLabels", "relationshipTypes", "concurrency", "jobId", "logProgress", "sudo",
}

// wccResult is one gds.wcc run over the selected subgraph: each node's
// componentId, the number of nodes with that componentId, and whether the
// node is the first with it; and the configuration as GDS reports it.
type wccResult struct {
	projection *GraphProjection
	graph      *gdsSubgraph
	component  []int64
	size       []int
	first      []bool

	consecutive   bool
	minSize       int64
	configuration map[string]interface{}
}

// callGdsWCCStream implements gds.wcc.stream(graphName, configuration) as
// GDS 2.13 does: the weakly connected components of the projection's
// selected nodes, joined by its selected relationships read both ways
// (whatever their projected orientation); a node without any is its own
// component. consecutiveIds numbers the surviving components 0, 1, … by
// first node after minComponentSize drops smaller ones. Rows are in the
// projection's node order; nodeId is Louvain's: the INTEGER an aggregation
// row supplied, as in GDS, or else the stored node ID.
func (e *StorageExecutor) callGdsWCCStream(args []interface{}) (*ExecuteResult, error) {
	run, err := e.runGdsWCC("gds.wcc.stream", args, true)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0, len(run.graph.nodes))
	var visible []int
	for i, id := range run.graph.nodes {
		if int64(run.size[i]) < run.minSize {
			continue
		}
		rows = append(rows, []interface{}{run.projection.nodeIDValue(id), run.component[i]})
		if run.consecutive {
			visible = append(visible, int(run.component[i]))
		}
	}
	if run.consecutive {
		louvain.Renumber(visible)
		for i, component := range visible {
			rows[i][1] = int64(component)
		}
	}
	return &ExecuteResult{Columns: []string{"nodeId", "componentId"}, Rows: rows}, nil
}

// callGdsWCCStats implements gds.wcc.stats(graphName, configuration): the
// number of components and the distribution of their sizes, from the same
// run as the stream; GDS reports no distribution for no components.
func (e *StorageExecutor) callGdsWCCStats(args []interface{}) (*ExecuteResult, error) {
	started := time.Now()
	run, err := e.runGdsWCC("gds.wcc.stats", args, false)
	if err != nil {
		return nil, err
	}
	computed := time.Now()
	var sizes []int64
	for i, first := range run.first {
		if first {
			sizes = append(sizes, int64(run.size[i]))
		}
	}
	distribution := map[string]interface{}{}
	if len(sizes) > 0 {
		distribution = gdsDistribution(sizes, gdsComponentPercentiles)
	}
	return &ExecuteResult{
		Columns: []string{"componentCount", "componentDistribution", "postProcessingMillis", "preProcessingMillis", "computeMillis", "configuration"},
		Rows: [][]interface{}{{
			int64(len(sizes)), distribution, time.Since(computed).Milliseconds(), int64(0), computed.Sub(started).Milliseconds(), run.configuration,
		}},
	}, nil
}

// runGdsWCC reads a gds.wcc call's graph and configuration and computes the
// components through the shared union-find, as GDS 2.13.12 does:
//   - A threshold above zero, with relationshipWeightProperty, keeps only
//     relationships weighing more (a missing weight, NaN, never does); a
//     threshold of zero or less keeps every relationship. A threshold above
//     zero without relationshipWeightProperty is refused.
//   - Without seedProperty, a component is named by its lowest node position
//     in the selected subgraph.
//   - With seedProperty, a component is named by the least of its members'
//     seeds. A node without a seed, or with a negative one, counts as the
//     largest seed + 1 + its rank among such nodes in node order, so a
//     component without seeds is named above every seed. GDS numbers those
//     nodes in its traversal order instead, which depends on concurrency.
//     Components that share a seed share their name, and count as one
//     component for minComponentSize and the statistics, as in GDS.
func (e *StorageExecutor) runGdsWCC(procedure string, args []interface{}, stream bool) (*wccResult, error) {
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
	keys := wccConfigKeys
	if stream {
		keys = append(append([]string(nil), keys...), "minComponentSize")
	}
	c, err := newGDSConfig(options, keys...)
	if err != nil {
		return nil, err
	}
	run := &wccResult{projection: projection, minSize: math.MinInt64}
	if stream {
		if run.minSize, err = c.integer("minComponentSize", "Integer", math.MinInt64, math.MaxInt64, 0, false); err != nil {
			return nil, err
		}
	}
	weightProperty, weighted, err := c.text("relationshipWeightProperty")
	if err != nil {
		return nil, err
	}
	threshold, err := c.number("threshold", "Double", 0)
	if err != nil {
		return nil, err
	}
	if run.consecutive, _, err = c.boolean("consecutiveIds"); err != nil {
		return nil, err
	}
	seedProperty, _, err := c.text("seedProperty")
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
	base, err := c.algorithmBase()
	if err != nil {
		return nil, err
	}
	if threshold > 0 && !weighted {
		return nil, localizedError(localization.CypherGraphProceduresThresholdRequiresWeight(), nil)
	}
	if seedProperty != "" && run.consecutive {
		return nil, localizedError(localization.CypherGraphProceduresLouvainSeedingConsecutive(), nil)
	}
	if err := projection.validateSeedProperty(nodeLabels, seedProperty); err != nil {
		return nil, err
	}
	run.graph, err = selectGDSSubgraph(projection, nodeLabels, relationshipTypes, weightProperty, gdsSubgraphOptions{weightRequested: weighted})
	if err != nil {
		return nil, err
	}

	size := len(run.graph.nodes)
	filtered := weighted && threshold > 0
	forest := wcc.New(size)
	for source, edges := range run.graph.outgoing {
		for _, edge := range edges {
			if !filtered || edge.weight > threshold {
				forest.Union(source, edge.target)
			}
		}
	}
	run.component = make([]int64, size)
	run.size = make([]int, size)
	run.first = make([]bool, size)
	if seedProperty == "" {
		for i, lowest := range forest.Lowest() {
			run.component[i], run.size[i], run.first[i] = int64(lowest), forest.Size(i), lowest == i
		}
	} else if err := run.nameBySeeds(projection, forest, seedProperty); err != nil {
		return nil, err
	}

	run.configuration = map[string]interface{}{
		"nodeLabels":        namesOrAll(nodeLabels),
		"relationshipTypes": namesOrAll(relationshipTypes),
		"consecutiveIds":    run.consecutive,
		"threshold":         threshold,
		"seedProperty":      nil,
	}
	if seedProperty != "" {
		run.configuration["seedProperty"] = seedProperty
	}
	if weighted {
		run.configuration["relationshipWeightProperty"] = weightProperty
	}
	base.echo(run.configuration)
	return run, nil
}

// nameBySeeds names each component by the least of its members' seeds
// (runGdsWCC), and groups the components by name.
func (run *wccResult) nameBySeeds(projection *GraphProjection, forest *wcc.Forest, seedProperty string) error {
	seeds, err := projection.seedValues(run.graph, seedProperty)
	if err != nil {
		return err
	}
	largest := int64(-1)
	for _, seed := range seeds {
		largest = max(largest, seed)
	}
	least := make([]int64, len(seeds)) // by component root
	for i := range least {
		least[i] = math.MaxInt64
	}
	unseeded := largest + 1
	for i, seed := range seeds {
		if seed < 0 {
			seed = unseeded
			unseeded++
		}
		root := forest.Find(i)
		least[root] = min(least[root], seed)
	}
	members := make(map[int64]int)
	for i := range run.component {
		run.component[i] = least[forest.Find(i)]
		members[run.component[i]]++
	}
	seen := make(map[int64]bool, len(members))
	for i, name := range run.component {
		run.size[i], run.first[i] = members[name], !seen[name]
		seen[name] = true
	}
	return nil
}
