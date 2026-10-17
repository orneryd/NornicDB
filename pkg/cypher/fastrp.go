// FastRP (Fast Random Projection) implementation for Neo4j GDS compatibility.
//
// FastRP creates node embeddings based on graph structure using random projection.
// This is useful for downstream ML tasks like node classification, link prediction,
// and similarity search.
//
// Neo4j GDS FastRP API:
//   CALL gds.fastRP.stream(graphName, configuration)
//   CALL gds.fastRP.stats(graphName, configuration)
//   CALL gds.graph.project(graphName, nodeQuery, relationshipQuery, config)
//   CALL gds.graph.list()
//   CALL gds.graph.drop(graphName)
//   CALL gds.version()
//
// Example Usage:
//
//	// Create a graph projection
//	CALL gds.graph.project('myGraph', 'Person', 'KNOWS')
//
//	// Generate embeddings
//	CALL gds.fastRP.stream('myGraph', {embeddingDimension: 64})
//	YIELD nodeId, embedding
//	RETURN nodeId, embedding
//
//	// Clean up
//	CALL gds.graph.drop('myGraph')

package cypher

import (
	"context"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	mathrand "math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Memory constants for streaming thresholds
const (
	// streamChunkSize is the batch size for streaming operations
	streamChunkSize = 1000
	// maxFastRPEmbeddingDimension prevents pathological embedding requests from
	// forcing oversized exact-length allocations.
	maxFastRPEmbeddingDimension = 8192
)

// ============================================================================
// GDS Version
// ============================================================================

// callGdsVersion implements CALL gds.version(), with GDS's column name,
// gdsVersion (#1161); RETURN gds.version() is the same value.
func (e *StorageExecutor) callGdsVersion() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"gdsVersion"},
		Rows:    [][]any{{gdsVersion}},
	}, nil
}

// ============================================================================
// FastRP Algorithm
// ============================================================================

// FastRPConfig holds configuration for FastRP, as GDS's gds.fastRP reads it
// (#1191).
type FastRPConfig struct {
	EmbeddingDimension         int
	IterationWeights           []float64
	NormalizationStrength      float64
	NodeSelfInfluence          float64
	PropertyRatio              float64
	FeatureProperties          []string
	RelationshipWeightProperty string
	RandomSeed                 int64
	NodeLabels                 []string // nil: every projected label
	RelationshipTypes          []string // nil: every projected type
	// Configuration is the configuration as gds.fastRP.stats reports it.
	Configuration map[string]interface{}
}

// fastRPConfigKeys are the configuration keys gds.fastRP.stream and stats
// take.
var fastRPConfigKeys = []string{
	"embeddingDimension", "iterationWeights", "normalizationStrength", "nodeSelfInfluence", "propertyRatio",
	"featureProperties", "relationshipWeightProperty", "randomSeed", "nodeLabels", "relationshipTypes",
	"concurrency", "jobId", "logProgress", "sudo",
}

// callGdsFastRPStream implements CALL gds.fastRP.stream(graphName, config)
func (e *StorageExecutor) callGdsFastRPStream(args []interface{}, contexts ...context.Context) (*ExecuteResult, error) {
	embeddings, _, projection, err := e.runGdsFastRP("gds.fastRP.stream", args, contexts...)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, 0, len(embeddings))
	for _, embedding := range embeddings {
		rows = append(rows, []any{embedding.nodeID, embedding.vector})
	}
	return &ExecuteResult{Columns: []string{"nodeId", "embedding"}, Rows: rows, gdsProjection: projection}, nil
}

// callGdsFastRPStats implements CALL gds.fastRP.stats(graphName, config): it
// runs FastRP and reports, as GDS does, the node count, the time taken and
// the configuration (#1191).
func (e *StorageExecutor) callGdsFastRPStats(args []interface{}, contexts ...context.Context) (*ExecuteResult, error) {
	started := time.Now()
	embeddings, config, _, err := e.runGdsFastRP("gds.fastRP.stats", args, contexts...)
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{
		Columns: []string{"nodeCount", "preProcessingMillis", "computeMillis", "configuration"},
		Rows:    [][]any{{int64(len(embeddings)), int64(0), time.Since(started).Milliseconds(), config.Configuration}},
	}, nil
}

// runGdsFastRP reads a gds.fastRP call's graph and configuration and computes
// the embeddings.
func (e *StorageExecutor) runGdsFastRP(procedure string, args []interface{}, contexts ...context.Context) ([]fastRPEmbedding, FastRPConfig, *GraphProjection, error) {
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, FastRPConfig{}, nil, err
	}
	options, err := optionalProcedureMap(procedure, args, 1, "config")
	if err != nil {
		return nil, FastRPConfig{}, nil, err
	}
	config, err := fastRPConfiguration(options)
	if err != nil {
		return nil, FastRPConfig{}, nil, err
	}
	projection, err := e.graphProjection(graphName, contexts...)
	if err != nil {
		return nil, FastRPConfig{}, nil, err
	}
	embeddings, err := generateFastRPEmbeddings(projection, config)
	return embeddings, config, projection, err
}

// fastRPConfiguration reads gds.fastRP's configuration as GDS does (#1191):
// embeddingDimension is mandatory, a key FastRP doesn't take and a value of
// the wrong type or range are refused with GDS's messages. randomSeed
// defaults to 42, so the same call gives the same embeddings (GDS picks a
// random seed); embeddingDimension is at most maxFastRPEmbeddingDimension.
func fastRPConfiguration(options map[string]interface{}) (FastRPConfig, error) {
	config := FastRPConfig{IterationWeights: []float64{0.0, 1.0, 1.0}, RandomSeed: 42}
	c, err := newGDSConfig(options, fastRPConfigKeys...)
	if err != nil {
		return config, err
	}
	dimension, err := c.integer("embeddingDimension", "Integer", 1, maxFastRPEmbeddingDimension, 0, true)
	if err != nil {
		return config, err
	}
	config.EmbeddingDimension = int(dimension)
	if weights := c.value("iterationWeights"); weights != nil {
		items, ok := cypherListValue(weights)
		if !ok {
			return config, c.typeError("iterationWeights", "List")
		}
		config.IterationWeights = make([]float64, 0, len(items))
		for _, item := range items {
			number, isNumber := gdsScalarNumber(item)
			if !isNumber {
				return config, localizedError(localization.CypherGraphProceduresIterationWeightType(fmt.Sprint(item), gdsJavaType(item)), nil)
			}
			config.IterationWeights = append(config.IterationWeights, number)
		}
	}
	if config.NormalizationStrength, err = c.number("normalizationStrength", "Number", 0); err != nil {
		return config, err
	}
	if config.NodeSelfInfluence, err = c.number("nodeSelfInfluence", "Number", 0); err != nil {
		return config, err
	}
	if config.NodeSelfInfluence < 0 {
		return config, localizedError(localization.CypherGraphProceduresConfigValueNegative("nodeSelfInfluence"), nil)
	}
	if config.PropertyRatio, err = c.number("propertyRatio", "Double", 0); err != nil {
		return config, err
	}
	if features := c.value("featureProperties"); features != nil {
		items, ok := cypherListValue(features)
		if !ok {
			return config, c.typeError("featureProperties", "List")
		}
		for _, item := range items {
			name, isText := item.(string)
			if !isText {
				return config, c.typeError("featureProperties", "List")
			}
			config.FeatureProperties = append(config.FeatureProperties, name)
		}
	}
	if property, ok, err := c.text("relationshipWeightProperty"); err != nil {
		return config, err
	} else if ok {
		config.RelationshipWeightProperty = property
	}
	seed, err := c.integer("randomSeed", "Long", -1<<63, 1<<63-1, 42, false)
	if err != nil {
		return config, err
	}
	config.RandomSeed = seed
	if config.NodeLabels, err = c.names("nodeLabels"); err != nil {
		return config, err
	}
	if config.RelationshipTypes, err = c.names("relationshipTypes"); err != nil {
		return config, err
	}
	concurrency, err := c.integer("concurrency", "Integer", 1, 1<<31-1, 4, false)
	if err != nil {
		return config, err
	}
	logProgress, given, err := c.boolean("logProgress")
	if err != nil {
		return config, err
	}
	if !given {
		logProgress = true
	}
	sudo, _, err := c.boolean("sudo")
	if err != nil {
		return config, err
	}
	if err := config.validate(); err != nil {
		return config, err
	}

	jobID, err := c.jobID()
	if err != nil {
		return config, err
	}
	config.Configuration = map[string]interface{}{
		"embeddingDimension":    int64(config.EmbeddingDimension),
		"iterationWeights":      floatsToInterfaces(config.IterationWeights),
		"normalizationStrength": config.NormalizationStrength,
		"nodeSelfInfluence":     config.NodeSelfInfluence,
		"propertyRatio":         config.PropertyRatio,
		"featureProperties":     stringsToInterfaces(config.FeatureProperties),
		"nodeLabels":            namesOrAll(config.NodeLabels),
		"relationshipTypes":     namesOrAll(config.RelationshipTypes),
		"randomSeed":            config.RandomSeed,
		"concurrency":           concurrency,
		"logProgress":           logProgress,
		"sudo":                  sudo,
		"jobId":                 jobID,
	}
	if config.RelationshipWeightProperty != "" {
		config.Configuration["relationshipWeightProperty"] = config.RelationshipWeightProperty
	}
	return config, nil
}

// validate refuses a configuration GDS refuses as a whole.
func (c FastRPConfig) validate() error {
	if c.PropertyRatio < 0 || c.PropertyRatio > 1 {
		return localizedError(localization.CypherGraphProceduresConfigValueOutOfRange("propertyRatio", gdsNumberText(c.PropertyRatio, 'f', -1), "[0.00, 1.00]"), nil)
	}
	if c.PropertyRatio > 0 && len(c.FeatureProperties) == 0 {
		return localizedError(localization.CypherGraphProceduresFeaturePropertiesRequired(), nil)
	}
	if len(c.IterationWeights) == 0 && c.NodeSelfInfluence == 0 {
		return localizedError(localization.CypherGraphProceduresIterationWeightsEmpty(), nil)
	}
	return nil
}

func floatsToInterfaces(values []float64) []interface{} {
	out := make([]interface{}, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func stringsToInterfaces(values []string) []interface{} {
	out := make([]interface{}, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

// namesOrAll is a label or type filter as GDS reports it: ['*'] for none.
func namesOrAll(names []string) []interface{} {
	if len(names) == 0 {
		return []interface{}{"*"}
	}
	return stringsToInterfaces(names)
}

// fastRPEmbedding is one node's FastRP embedding.
type fastRPEmbedding struct {
	nodeID string
	vector []float64
}

// gdsSubgraph is the part of a projection a GDS algorithm reads (FastRP,
// node similarity, link prediction): the selected nodes and, between them,
// relationships of its relationshipTypes, each node's relationships as
// projected with their weights.
type gdsSubgraph struct {
	nodes    []string
	index    map[string]int
	outgoing [][]gdsEdge

	outgoingTypes [][]int // constituent graph ranks, requested only when an algorithm reads type graphs in turn
}

// gdsSubgraphOptions preserves explicit weight-property selection and requests
// type metadata or a relationship visitor without unused adjacency allocations.
type gdsSubgraphOptions struct {
	// Link prediction preserves its empty-list selection extension.
	emptyTypesNone bool

	weightRequested   bool
	includeTypes      bool
	omitAdjacency     bool
	visitRelationship func(*ProjectedRelationship)
}

type gdsEdge struct {
	target int
	weight float64
}

// selectGDSSubgraph selects nodes and relationships from proj, with
// GDS's errors for labels, types and weight keys the graph doesn't hold.
// Missing values stay NaN; algorithms decide whether and when to validate them.
func selectGDSSubgraph(proj *GraphProjection, nodeLabels, relationshipTypes []string, weightProperty string, options ...gdsSubgraphOptions) (*gdsSubgraph, error) {
	var selection gdsSubgraphOptions
	if len(options) > 0 {
		selection = options[0]
	}
	if missing := missingNames(nodeLabels, proj.Labels); len(missing) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresNodeLabelsNotInGraph(gdsNameList(missing), gdsNameList(proj.Labels)), nil)
	}
	types, err := proj.resolveRelationshipTypes(relationshipTypes, !selection.emptyTypesNone, "selection")
	if err != nil {
		return nil, err
	}
	if property := weightProperty; property != "" || selection.weightRequested {
		available := commonKeys(proj.RelationshipPropertyKeys, types)
		if !containsString(available, property) {
			return nil, localizedError(localization.CypherGraphProceduresWeightPropertyNotFound(property, gdsNameList(types), gdsNameList(available)), nil)
		}
	}
	// One selected constituent needs only target ordering. Repeated names in
	// the selection do not introduce another graph or require per-arc ranks.
	includeTypes := false
	if selection.includeTypes && !selection.omitAdjacency && len(types) > 1 {
		for _, relationshipType := range types[1:] {
			if relationshipType != types[0] {
				includeTypes = true
				break
			}
		}
	}
	graph := &gdsSubgraph{}
	// The projection owns an immutable index. Without a label restriction,
	// reuse it; filtered selections build only their selected-node index.
	if len(nodeLabels) == 0 && len(proj.nodeIndex) == len(proj.NodeIDs) {
		graph.nodes, graph.index = proj.NodeIDs, proj.nodeIndex
	} else {
		graph.index = make(map[string]int)
		for _, id := range proj.NodeIDs {
			if !proj.matchesNodeLabels(id, nodeLabels) {
				continue
			}
			graph.index[id] = len(graph.nodes)
			graph.nodes = append(graph.nodes, id)
		}
	}
	if !selection.omitAdjacency {
		graph.outgoing = make([][]gdsEdge, len(graph.nodes))
	}
	if includeTypes {
		graph.outgoingTypes = make([][]int, len(graph.nodes))
	}
	// A nodes-only consumer need not traverse relationships.
	if selection.omitAdjacency && selection.visitRelationship == nil {
		return graph, nil
	}
	for i := range proj.Relationships {
		relationship := &proj.Relationships[i]
		if !containsString(types, relationship.Type) {
			continue
		}
		source, ok := graph.index[relationship.Source]
		if !ok {
			continue
		}
		target, ok := graph.index[relationship.Target]
		if !ok {
			continue
		}
		if selection.visitRelationship != nil {
			selection.visitRelationship(relationship)
		}
		if selection.omitAdjacency {
			continue
		}
		weight := 1.0
		if property := weightProperty; property != "" || selection.weightRequested {
			// A NaN weight is a missing one, as GDS reads it.
			value, ok := relationship.Properties[property]
			if !ok {
				value = math.NaN()
			}
			weight = value
		}
		graph.outgoing[source] = append(graph.outgoing[source], gdsEdge{target: target, weight: weight})
		if includeTypes {
			graph.outgoingTypes[source] = append(graph.outgoingTypes[source], proj.relationshipTypeIndex[relationship.Type])
		}
	}
	return graph, nil
}

// generateFastRPEmbeddings is GDS's FastRP (#1191) over the projection's
// nodes, in its node order. Each node starts from a sparse random vector
// (L2-normalised, scaled by its degree to normalizationStrength), the last
// propertyRatio of whose dimensions hold its featureProperties, each value
// (each element of a list property) times a random vector of its own. Each
// iteration gives every node the relationship-weighted average of the
// previous iteration's vectors over its relationships, as projected
// (outgoing; both ways for UNDIRECTED), which the next iteration reads; the
// embedding adds each iteration's vector L2-normalised, times its
// iterationWeights entry, plus nodeSelfInfluence times the normalised start,
// as GDS does. A node nothing propagates to gets a zero vector.
func generateFastRPEmbeddings(proj *GraphProjection, config FastRPConfig) ([]fastRPEmbedding, error) {
	graph, err := selectGDSSubgraph(proj, config.NodeLabels, config.RelationshipTypes, config.RelationshipWeightProperty)
	if err != nil {
		return nil, err
	}
	dim := util.SafePreallocCap(config.EmbeddingDimension, maxFastRPEmbeddingDimension)
	size := len(graph.nodes)
	if size == 0 || dim <= 0 {
		return nil, nil
	}
	propDim := int(float64(dim) * config.PropertyRatio)
	var features [][]float64 // each node's feature values, a list property's elements in turn
	featureCount := 0
	if propDim > 0 {
		labels := config.NodeLabels
		if len(labels) == 0 {
			labels = proj.Labels
		}
		available := commonKeys(proj.NodePropertyKeys, labels)
		if missing := missingNames(config.FeatureProperties, available); len(missing) > 0 {
			return nil, localizedError(localization.CypherGraphProceduresFeaturePropertiesMissing(gdsNameList(config.FeatureProperties), gdsNameList(labels), gdsNameList(available)), nil)
		}
		widths := make([]int, len(config.FeatureProperties))
		features = make([][]float64, size)
		for i, id := range graph.nodes {
			for p, name := range config.FeatureProperties {
				values, err := fastRPFeatureValues(proj, id, name, i)
				if err != nil {
					return nil, err
				}
				// A list property's arrays must all be one length.
				if i == 0 {
					widths[p] = len(values)
				} else if len(values) != widths[p] {
					return nil, localizedError(localization.CypherGraphProceduresArrayLengthsDiffer(name, strconv.Itoa(len(values)), strconv.Itoa(widths[p])), nil)
				}
				features[i] = append(features[i], values...)
			}
		}
		featureCount = len(features[0])
	}

	rng := mathrand.New(mathrand.NewSource(config.RandomSeed))
	initial := make([][]float64, size)
	topologyDim := dim - propDim
	for i := range initial {
		initial[i] = make([]float64, dim)
		for j := 0; j < topologyDim; j++ {
			initial[i][j] = sparseRandomValue(rng)
		}
		normalizeEmbeddings(initial[i:i+1], topologyDim)
		if config.NormalizationStrength != 0 {
			if degree := len(graph.outgoing[i]); degree > 0 {
				scale := math.Pow(float64(degree), config.NormalizationStrength)
				for j := 0; j < topologyDim; j++ {
					initial[i][j] *= scale
				}
			}
		}
	}
	if propDim > 0 {
		featureVectors := make([][]float64, featureCount)
		for p := range featureVectors {
			featureVectors[p] = make([]float64, propDim)
			for k := range featureVectors[p] {
				featureVectors[p][k] = sparseRandomValue(rng)
			}
		}
		for i := range initial {
			for f, value := range features[i] {
				for k, r := range featureVectors[f] {
					initial[i][topologyDim+k] += value * r
				}
			}
		}
	}

	previous := initial
	final := make([][]float64, size)
	for i := range final {
		final[i] = make([]float64, dim)
	}
	for iteration, weight := range config.IterationWeights {
		current := make([][]float64, size)
		for i := range current {
			current[i] = make([]float64, dim)
			for _, edge := range graph.outgoing[i] {
				// GDS validates weights on the first propagation, even if
				// this iteration contributes zero to the final embedding.
				if iteration == 0 && math.IsNaN(edge.weight) {
					return nil, localizedError(localization.CypherGraphProceduresRelationshipPropertyMissing(config.RelationshipWeightProperty, strconv.Itoa(i), strconv.Itoa(edge.target)), nil)
				}
				for j, value := range previous[edge.target] {
					current[i][j] += value * edge.weight
				}
			}
			if degree := len(graph.outgoing[i]); degree > 1 {
				for j := range current[i] {
					current[i][j] /= float64(degree)
				}
			}
			if norm := vectorNorm(current[i]); weight != 0 && norm > 0 {
				for j, value := range current[i] {
					final[i][j] += weight * value / norm
				}
			}
		}
		previous = current
	}
	if config.NodeSelfInfluence != 0 {
		for i := range final {
			norm := vectorNorm(initial[i])
			if norm < fastRPEpsilon {
				norm = 1
			}
			for j, value := range initial[i] {
				final[i][j] += config.NodeSelfInfluence * value / norm
			}
		}
	}

	embeddings := make([]fastRPEmbedding, size)
	for i, id := range graph.nodes {
		embeddings[i] = fastRPEmbedding{nodeID: id, vector: final[i]}
	}
	return embeddings, nil
}

// fastRPEpsilon is GDS's smallest L2 norm FastRP divides by (10 /
// Float.MAX_VALUE); a smaller start vector is taken as it is.
const fastRPEpsilon = 10 / 3.40282346638528859811704183484516925440e+38

// vectorNorm is a vector's L2 norm.
func vectorNorm(vector []float64) float64 {
	sum := 0.0
	for _, value := range vector {
		sum += value * value
	}
	return math.Sqrt(sum)
}

// fastRPFeatureValues reads scalar and array features through the same
// typed reader, then validates every component in one place.
func fastRPFeatureValues(proj *GraphProjection, id, name string, position int) ([]float64, error) {
	kind, invalid := "Float", "`NaN`"
	if strings.HasPrefix(proj.NodePropertyTypes[name], "List") {
		kind, invalid = "List of Float", "NaN"
	}
	value, err := proj.nodePropertyValue(id, name, kind)
	if err != nil {
		return nil, err
	}
	values, list := value.([]float64)
	if !list {
		values = []float64{value.(float64)}
	}
	for _, component := range values {
		if math.IsNaN(component) {
			return nil, localizedError(localization.CypherGraphProceduresFeaturePropertyInvalid(strconv.Itoa(position), name, invalid), nil)
		}
	}
	return values, nil
}

// missingNames is the names not in available.
func missingNames(names, available []string) []string {
	var missing []string
	for _, name := range names {
		if !containsString(available, name) {
			missing = append(missing, name)
		}
	}
	return missing
}

// commonKeys is the keys every one of names has in keys, in order.
func commonKeys(keys map[string][]string, names []string) []string {
	if len(names) == 0 {
		return nil
	}
	var common []string
	for _, key := range keys[names[0]] {
		inAll := true
		for _, name := range names[1:] {
			if !containsString(keys[name], key) {
				inAll = false
				break
			}
		}
		if inAll {
			common = append(common, key)
		}
	}
	return common
}

// fastRPConfigInt reads an integer config value (an INTEGER, or a FLOAT
// with no fraction).
func fastRPConfigInt(value interface{}) (int, bool) {
	number, ok := gdsConfigIntegerValue(value, true)
	return int(number), ok
}

// anyString reports whether values holds any of wanted.
func anyString(values, wanted []string) bool {
	for _, value := range values {
		if containsString(wanted, value) {
			return true
		}
	}
	return false
}

// initializeEmbeddingChunk initializes embeddings for a chunk of nodes
func initializeEmbeddingChunk(embeddings [][]float64, start, end, dim int, rng *mathrand.Rand) {
	dim = util.SafePreallocCap(dim, maxFastRPEmbeddingDimension)
	if dim <= 0 || dim > maxFastRPEmbeddingDimension {
		for i := start; i < end; i++ {
			embeddings[i] = nil
		}
		return
	}
	for i := start; i < end; i++ {
		// dim is directly bounded above before this allocation.
		embeddings[i] = make([]float64, dim)
		for j := 0; j < dim; j++ {
			embeddings[i][j] = sparseRandomValue(rng)
		}
	}
}

// sparseRandomValue is one coordinate of a sparse random projection: -1, 0
// or 1 with probabilities 1/6, 2/3 and 1/6.
func sparseRandomValue(rng *mathrand.Rand) float64 {
	r := rng.Float64()
	if r < 1.0/6.0 {
		return -1.0
	} else if r > 5.0/6.0 {
		return 1.0
	}
	return 0.0
}

// normalizeEmbeddings L2 normalizes the first dim coordinates of each
// embedding in place; a zero vector stays zero.
func normalizeEmbeddings(embeddings [][]float64, dim int) {
	for i := range embeddings {
		if embeddings[i] == nil {
			continue
		}
		norm := 0.0
		for j := 0; j < dim; j++ {
			norm += embeddings[i][j] * embeddings[i][j]
		}
		if norm > 0 {
			invNorm := 1.0 / math.Sqrt(norm)
			for j := 0; j < dim; j++ {
				embeddings[i][j] *= invNorm
			}
		}
	}
}
