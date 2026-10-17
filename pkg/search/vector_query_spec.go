package search

import (
	"context"
	"errors"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// VectorQuerySpec describes how to resolve embeddings for a Cypher-style vector query.
//
// This is intentionally a "core" representation of Cypher vector index metadata:
// the Cypher layer can remain syntax-compatible, while the search layer owns the
// implementation details and can choose the best execution strategy.
type VectorQuerySpec struct {
	// IndexName is informational only (used for debugging/observability).
	IndexName string

	// Label optionally filters candidate nodes to those that have this label.
	Label string

	// Property is the Cypher vector index "property" name.
	//
	// Resolution semantics:
	//   1) Prefer NamedEmbeddings[Property] (or "default" when Property is empty)
	//   2) If Property is set, next try node.Properties[Property] as a vector array
	//   3) Fallback to ChunkEmbeddings[0..N] (best score across chunks)
	Property string

	// Similarity is the similarity function name. Supported values:
	// "cosine" (default), "dot", "euclidean".
	Similarity string

	// Limit is the maximum number of results to return (top-K).
	Limit int

	// ScoreScale is the scale of each hit's Score: Neo4j's (the zero value, for
	// Cypher) or the raw similarity (for the Qdrant API).
	ScoreScale VectorScoreScale

	// Exact evaluates every indexed node instead of an approximate candidate set.
	Exact bool
}

// VectorScoreScale is the scale a vector query reports its scores on. It never
// changes which nodes are returned or their order.
type VectorScoreScale int

const (
	// VectorScoreNeo4j is Neo4j's scale for its vector indexes (#907): cosine
	// max((1 + cos) / 2, 0) and euclidean 1 / (1 + d²), both in [0, 1]; dot is
	// the dot product.
	VectorScoreNeo4j VectorScoreScale = iota
	// VectorScoreSimilarity is the raw similarity, as Qdrant reports it: the
	// cosine similarity in [-1, 1], the dot product, and for euclidean
	// 1 / (1 + d) (vector.EuclideanSimilarity).
	VectorScoreSimilarity
)

// VectorQueryHit is a lightweight result row for Cypher-compatible vector queries.
// ID is the node ID (not a chunk/named vector ID).
type VectorQueryHit struct {
	ID    string
	Score float64
}

// RelationshipVectorQuerySpec describes a Cypher-style relationship vector query.
type RelationshipVectorQuerySpec struct {
	// IndexName is informational only (used for debugging/observability).
	IndexName string

	// Type optionally filters candidate relationships by relationship type.
	Type string

	// Property is the relationship vector property name.
	Property string

	// Similarity is the similarity function name. Supported values:
	// "cosine" (default), "dot", "euclidean".
	Similarity string

	// Limit is the maximum number of results to return.
	Limit int
}

// RelationshipVectorQueryHit is a lightweight result row for relationship vector queries.
// ID is the relationship ID.
type RelationshipVectorQueryHit struct {
	ID    string
	Score float64
}

// VectorQueryNodes executes a Cypher-style vector query.
//
// This method preserves Cypher semantics for per-node embedding selection:
//  1. Prefer NamedEmbeddings[Property] (or "default" when Property is empty)
//  2. If Property is set, next try node.Properties[Property] as a vector array
//  3. Fallback to ChunkEmbeddings[0..N] (best score across chunks)
//
// For performance, cosine-similarity queries are executed against the in-memory
// vector index (unified pipeline) rather than scanning storage. This legacy
// entry point preserves live-indexed Cypher reads before full warmup; use
// VectorQueryNodesWithOptions for reads requiring warming/readiness.
//
// Scores are on spec.ScoreScale; the zero value is Neo4j's scale for its vector
// indexes (#907): cosine max((1 + cos) / 2, 0) and euclidean 1 / (1 + d²), both
// in [0, 1] (vector.Neo4jCosineSimilarity, vector.Neo4jEuclideanSimilarity);
// dot, a NornicDB similarity, is the dot product.
func (s *Service) VectorQueryNodes(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec) ([]VectorQueryHit, error) {
	return s.vectorQueryNodes(ctx, queryEmbedding, spec, nil, false)
}

// VectorQueryNodesWithOptions is VectorQueryNodes for a caller with its own
// search options. Of opts it reads HNSWEf, the query's own HNSW beam width, and
// Where, which admits candidate nodes before the top-K cut, as a Cypher WHERE
// on the node does; nil opts uses default options. It is the vector query the
// Qdrant API's Search runs, on the same index and strategy as Cypher's.
// Unlike the legacy scalar-vector API, this optioned entry point warms a lazy
// service and rejects an incomplete build before returning any results.
//
// Example:
//
//	hits, err := svc.VectorQueryNodesWithOptions(ctx, query, search.VectorQuerySpec{
//		Label: "QdrantPoint", Property: "default", Similarity: "cosine",
//		Limit: 10, ScoreScale: search.VectorScoreSimilarity,
//	}, &search.SearchOptions{HNSWEf: 128, Where: func(n *storage.Node) bool { return n.Properties["tag"] == "a" }})
func (s *Service) VectorQueryNodesWithOptions(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, opts *SearchOptions) ([]VectorQueryHit, error) {
	return s.vectorQueryNodes(ctx, queryEmbedding, spec, opts, true)
}

// vectorQueryNodes shares vector semantics across the two public contracts.
// Cypher's established scalar fast path can use live indexed vectors during
// warming; optioned reads (Qdrant) require the configured population to finish.
func (s *Service) vectorQueryNodes(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, opts *SearchOptions, requireReady bool) ([]VectorQueryHit, error) {
	if s == nil || s.engine == nil {
		return nil, localizedError(localization.SearchServiceUnavailable(), nil)
	}
	if len(queryEmbedding) == 0 {
		return nil, localizedError(localization.SearchQueryEmbeddingRequired(), nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if requireReady {
		if err := s.prepareSearchRead(ctx); err != nil {
			return nil, err
		}
	}
	if spec.Limit <= 0 {
		spec.Limit = 10
	}

	vectorName := spec.Property
	if vectorName == "" {
		vectorName = "default"
	}
	similarity := spec.Similarity
	if similarity == "" {
		similarity = "cosine"
	}

	if !s.vectorEnabled.Load() {
		return nil, nil
	}

	// If the query dimensions don't match the configured index dimensions, Cypher expects
	// an empty result set (not an error). The active vector backing may be either the
	// in-memory VectorIndex or the file-backed VectorFileStore.
	if dims := s.VectorIndexDimensions(); dims > 0 && s.EmbeddingCount() > 0 && len(queryEmbedding) != dims {
		return nil, nil
	}

	// Fast path: cosine queries can use the indexed vector pipeline.
	if strings.EqualFold(similarity, "cosine") && !spec.Exact {
		hits, err := s.vectorQueryNodesIndexed(ctx, queryEmbedding, spec, vectorName, opts)
		if spec.ScoreScale == VectorScoreNeo4j {
			for i := range hits {
				hits[i].Score = neo4jCosineScore(hits[i].Score)
			}
		}
		return hits, err
	}

	// Exact (index-backed) path for dot/euclidean: compute per-node scores from in-memory vectors
	// without scanning storage.
	return s.vectorQueryNodesExact(ctx, queryEmbedding, spec, vectorName, strings.ToLower(similarity), opts)
}

// VectorQueryRelationships executes a Cypher-style relationship vector query using
// in-memory relationship-vector metadata. This avoids scanning storage.AllEdges()
// on every fact-search call.
func (s *Service) VectorQueryRelationships(ctx context.Context, queryEmbedding []float32, spec RelationshipVectorQuerySpec) ([]RelationshipVectorQueryHit, error) {
	if s == nil || s.engine == nil {
		return nil, localizedError(localization.SearchServiceUnavailable(), nil)
	}
	if len(queryEmbedding) == 0 {
		return nil, localizedError(localization.SearchQueryEmbeddingRequired(), nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if spec.Limit <= 0 {
		spec.Limit = 10
	}
	if spec.Property == "" {
		return nil, nil
	}
	similarity := strings.ToLower(strings.TrimSpace(spec.Similarity))
	if similarity == "" {
		similarity = "cosine"
	}

	type edgeMeta struct {
		id  string
		vec []float32
	}
	candidates := make([]edgeMeta, 0, util.SafePreallocProduct(spec.Limit, 2))
	// The node and relationship metadata is written under indexMu (index
	// writes): lock order indexMu -> mu.
	s.indexMu.RLock()
	s.mu.RLock()
	for edgeID, props := range s.edgePropVector {
		if spec.Type != "" && s.edgeTypes[edgeID] != spec.Type {
			continue
		}
		vec, ok := props[spec.Property]
		if !ok || len(vec) != len(queryEmbedding) {
			continue
		}
		// Relationship vectors are copied when indexed and then treated as immutable.
		// Snapshot the slice header only; copying every vector per query dominates
		// Graphiti relationship retrieval allocations.
		candidates = append(candidates, edgeMeta{id: edgeID, vec: vec})
	}
	s.mu.RUnlock()
	s.indexMu.RUnlock()
	if len(candidates) == 0 {
		return nil, nil
	}

	type scoredEdge struct {
		candidate int
		score     float64
	}
	// Every candidate is ranked with one cheap pass; only the returned hits
	// get Neo4j's exact score, which costs two strictly ordered float32
	// passes (cypherVectorRanker).
	prepared := newCypherVectorQuery(similarity, queryEmbedding)
	scored := make([]scoredEdge, 0, len(candidates))
	for index, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		rank := prepared.rank(cand.vec)
		if math.IsInf(rank, -1) {
			continue
		}
		scored = append(scored, scoredEdge{candidate: index, score: rank})
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if len(scored) > spec.Limit {
		scored = scored[:spec.Limit]
	}
	// The hits' exact scores, kept in descending order (an insertion sort:
	// the ranking left them in order but for near-ties).
	for i := range scored {
		scored[i].score = prepared.score(candidates[scored[i].candidate].vec)
		for j := i; j > 0 && scored[j].score > scored[j-1].score; j-- {
			scored[j], scored[j-1] = scored[j-1], scored[j]
		}
	}
	out := make([]RelationshipVectorQueryHit, 0, len(scored))
	for _, r := range scored {
		out = append(out, RelationshipVectorQueryHit{ID: candidates[r.candidate].id, Score: r.score})
	}
	return out, nil
}

func (s *Service) vectorQueryNodesIndexed(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, vectorName string, caller *SearchOptions) ([]VectorQueryHit, error) {
	opts := DefaultSearchOptions()
	opts.Limit = spec.Limit
	opts.CandidateTarget = spec.Limit
	if caller != nil {
		opts.HNSWEf = caller.HNSWEf
		opts.QuantizationIgnore = caller.QuantizationIgnore
		opts.QuantizationRescore = caller.QuantizationRescore
		opts.QuantizationOversampling = caller.QuantizationOversampling
		opts.Where = caller.Where
	}
	return s.vectorQueryNodesIndexedWithOptions(ctx, queryEmbedding, spec, vectorName, opts)
}

func (s *Service) vectorQueryNodesIndexedWithOptions(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, vectorName string, opts *SearchOptions) ([]VectorQueryHit, error) {
	if s.EmbeddingCount() == 0 {
		return nil, nil
	}

	pipeline, err := s.getOrCreateVectorPipeline(ctx)
	if err != nil {
		return nil, err
	}

	resolved := DefaultSearchOptions()
	if opts != nil {
		copy := *opts
		resolved = &copy
	}
	resolved.Limit = spec.Limit
	if resolved.CandidateTarget <= 0 {
		resolved.CandidateTarget = spec.Limit
	}
	resolved.MaxOverfetchRatio = math.MaxFloat64
	minSimilarity := -1.0
	resolved.MinSimilarity = &minSimilarity
	normalizedQuery := vector.Normalize(queryEmbedding)
	ctx, err = withQuantizationOptions(ctx, resolved)
	if err != nil {
		return nil, err
	}
	var quantizedIndex *HNSWIndex
	if h, ok := pipeline.candidateGen.(*HNSWCandidateGen); ok && h.quantizationActive(ctx) && !quantizationOptions(ctx).rescore {
		quantizedIndex = h.hnswIndex
	}

	var supplement []vectorQueryScoredNode
	supplementReady := false

	var postProcessErr error
	seenOrphans := make(map[string]bool)
	results, _, err := s.adaptiveVectorSearch(ctx, pipeline, queryEmbedding, resolved, nil, func(candidates []indexResult) []indexResult {
		if postProcessErr != nil {
			return nil
		}
		// Candidate discovery activates a pending PQ model after collection
		// growth. Score supplements afterwards, once, using that same policy.
		if !supplementReady && spec.Property != "" {
			supplement, postProcessErr = s.propertyVectorNodeScores(ctx, normalizedQuery, spec.Label, spec.Property, quantizedIndex)
			if postProcessErr != nil {
				return nil
			}
			supplementReady = true
		}
		processed, processErr := s.scoreVectorQueryCandidates(ctx, normalizedQuery, spec, vectorName, candidates, supplement, quantizedIndex)
		postProcessErr = processErr
		// Filtering here, inside the widening loop, lets a selective Where
		// widen the search until it has the top K it admits.
		return s.filterByTypeAndProperties(ctx, processed, resolved, seenOrphans)
	})
	if err != nil {
		if errors.Is(err, ErrDimensionMismatch) {
			return nil, nil
		}
		return nil, err
	}
	if postProcessErr != nil {
		return nil, postProcessErr
	}

	hits := make([]VectorQueryHit, 0, min(len(results), spec.Limit))
	for _, result := range results {
		hits = append(hits, VectorQueryHit{ID: result.ID, Score: result.Score})
		if len(hits) >= spec.Limit {
			break
		}
	}
	return hits, nil
}

func (s *Service) scoreVectorQueryCandidates(ctx context.Context, normalizedQuery []float32, spec VectorQuerySpec, vectorName string, candidates []indexResult, supplement []vectorQueryScoredNode, quantizedIndex *HNSWIndex) ([]indexResult, error) {
	candidateNodes := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidateNodes[normalizeVectorResultIDToNodeID(candidate.ID)] = struct{}{}
	}

	out := make([]vectorQueryScoredNode, 0, min(len(candidateNodes), util.SafePreallocProduct(spec.Limit, 2)))
	meta := s.snapshotVectorQueryCandidateMeta(candidateNodes)
	scoreVector, release := s.vectorQueryScoreReader(normalizedQuery, quantizedIndex)
	defer release()

	for nodeID := range candidateNodes {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		nodeMeta, ok := meta[nodeID]
		if !ok {
			continue
		}

		if spec.Label != "" {
			has := false
			for _, l := range nodeMeta.labels {
				if l == spec.Label {
					has = true
					break
				}
			}
			if !has {
				continue
			}
		}

		// Apply Cypher precedence for selecting which vector represents this node.
		bestScore := -2.0

		if named := nodeMeta.named; named != nil {
			if vecID, ok := named[vectorName]; ok {
				if score, ok := scoreVector(vecID); ok {
					bestScore = score
				}
			}
			// If there is no explicit Cypher property/index mapping (property key is empty),
			// fall through to managed named embeddings (any key) before chunk fallback.
			if bestScore < -1.0 && spec.Property == "" {
				for _, vecID := range named {
					if score, ok := scoreVector(vecID); ok {
						if score > bestScore {
							bestScore = score
						}
					}
				}
			}
		}

		if bestScore < -1.0 && spec.Property != "" {
			if props := nodeMeta.props; props != nil {
				if vecID, ok := props[spec.Property]; ok {
					if score, ok := scoreVector(vecID); ok {
						bestScore = score
					}
				}
			}
		}

		if bestScore < -1.0 {
			for _, vecID := range nodeMeta.chunks {
				if score, ok := scoreVector(vecID); ok {
					if score > bestScore {
						bestScore = score
					}
				}
			}
		}

		if bestScore >= -1.0 {
			out = append(out, vectorQueryScoredNode{id: nodeID, score: bestScore})
		}
	}

	if spec.Property != "" {
		out = append(out, supplement...)
	}
	out = collapseScoredNodesByID(out)
	sort.Slice(out, func(i, j int) bool { return out[i].score > out[j].score })
	results := make([]indexResult, 0, len(out))
	for _, result := range out {
		results = append(results, indexResult{ID: result.id, Score: result.score})
	}
	return results, nil
}

type vectorQueryScoredNode struct {
	id    string
	score float64
}

func collapseScoredNodesByID(nodes []vectorQueryScoredNode) []vectorQueryScoredNode {
	if len(nodes) == 0 {
		return nil
	}
	best := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		if prev, ok := best[node.id]; !ok || node.score > prev {
			best[node.id] = node.score
		}
	}
	out := make([]vectorQueryScoredNode, 0, len(best))
	for id, score := range best {
		out = append(out, vectorQueryScoredNode{id: id, score: score})
	}
	return out
}

func (s *Service) propertyVectorNodeScores(ctx context.Context, normalizedQuery []float32, label, property string, quantizedIndex *HNSWIndex) ([]vectorQueryScoredNode, error) {
	type candidate struct {
		nodeID string
		vecID  string
	}
	candidates := make([]candidate, 0, 64)
	// The node and relationship metadata is written under indexMu (index
	// writes): lock order indexMu -> mu.
	s.indexMu.RLock()
	s.mu.RLock()
	for nodeID, props := range s.nodePropVector {
		vecID, ok := props[property]
		if !ok {
			continue
		}
		if label != "" {
			hasLabel := false
			for _, nodeLabel := range s.nodeLabels[nodeID] {
				if nodeLabel == label {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}
		candidates = append(candidates, candidate{nodeID: nodeID, vecID: vecID})
	}
	s.mu.RUnlock()
	s.indexMu.RUnlock()

	if len(candidates) == 0 {
		return nil, nil
	}
	scoreVector, release := s.vectorQueryScoreReader(normalizedQuery, quantizedIndex)
	defer release()
	out := make([]vectorQueryScoredNode, 0, len(candidates))
	for _, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		score, ok := scoreVector(cand.vecID)
		if !ok {
			continue
		}
		out = append(out, vectorQueryScoredNode{id: cand.nodeID, score: score})
	}
	return out, nil
}

type vectorQueryCandidateMeta struct {
	labels []string
	named  map[string]string
	props  map[string]string
	chunks []string
}

func (s *Service) snapshotVectorQueryCandidateMeta(candidateNodes map[string]struct{}) map[string]vectorQueryCandidateMeta {
	meta := make(map[string]vectorQueryCandidateMeta, len(candidateNodes))
	// The node and relationship metadata is written under indexMu (index
	// writes): lock order indexMu -> mu.
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for nodeID := range candidateNodes {
		labels := append([]string(nil), s.nodeLabels[nodeID]...)
		named := cloneStringMap(s.nodeNamedVector[nodeID])
		props := cloneStringMap(s.nodePropVector[nodeID])
		chunks := append([]string(nil), s.nodeChunkVectors[nodeID]...)
		meta[nodeID] = vectorQueryCandidateMeta{
			labels: labels,
			named:  named,
			props:  props,
			chunks: chunks,
		}
	}
	return meta
}

func (s *Service) scoreVectorIDDot(normalizedQuery []float32, vecID string) (float64, bool) {
	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	s.mu.RUnlock()

	var vec []float32
	if vfs != nil {
		if stored, ok := vfs.GetVector(vecID); ok {
			vec = stored
		}
	}
	if len(vec) == 0 && vi != nil {
		vi.mu.RLock()
		stored, ok := vi.vectors[vecID]
		if ok {
			vec = stored
		}
		vi.mu.RUnlock()
	}
	if len(vec) == 0 {
		return 0, false
	}

	return clampCosine(float64(vector.DotProductSIMD(normalizedQuery, vec))), true
}

// clampCosine keeps a cosine score within [-1, 1]. Float32 accumulation can
// drift slightly outside those bounds; clamping preserves Cypher cosine
// semantics and avoids dropping boundary hits (for example, opposite vectors
// drifting to <-1 on ASC fast-path queries). NaN stays NaN.
func clampCosine(score float64) float64 {
	return math.Max(-1, math.Min(1, score))
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func (s *Service) vectorQueryNodesExact(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, vectorName string, similarity string, opts *SearchOptions) ([]VectorQueryHit, error) {
	if s.EmbeddingCount() == 0 {
		return nil, nil
	}
	if dims := s.VectorIndexDimensions(); dims > 0 && len(queryEmbedding) != dims {
		return nil, nil
	}
	prepared := newCypherVectorQuery(similarity, queryEmbedding)
	prepared.scale = spec.ScoreScale

	// scoreAll scores every node under the metadata read locks; the Where
	// filter below reads storage, so it runs after they are released.
	scoreAll := func() ([]vectorQueryScoredNode, error) {
		scored := make([]vectorQueryScoredNode, 0, util.SafePreallocProduct(spec.Limit, 2))

		// The node and relationship metadata is written under indexMu (index
		// writes): lock order indexMu -> mu.
		s.indexMu.RLock()
		defer s.indexMu.RUnlock()
		s.mu.RLock()
		defer s.mu.RUnlock()

		for nodeID, labels := range s.nodeLabels {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			if spec.Label != "" {
				has := false
				for _, l := range labels {
					if l == spec.Label {
						has = true
						break
					}
				}
				if !has {
					continue
				}
			}

			bestScore := math.Inf(-1)

			// Apply Cypher precedence for selecting which vector represents this node.
			if named := s.nodeNamedVector[nodeID]; named != nil {
				if vecID, ok := named[vectorName]; ok {
					if v, ok := s.getVectorForCypher(vecID); ok {
						bestScore = prepared.score(v)
					}
				}
				// If there is no explicit Cypher property/index mapping (property key is empty),
				// fall through to managed named embeddings (any key) before chunk fallback.
				if math.IsInf(bestScore, -1) && spec.Property == "" {
					for _, vecID := range named {
						if v, ok := s.getVectorForCypher(vecID); ok {
							score := prepared.score(v)
							if score > bestScore {
								bestScore = score
							}
						}
					}
				}
			}

			if math.IsInf(bestScore, -1) && spec.Property != "" {
				if props := s.nodePropVector[nodeID]; props != nil {
					if vecID, ok := props[spec.Property]; ok {
						if v, ok := s.getVectorForCypher(vecID); ok {
							bestScore = prepared.score(v)
						}
					}
				}
			}

			if math.IsInf(bestScore, -1) {
				for _, vecID := range s.nodeChunkVectors[nodeID] {
					if v, ok := s.getVectorForCypher(vecID); ok {
						score := prepared.score(v)
						if score > bestScore {
							bestScore = score
						}
					}
				}
			}

			if !math.IsInf(bestScore, -1) {
				scored = append(scored, vectorQueryScoredNode{id: nodeID, score: bestScore})
			}
		}

		return scored, nil
	}
	s.mu.RLock()
	fileBacked := s.vectorFileStore != nil
	s.mu.RUnlock()
	var scored []vectorQueryScoredNode
	var err error
	if fileBacked {
		scored, err = s.exactStoredNodeScores(ctx, prepared, spec, vectorName)
	} else {
		scored, err = scoreAll()
	}
	if err != nil {
		return nil, err
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if opts != nil && opts.Where != nil {
		admitted := scored[:0]
		for _, candidate := range scored {
			if len(admitted) == spec.Limit {
				break
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			node, err := s.getNodeWithoutEmbeddings(storage.NodeID(candidate.id))
			if err != nil || !opts.Where(node) {
				continue
			}
			admitted = append(admitted, candidate)
		}
		scored = admitted
	}
	if len(scored) > spec.Limit {
		scored = scored[:spec.Limit]
	}

	out := make([]VectorQueryHit, 0, len(scored))
	for _, s := range scored {
		out = append(out, VectorQueryHit{ID: s.id, Score: s.score})
	}
	return out, nil
}

// exactStoredNodeScores uses the indexed population to resolve original node
// vectors from storage. The file store retains normalized coordinates only;
// reading the durable values preserves magnitudes without a second vector copy.
func (s *Service) exactStoredNodeScores(ctx context.Context, prepared cypherVectorQuery, spec VectorQuerySpec, vectorName string) ([]vectorQueryScoredNode, error) {
	s.indexMu.RLock()
	s.mu.RLock()
	// Snapshot admitted source fields, not just labels: excluded managed chunks
	// still have text/labels but must never re-enter exact search via storage.
	metadata := make(map[string]vectorQueryCandidateMeta, len(s.nodeLabels))
	for id, labels := range s.nodeLabels {
		matches := spec.Label == ""
		for _, label := range labels {
			matches = matches || label == spec.Label
		}
		if matches {
			metadata[id] = vectorQueryCandidateMeta{
				named:  cloneStringMap(s.nodeNamedVector[id]),
				props:  cloneStringMap(s.nodePropVector[id]),
				chunks: append([]string(nil), s.nodeChunkVectors[id]...),
			}
		}
	}
	s.mu.RUnlock()
	s.indexMu.RUnlock()
	out := make([]vectorQueryScoredNode, 0, min(len(metadata), spec.Limit))
	for id, admitted := range metadata {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node, err := s.engine.GetNode(storage.NodeID(id))
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// Restrict the durable node to the identities admitted at indexing time.
		eligible := *node
		eligible.NamedEmbeddings = make(map[string][]float32, len(admitted.named))
		for name := range admitted.named {
			eligible.NamedEmbeddings[name] = node.NamedEmbeddings[name]
		}
		eligible.Properties = make(map[string]any, len(admitted.props))
		for name := range admitted.props {
			eligible.Properties[name] = node.Properties[name]
		}
		eligible.ChunkEmbeddings = nil
		for _, chunkID := range admitted.chunks {
			chunk := 0
			if chunkID != id {
				var err error
				chunk, err = strconv.Atoi(strings.TrimPrefix(chunkID, id+"-chunk-"))
				if err != nil {
					continue
				}
			}
			if chunk >= 0 && chunk < len(node.ChunkEmbeddings) {
				eligible.ChunkEmbeddings = append(eligible.ChunkEmbeddings, node.ChunkEmbeddings[chunk])
			}
		}
		best := math.Inf(-1)
		for _, value := range resolveCypherCandidateEmbeddings(&eligible, spec.Property, vectorName) {
			best = math.Max(best, prepared.score(value))
		}
		if !math.IsInf(best, -1) {
			out = append(out, vectorQueryScoredNode{id: id, score: best})
		}
	}
	return out, nil
}

// cypherVectorQuery is a Cypher vector query's query vector, prepared once
// for its similarity: cosine needs it L2-normalized (vector.Neo4jCosineQuery).
// valid is false for a query that isn't a valid vector for the similarity
// (a zero vector for cosine): it has no hits.
type cypherVectorQuery struct {
	similarity string
	query      []float32
	cosine     vector.Neo4jCosineQuery
	valid      bool
	// scale is the scale score reports on; the zero value is Neo4j's.
	scale VectorScoreScale
}

func newCypherVectorQuery(similarity string, query []float32) cypherVectorQuery {
	prepared := cypherVectorQuery{similarity: similarity, query: query, valid: true}
	if similarity != "euclidean" && similarity != "dot" {
		prepared.cosine, prepared.valid = vector.NewNeo4jCosineQuery(query)
	}
	return prepared
}

// score is candidate's score on q.scale (see VectorScoreScale); a candidate
// that isn't a valid vector for the similarity scores -Inf, no hit.
func (q cypherVectorQuery) score(candidate []float32) float64 {
	var score float64
	ok := q.valid
	switch {
	case !ok:
	case q.similarity == "euclidean" && q.scale == VectorScoreSimilarity:
		ok = len(candidate) == len(q.query)
		score = vector.EuclideanSimilarity(q.query, candidate)
	case q.similarity == "euclidean":
		score, ok = vector.Neo4jEuclideanSimilarity(q.query, candidate)
	case q.similarity == "dot":
		return float64(vector.DotProduct(q.query, candidate))
	case q.scale == VectorScoreSimilarity:
		ok = len(candidate) == len(q.query)
		score = vector.CosineSimilarity(q.query, candidate)
	default:
		score, ok = q.cosine.Similarity(candidate)
	}
	if !ok {
		return math.Inf(-1)
	}
	return score
}

// rank orders candidates as score does, cheaply: for cosine, the dot
// product over the candidate's L2 norm in one float64 pass (the query's norm
// is the same for every candidate), where the exact score takes two strictly
// ordered float32 passes; an invalid candidate ranks -Inf. Near-ties may
// order otherwise than the exact scores, which are recomputed for the hits
// returned.
func (q cypherVectorQuery) rank(candidate []float32) float64 {
	if !q.valid || q.similarity == "euclidean" || q.similarity == "dot" {
		return q.score(candidate)
	}
	var dot, norm float64
	for i, y := range candidate {
		dot += float64(q.query[i]) * float64(y)
		norm += float64(y) * float64(y)
	}
	if !(norm > 0) || math.IsInf(norm, 0) {
		return math.Inf(-1)
	}
	return dot / math.Sqrt(norm)
}

// neo4jCosineScore maps a cosine in [-1, 1] from the vector index to Neo4j's
// cosine score, max((1 + cos) / 2, 0), in float32 as Neo4j's index scores
// are.
func neo4jCosineScore(cosine float64) float64 {
	return float64(float32(math.Max((1+cosine)/2, 0)))
}

func resolveCypherCandidateEmbeddings(node *storage.Node, propertyKey string, vectorName string) [][]float32 {
	if node == nil {
		return nil
	}

	if node.NamedEmbeddings != nil {
		if emb, ok := node.NamedEmbeddings[vectorName]; ok && len(emb) > 0 {
			return [][]float32{emb}
		}
		// When Cypher does not bind to a specific property/index (propertyKey empty),
		// include all managed named embeddings as candidates.
		if propertyKey == "" {
			all := make([][]float32, 0, len(node.NamedEmbeddings))
			for _, emb := range node.NamedEmbeddings {
				if len(emb) > 0 {
					all = append(all, emb)
				}
			}
			if len(all) > 0 {
				return all
			}
		}
	}

	if propertyKey != "" && node.Properties != nil {
		if emb, ok := node.Properties[propertyKey]; ok {
			if vec := toFloat32SliceAny(emb); len(vec) > 0 {
				return [][]float32{vec}
			}
		}
	}

	if len(node.ChunkEmbeddings) > 0 {
		return node.ChunkEmbeddings
	}

	return nil
}

func toFloat32SliceAny(v any) []float32 {
	switch x := v.(type) {
	case []float32:
		out := make([]float32, len(x))
		copy(out, x)
		return out
	case []float64:
		out := make([]float32, len(x))
		for i, f := range x {
			out[i] = float32(f)
		}
		return out
	case []any:
		out := make([]float32, 0, len(x))
		for _, item := range x {
			switch t := item.(type) {
			case float32:
				out = append(out, t)
			case float64:
				out = append(out, float32(t))
			case int:
				out = append(out, float32(t))
			case int64:
				out = append(out, float32(t))
			default:
				return nil
			}
		}
		return out
	default:
		return nil
	}
}
