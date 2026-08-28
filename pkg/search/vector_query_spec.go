package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

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
}

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
// vector index (unified pipeline) rather than scanning storage.
func (s *Service) VectorQueryNodes(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec) ([]VectorQueryHit, error) {
	if s == nil || s.engine == nil {
		return nil, fmt.Errorf("search service unavailable")
	}
	if len(queryEmbedding) == 0 {
		return nil, fmt.Errorf("query embedding required")
	}
	if ctx == nil {
		ctx = context.Background()
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

	// If the query dimensions don't match the configured index dimensions, Cypher expects
	// an empty result set (not an error). The active vector backing may be either the
	// in-memory VectorIndex or the file-backed VectorFileStore.
	if dims := s.VectorIndexDimensions(); dims > 0 && s.EmbeddingCount() > 0 && len(queryEmbedding) != dims {
		return nil, nil
	}

	// Fast path: cosine queries can use the indexed vector pipeline.
	if strings.EqualFold(similarity, "cosine") {
		return s.vectorQueryNodesIndexed(ctx, queryEmbedding, spec, vectorName)
	}

	// Exact (index-backed) path for dot/euclidean: compute per-node scores from in-memory vectors
	// without scanning storage.
	return s.vectorQueryNodesExact(ctx, queryEmbedding, spec, vectorName, strings.ToLower(similarity))
}

// VectorQueryRelationships executes a Cypher-style relationship vector query using
// in-memory relationship-vector metadata. This avoids scanning storage.AllEdges()
// on every fact-search call.
func (s *Service) VectorQueryRelationships(ctx context.Context, queryEmbedding []float32, spec RelationshipVectorQuerySpec) ([]RelationshipVectorQueryHit, error) {
	if s == nil || s.engine == nil {
		return nil, fmt.Errorf("search service unavailable")
	}
	if len(queryEmbedding) == 0 {
		return nil, fmt.Errorf("query embedding required")
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
	if len(candidates) == 0 {
		return nil, nil
	}

	type scoredEdge struct {
		id    string
		score float64
	}
	scored := make([]scoredEdge, 0, len(candidates))
	for _, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var score float64
		switch similarity {
		case "cosine":
			score = vector.CosineSimilarity(queryEmbedding, cand.vec)
			if score > 1.0 {
				score = 1.0
			} else if score < -1.0 {
				score = -1.0
			}
		default:
			score = cypherVectorSimilarity(similarity, queryEmbedding, cand.vec)
		}
		scored = append(scored, scoredEdge{id: cand.id, score: score})
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if len(scored) > spec.Limit {
		scored = scored[:spec.Limit]
	}
	out := make([]RelationshipVectorQueryHit, 0, len(scored))
	for _, r := range scored {
		out = append(out, RelationshipVectorQueryHit{ID: r.id, Score: r.score})
	}
	return out, nil
}

func (s *Service) vectorQueryNodesIndexed(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, vectorName string) ([]VectorQueryHit, error) {
	opts := DefaultSearchOptions()
	opts.Limit = spec.Limit
	opts.CandidateTarget = spec.Limit
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
	minSimilarity := -1.0
	resolved.MinSimilarity = &minSimilarity
	normalizedQuery := vector.Normalize(queryEmbedding)

	var supplement []vectorQueryScoredNode
	if spec.Property != "" {
		supplement, err = s.exactPropertyVectorNodeScores(ctx, normalizedQuery, spec.Label, spec.Property)
		if err != nil {
			return nil, err
		}
	}

	var postProcessErr error
	results, _, err := s.adaptiveVectorSearch(ctx, pipeline, queryEmbedding, resolved, func(candidates []indexResult) []indexResult {
		if postProcessErr != nil {
			return nil
		}
		processed, processErr := s.scoreVectorQueryCandidates(ctx, normalizedQuery, spec, vectorName, candidates, supplement)
		postProcessErr = processErr
		return processed
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

func (s *Service) scoreVectorQueryCandidates(ctx context.Context, normalizedQuery []float32, spec VectorQuerySpec, vectorName string, candidates []indexResult, supplement []vectorQueryScoredNode) ([]indexResult, error) {
	candidateNodes := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidateNodes[normalizeVectorResultIDToNodeID(candidate.ID)] = struct{}{}
	}

	out := make([]vectorQueryScoredNode, 0, min(len(candidateNodes), util.SafePreallocProduct(spec.Limit, 2)))
	meta := s.snapshotVectorQueryCandidateMeta(candidateNodes)

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
				if score, ok := s.scoreVectorIDDot(normalizedQuery, vecID); ok {
					bestScore = score
				}
			}
			// If there is no explicit Cypher property/index mapping (property key is empty),
			// fall through to managed named embeddings (any key) before chunk fallback.
			if bestScore < -1.0 && spec.Property == "" {
				for _, vecID := range named {
					if score, ok := s.scoreVectorIDDot(normalizedQuery, vecID); ok {
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
					if score, ok := s.scoreVectorIDDot(normalizedQuery, vecID); ok {
						bestScore = score
					}
				}
			}
		}

		if bestScore < -1.0 {
			for _, vecID := range nodeMeta.chunks {
				if score, ok := s.scoreVectorIDDot(normalizedQuery, vecID); ok {
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

func (s *Service) exactPropertyVectorNodeScores(ctx context.Context, normalizedQuery []float32, label, property string) ([]vectorQueryScoredNode, error) {
	type candidate struct {
		nodeID string
		vecID  string
	}
	candidates := make([]candidate, 0, 64)
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

	out := make([]vectorQueryScoredNode, 0, len(candidates))
	for _, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		score, ok := s.scoreVectorIDDot(normalizedQuery, cand.vecID)
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

	score := float64(vector.DotProductSIMD(normalizedQuery, vec))
	// SIMD float32 accumulation can drift slightly outside cosine bounds.
	// Clamp to preserve Cypher cosine semantics and avoid dropping boundary hits
	// (for example, opposite vectors drifting to <-1 on ASC fast-path queries).
	if score > 1.0 {
		score = 1.0
	} else if score < -1.0 {
		score = -1.0
	}
	return score, true
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

func (s *Service) vectorQueryNodesExact(ctx context.Context, queryEmbedding []float32, spec VectorQuerySpec, vectorName string, similarity string) ([]VectorQueryHit, error) {
	if s.EmbeddingCount() == 0 {
		return nil, nil
	}
	if dims := s.VectorIndexDimensions(); dims > 0 && len(queryEmbedding) != dims {
		return nil, nil
	}

	type scoredNode struct {
		id    string
		score float64
	}
	scored := make([]scoredNode, 0, util.SafePreallocProduct(spec.Limit, 2))

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
					bestScore = cypherVectorSimilarity(similarity, queryEmbedding, v)
				}
			}
			// If there is no explicit Cypher property/index mapping (property key is empty),
			// fall through to managed named embeddings (any key) before chunk fallback.
			if math.IsInf(bestScore, -1) && spec.Property == "" {
				for _, vecID := range named {
					if v, ok := s.getVectorForCypher(vecID); ok {
						score := cypherVectorSimilarity(similarity, queryEmbedding, v)
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
						bestScore = cypherVectorSimilarity(similarity, queryEmbedding, v)
					}
				}
			}
		}

		if math.IsInf(bestScore, -1) {
			for _, vecID := range s.nodeChunkVectors[nodeID] {
				if v, ok := s.getVectorForCypher(vecID); ok {
					score := cypherVectorSimilarity(similarity, queryEmbedding, v)
					if score > bestScore {
						bestScore = score
					}
				}
			}
		}

		if !math.IsInf(bestScore, -1) {
			scored = append(scored, scoredNode{id: nodeID, score: bestScore})
		}
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if len(scored) > spec.Limit {
		scored = scored[:spec.Limit]
	}

	out := make([]VectorQueryHit, 0, len(scored))
	for _, s := range scored {
		out = append(out, VectorQueryHit{ID: s.id, Score: s.score})
	}
	return out, nil
}

func cypherVectorSimilarity(similarity string, query []float32, candidate []float32) float64 {
	switch similarity {
	case "euclidean":
		return vector.EuclideanSimilarity(query, candidate)
	case "dot":
		return float64(vector.DotProduct(query, candidate))
	default:
		return vector.CosineSimilarity(query, candidate)
	}
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
