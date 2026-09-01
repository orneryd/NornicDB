package search

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

type queryTextContextKey struct{}

func withQueryText(ctx context.Context, query string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, queryTextContextKey{}, query)
}

func queryTextFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(queryTextContextKey{}).(string)
	return strings.TrimSpace(v)
}

func (s *Service) clearClusterLexicalProfiles() {
	s.clusterLexicalMu.Lock()
	s.clusterLexicalProfiles = make(map[int]map[string]float64)
	s.clusterLexicalMu.Unlock()
}

func (s *Service) rebuildClusterLexicalProfiles() {
	s.mu.RLock()
	clusterIndex := s.clusterIndex
	fulltext := s.fulltextIndex
	s.mu.RUnlock()
	if clusterIndex == nil || !clusterIndex.IsClustered() || fulltext == nil {
		s.clearClusterLexicalProfiles()
		return
	}

	numClusters := clusterIndex.NumClusters()
	if numClusters <= 0 {
		s.clearClusterLexicalProfiles()
		return
	}

	topTerms := envutil.GetInt("NORNICDB_VECTOR_HYBRID_ROUTING_LEX_TOP_TERMS", 64)
	if topTerms < 8 {
		topTerms = 8
	}

	profiles := make(map[int]map[string]float64, numClusters)
	for cid := 0; cid < numClusters; cid++ {
		memberIDs := clusterIndex.GetClusterMemberIDsForCluster(cid)
		if len(memberIDs) == 0 {
			continue
		}
		tf := make(map[string]float64, 128)
		total := 0.0
		for _, id := range memberIDs {
			text, ok := fulltext.GetDocument(id)
			if !ok || text == "" {
				continue
			}
			for _, tok := range tokenize(text) {
				tf[tok]++
				total++
			}
		}
		if total <= 0 {
			continue
		}
		for tok, v := range tf {
			tf[tok] = v / total
		}
		profiles[cid] = topNTokenWeights(tf, topTerms)
	}

	s.clusterLexicalMu.Lock()
	s.clusterLexicalProfiles = profiles
	s.clusterLexicalMu.Unlock()
	logSearchPrintf("🔍 Hybrid routing lexical profiles built for %d clusters", len(profiles))
}

func topNTokenWeights(weights map[string]float64, n int) map[string]float64 {
	if len(weights) <= n {
		return weights
	}
	type kv struct {
		k string
		v float64
	}
	items := make([]kv, 0, len(weights))
	for k, v := range weights {
		items = append(items, kv{k: k, v: v})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
	out := make(map[string]float64, n)
	for i := 0; i < n && i < len(items); i++ {
		out[items[i].k] = items[i].v
	}
	return out
}

func (s *Service) selectHybridClusters(ctx context.Context, query []float32, defaultN int) []int {
	s.mu.RLock()
	clusterIndex := s.clusterIndex
	s.mu.RUnlock()
	if clusterIndex == nil || !clusterIndex.IsClustered() {
		return nil
	}
	if defaultN <= 0 {
		defaultN = 3
	}
	numClusters := clusterIndex.NumClusters()
	if numClusters <= 0 {
		return nil
	}
	semanticProbe := defaultN * 2
	if semanticProbe > numClusters {
		semanticProbe = numClusters
	}
	if semanticProbe <= 0 {
		return nil
	}
	semanticClusters := clusterIndex.FindNearestClusters(query, semanticProbe)
	if len(semanticClusters) == 0 {
		return nil
	}

	queryText := queryTextFromContext(ctx)
	if queryText == "" {
		if len(semanticClusters) > defaultN {
			return semanticClusters[:defaultN]
		}
		return semanticClusters
	}
	queryTokens := tokenize(queryText)
	if len(queryTokens) == 0 {
		if len(semanticClusters) > defaultN {
			return semanticClusters[:defaultN]
		}
		return semanticClusters
	}

	s.clusterLexicalMu.RLock()
	profiles := s.clusterLexicalProfiles
	s.clusterLexicalMu.RUnlock()
	if len(profiles) == 0 {
		if len(semanticClusters) > defaultN {
			return semanticClusters[:defaultN]
		}
		return semanticClusters
	}

	type scored struct {
		id    int
		score float64
	}
	semanticRank := make(map[int]int, len(semanticClusters))
	for i, cid := range semanticClusters {
		semanticRank[cid] = i
	}

	lexical := make([]scored, 0, len(profiles))
	for cid, profile := range profiles {
		score := 0.0
		for _, tok := range queryTokens {
			score += profile[tok]
		}
		if score > 0 {
			lexical = append(lexical, scored{id: cid, score: score})
		}
	}
	sort.Slice(lexical, func(i, j int) bool { return lexical[i].score > lexical[j].score })
	lexicalRank := make(map[int]int, len(lexical))
	for i, item := range lexical {
		lexicalRank[item.id] = i
	}

	wSem := envFloat("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", 0.7)
	wLex := envFloat("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", 0.3)
	if wSem < 0 {
		wSem = 0
	}
	if wLex < 0 {
		wLex = 0
	}
	if wSem+wLex == 0 {
		wSem = 0.7
		wLex = 0.3
	}
	sumW := wSem + wLex
	wSem /= sumW
	wLex /= sumW

	// Fuse rank-based scores to keep lexical/semantic scales comparable.
	const rrfK = 60.0
	union := make(map[int]struct{}, len(semanticRank)+len(lexicalRank))
	for cid := range semanticRank {
		union[cid] = struct{}{}
	}
	for cid := range lexicalRank {
		union[cid] = struct{}{}
	}
	fused := make([]scored, 0, len(union))
	for cid := range union {
		score := 0.0
		if r, ok := semanticRank[cid]; ok {
			score += wSem * (1.0 / (rrfK + float64(r+1)))
		}
		if r, ok := lexicalRank[cid]; ok {
			score += wLex * (1.0 / (rrfK + float64(r+1)))
		}
		fused = append(fused, scored{id: cid, score: score})
	}
	sort.Slice(fused, func(i, j int) bool { return fused[i].score > fused[j].score })

	outN := defaultN
	if outN > len(fused) {
		outN = len(fused)
	}
	if outN <= 0 {
		return nil
	}
	out := make([]int, 0, outN)
	for i := 0; i < outN; i++ {
		out = append(out, fused[i].id)
	}
	return out
}

func (s *Service) applyBM25SeedHints() {
	s.mu.RLock()
	clusterIndex := s.clusterIndex
	fulltext := s.fulltextIndex
	s.mu.RUnlock()
	if clusterIndex == nil || fulltext == nil {
		return
	}
	if !clusterIndex.IsClustered() && clusterIndex.Count() == 0 {
		return
	}
	ids := bm25SeedDocIDs(fulltext)
	if len(ids) == 0 {
		return
	}
	indices := clusterIndex.GetIndicesForNodeIDs(ids)
	if len(indices) == 0 {
		return
	}
	clusterIndex.SetPreferredSeedIndices(indices)
	logSearchPrintf("[K-MEANS] 🔍 Applied BM25 seed hints: %d preferred seeds", len(indices))
}
