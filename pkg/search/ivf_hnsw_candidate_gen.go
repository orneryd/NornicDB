package search

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type clusterHNSWLookup func(clusterID int) *HNSWIndex

// clusterView is the cluster generation one call routes on: its routing
// (centroids and membership), its population, the clusters a query probes
// and its graphs.
type clusterView struct {
	routing *gpu.Clustering
	index   *gpu.ClusterIndex
	probes  int
	graph   clusterHNSWLookup
}

// clusterServing is where a clustered candidate generator reads the cluster
// generation a call uses: view returns the one the query took at its start
// (and the call's hold on it, ended by done), so every call of a query,
// adaptive overfetch rounds included, routes on that generation and never on
// a copy a pipeline took earlier. The selector reads the generation's
// lexical profiles with lock held (the lock publication takes exclusively).
type clusterServing struct {
	lock sync.Locker
	view func(ctx context.Context) (context.Context, clusterView, func())
}

// open returns the view for one call, starting from the generator's own
// when no serving source is set. unlock ends the routing section; done the
// call's hold on the generation. Both run at most once, so a caller defers
// them right after open (a panic in routing, for example in the configured
// analyzer, still unlocks and releases) and may also unlock early, before
// scoring.
func (c *clusterServing) open(ctx context.Context, own clusterView) (context.Context, clusterView, func(), func()) {
	if c == nil {
		return ctx, own, func() {}, func() {}
	}
	ctx, view, done := c.view(ctx)
	c.lock.Lock()
	var once sync.Once
	return ctx, view, func() { once.Do(c.lock.Unlock) }, done
}

// population is the vector count of the cluster generation ctx's query
// serves from (own's when no serving source is set): the population a
// clustered generator searches when it routes every cluster.
func (c *clusterServing) population(ctx context.Context, own *gpu.ClusterIndex) int {
	if c == nil {
		if own == nil {
			return 0
		}
		return own.Count()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, view, done := c.view(ctx)
	defer done()
	if view.index == nil {
		return 0
	}
	return view.index.Count()
}

// ownClusterView is a generator's view without a serving source: the
// index's published routing.
func ownClusterView(index *gpu.ClusterIndex, probes int, graph clusterHNSWLookup) clusterView {
	view := clusterView{index: index, probes: probes, graph: graph}
	if index != nil {
		view.routing = index.PublishedClustering()
	}
	return view
}

// routeClusters is the clusters a clustered generator searches for a request
// of limit candidates: every populated cluster when the request is as wide as
// the index's population, so the search can be complete (exhaustion requires
// every cluster); otherwise the selector's choice, or the view.probes nearest.
// The caller holds the view's routing section.
func routeClusters(ctx context.Context, query []float32, view clusterView, selector func(context.Context, []float32, int) []int, limit int) []int {
	if view.index != nil && limit >= view.index.Count() {
		return view.routing.PopulatedClusters()
	}
	var clusterIDs []int
	if selector != nil {
		clusterIDs = selector(ctx, query, view.probes)
	}
	if len(clusterIDs) == 0 {
		clusterIDs = view.routing.NearestClusters(query, view.probes)
	}
	return clusterIDs
}

// IVFHNSWCandidateGen implements IVF-HNSW: centroid routing (IVF) into per-cluster HNSW indexes.
//
// This is designed for large CPU-only datasets:
//   - K-means provides a coarse routing layer (choose nearest clusters)
//   - Per-cluster HNSW provides fast ANN within each cluster
//
// For GPU-enabled setups, prefer the existing GPU brute-force and GPU k-means paths.
type IVFHNSWCandidateGen struct {
	clusterIndex        *gpu.ClusterIndex
	getClusterHNSW      clusterHNSWLookup
	numClustersToSearch int
	clusterSelector     func(ctx context.Context, query []float32, defaultN int) []int
	// serving, when set, is held while routing and taking graphs, so both
	// come from one published cluster generation.
	serving *clusterServing
}

func NewIVFHNSWCandidateGen(clusterIndex *gpu.ClusterIndex, getClusterHNSW clusterHNSWLookup, numClustersToSearch int) *IVFHNSWCandidateGen {
	if numClustersToSearch <= 0 {
		numClustersToSearch = 3
	}
	return &IVFHNSWCandidateGen{
		clusterIndex:        clusterIndex,
		getClusterHNSW:      getClusterHNSW,
		numClustersToSearch: numClustersToSearch,
	}
}

// SetClusterSelector sets an optional custom cluster selector used for routing.
func (g *IVFHNSWCandidateGen) SetClusterSelector(fn func(ctx context.Context, query []float32, defaultN int) []int) *IVFHNSWCandidateGen {
	g.clusterSelector = fn
	return g
}

// SetServing makes the generator serve each query from the cluster
// generation the query holds (see clusterServing); the selector is then
// called with lock held and must not lock it.
func (g *IVFHNSWCandidateGen) SetServing(lock sync.Locker, view func(context.Context) (context.Context, clusterView, func())) *IVFHNSWCandidateGen {
	g.serving = &clusterServing{lock: lock, view: view}
	return g
}

func (g *IVFHNSWCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

// candidatePopulation is the number of vectors the clustered index holds.
func (g *IVFHNSWCandidateGen) candidatePopulation(ctx context.Context) int {
	if ctx == nil {
		ctx = context.Background()
	}
	_, view, unlock, done := g.serving.open(ctx, ownClusterView(g.clusterIndex, g.numClustersToSearch, g.getClusterHNSW))
	defer done()
	defer unlock()
	_, population := graphPopulation(view)
	return population
}

// graphPopulation is every populated cluster of view and the live vectors
// their graphs hold together: what IVF-HNSW searches when it routes every
// cluster. A populated cluster without a graph adds nothing here; routing to
// it fails (routedGraphs). The caller holds the view's routing section.
func graphPopulation(view clusterView) (clusters []int, population int) {
	if view.routing == nil || view.graph == nil {
		return nil, 0
	}
	clusters = view.routing.PopulatedClusters()
	for _, cluster := range clusters {
		if graph := view.graph(cluster); graph != nil {
			population += graph.Size()
		}
	}
	return clusters, population
}

func (g *IVFHNSWCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	candidates, evidence, err := g.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	return candidates, evidence.exhausted, err
}

// searchCandidatesWithEvidence searches the routed clusters' graphs. The pass
// is routing-bound when every graph search was itself exhausted and the limit
// cut none of the merged candidates: a wider limit searches the same graphs
// whole again. It is exhausted only when, besides, every cluster graph was
// routed (routedGraphs routes all of them for a request as wide as their
// population).
func (g *IVFHNSWCandidateGen) searchCandidatesWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, candidateEvidence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, view, unlock, done := g.serving.open(ctx, ownClusterView(g.clusterIndex, g.numClustersToSearch, g.getClusterHNSW))
	defer done()
	defer unlock()
	indexes, routedAll, err := g.routedGraphs(ctx, query, view, boundCandidateLimit(k))
	unlock()
	if err != nil {
		return nil, candidateEvidence{}, err
	}
	if len(indexes) == 0 {
		return []Candidate{}, candidateEvidence{exhausted: routedAll, routedComplete: true}, nil
	}

	// The pool kept for exact rescoring follows the single-graph rule,
	// oversampling included. Activation is per graph (pending PQ searches
	// originals until its graph is large enough), so any compressed graph
	// widens the pool, whatever the route order.
	quantized := false
	for _, index := range indexes {
		if index.quantizationActive(ctx) {
			quantized = true
			break
		}
	}
	totalLimit := quantizedCandidateLimit(ctx, k, quantized)
	results := make([][]ANNResult, len(indexes))
	exhausted := make([]bool, len(indexes))
	errors := make([]error, len(indexes))
	ef := hnswEfFromContext(ctx)
	workers := min(4, runtime.GOMAXPROCS(0), len(indexes))
	search := func(worker int) {
		for i := worker; i < len(indexes); i += workers {
			if err := ctx.Err(); err != nil {
				errors[i] = err
				return
			}
			// Any list may contain the entire global top-K: retain its full budget,
			// with a beam wide enough to supply it. A beam as wide as the
			// graph's population scores all of it (searchWithEfExhaustion).
			results[i], exhausted[i], errors[i] = indexes[i].searchWithEfExhaustion(ctx, query, totalLimit, minSimilarity, ivfSearchBeam(indexes[i], ef, totalLimit))
		}
	}
	if workers == 1 {
		search(0)
	} else {
		var pending sync.WaitGroup
		pending.Add(workers)
		for w := 0; w < workers; w++ {
			go func(worker int) { defer pending.Done(); search(worker) }(w)
		}
		pending.Wait()
	}
	// Merge in route order after all workers finish, preserving error and score policy.
	routed := true
	best := make(map[string]float64, totalLimit)
	for i, hits := range results {
		if errors[i] != nil {
			return nil, candidateEvidence{}, errors[i]
		}
		routed = routed && exhausted[i]
		for _, r := range hits {
			score := float64(r.Score)
			if prev, ok := best[r.ID]; !ok || score > prev {
				best[r.ID] = score
			}
		}
	}

	out := make([]Candidate, 0, len(best))
	for id, score := range best {
		out = append(out, Candidate{ID: id, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > totalLimit {
		out = out[:totalLimit]
		routed = false
	}
	return out, candidateEvidence{exhausted: routedAll && routed, routedComplete: routed}, nil
}

// routedGraphs routes query on view's generation and takes the routed
// clusters' graphs from the same generation: every cluster that has a graph
// when limit covers their population (routedAll), else the selector's choice
// or the view.probes nearest. Missing graphs are incomplete ANN state, not
// permission to scan the entire routed population.
func (g *IVFHNSWCandidateGen) routedGraphs(ctx context.Context, query []float32, view clusterView, limit int) ([]*HNSWIndex, bool, error) {
	if view.routing == nil {
		return nil, false, localizedError(localization.SearchClusterIndexNotClustered(), nil)
	}
	if view.graph == nil {
		return nil, false, localizedError(localization.SearchClusterHNSWLookupNotConfigured(), nil)
	}

	clusterIDs, population := graphPopulation(view)
	routedAll := limit >= population
	if !routedAll {
		clusterIDs = nil
		if g.clusterSelector != nil {
			clusterIDs = g.clusterSelector(ctx, query, view.probes)
		}
		if len(clusterIDs) == 0 {
			clusterIDs = view.routing.NearestClusters(query, view.probes)
		}
	}
	indexes := make([]*HNSWIndex, len(clusterIDs))
	for i, cid := range clusterIDs {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		indexes[i] = view.graph(cid)
		if indexes[i] == nil {
			return nil, false, fmt.Errorf("IVF-HNSW: missing graph for routed cluster %d", cid)
		}
	}
	return indexes, routedAll, nil
}

// ivfSearchBeam is a cluster graph's beam for a pool of limit candidates: the
// request's ef, or the graph's configured EfSearch, widened to hold the pool
// as single-graph search widens its beam (an explicitly larger ef is kept).
func ivfSearchBeam(index *HNSWIndex, requestEf, limit int) int {
	ef := requestEf
	if ef <= 0 {
		ef = index.Config().EfSearch
	}
	return max(ef, limit)
}
