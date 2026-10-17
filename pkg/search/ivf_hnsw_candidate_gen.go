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
// call's hold on the generation.
func (c *clusterServing) open(ctx context.Context, own clusterView) (context.Context, clusterView, func(), func()) {
	if c == nil {
		return ctx, own, func() {}, func() {}
	}
	ctx, view, done := c.view(ctx)
	c.lock.Lock()
	return ctx, view, c.lock.Unlock, done
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
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, view, unlock, done := g.serving.open(ctx, ownClusterView(g.clusterIndex, g.numClustersToSearch, g.getClusterHNSW))
	defer done()
	indexes, err := g.routedGraphs(ctx, query, view)
	unlock()
	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return []Candidate{}, nil
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
			// with a beam wide enough to supply it.
			results[i], errors[i] = indexes[i].SearchWithEf(ctx, query, totalLimit, minSimilarity, ivfSearchBeam(indexes[i], ef, totalLimit))
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
	best := make(map[string]float64, totalLimit)
	for i, hits := range results {
		if errors[i] != nil {
			return nil, errors[i]
		}
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
	}
	return out, nil
}

// routedGraphs routes query on view's generation and takes the routed
// clusters' graphs from the same generation. Missing graphs are incomplete
// ANN state, not permission to scan the entire routed population.
func (g *IVFHNSWCandidateGen) routedGraphs(ctx context.Context, query []float32, view clusterView) ([]*HNSWIndex, error) {
	if view.routing == nil {
		return nil, localizedError(localization.SearchClusterIndexNotClustered(), nil)
	}
	if view.graph == nil {
		return nil, localizedError(localization.SearchClusterHNSWLookupNotConfigured(), nil)
	}

	clusterIDs := []int(nil)
	if g.clusterSelector != nil {
		clusterIDs = g.clusterSelector(ctx, query, view.probes)
	}
	if len(clusterIDs) == 0 {
		clusterIDs = view.routing.NearestClusters(query, view.probes)
	}
	indexes := make([]*HNSWIndex, len(clusterIDs))
	for i, cid := range clusterIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		indexes[i] = view.graph(cid)
		if indexes[i] == nil {
			return nil, fmt.Errorf("IVF-HNSW: missing graph for routed cluster %d", cid)
		}
	}
	return indexes, nil
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
