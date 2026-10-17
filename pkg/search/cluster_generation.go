package search

import (
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
)

// clusterGeneration is the one owner of what clustered routing serves:
// routing and membership (in index, or staged while a generation is built),
// the per-cluster IVF-HNSW graphs, the lexical routing term counts and
// profiles, and the graph policy. It is kept current on every admission:
// only applyClusterAdmission, publishClusterGeneration and
// retireClusterGeneration change it, under clusterStateMu. A generation
// being built receives the same admissions through the staging log, replayed
// at publication by applyClusterAdmission.
type clusterGeneration struct {
	index    *gpu.ClusterIndex // population; published routing and membership
	staged   *gpu.Clustering   // membership while unpublished; nil once published
	graphs   map[int]*HNSWIndex
	terms    map[int]*clusterTerms
	profiles map[int]map[string]float64
	dirty    map[int]struct{} // clusters whose profile (published) or terms (staged) must be re-derived
	policy   HNSWQuantizationConfig
	// restored from persisted files: reconcile with the index's population
	// when published.
	reconcilePopulation bool
	// routingOnly: the derived state (graphs, lexical terms and profiles) was
	// retired while the index kept its published routing; only membership is
	// kept current until the next publication.
	routingOnly bool
}

// clusterTerms counts the analyzed tokens of a cluster's members' documents.
// Terms are also grouped by count (levels ascending), so a profile reads its
// top terms without scanning or sorting the cluster's vocabulary: an
// admission costs its changed tokens, not the cluster's size.
type clusterTerms struct {
	counts  map[string]float64
	total   float64
	byCount map[float64]map[string]struct{}
	levels  []float64 // distinct counts, ascending
}

func newClusterTerms() *clusterTerms {
	return &clusterTerms{counts: make(map[string]float64), byCount: make(map[float64]map[string]struct{})}
}

// add changes tok's count by n (negative to subtract); a count that reaches
// zero drops the term.
func (t *clusterTerms) add(tok string, n float64) {
	old := t.counts[tok]
	next := old + n
	if next < 0 {
		next = 0
	}
	if next == old {
		return
	}
	if old > 0 {
		t.leave(tok, old)
	}
	if next > 0 {
		t.counts[tok] = next
		t.enter(tok, next)
	} else {
		delete(t.counts, tok)
	}
	t.total += next - old
}

func (t *clusterTerms) enter(tok string, count float64) {
	set := t.byCount[count]
	if set == nil {
		set = make(map[string]struct{})
		t.byCount[count] = set
		i := sort.SearchFloat64s(t.levels, count)
		t.levels = slices.Insert(t.levels, i, count)
	}
	set[tok] = struct{}{}
}

func (t *clusterTerms) leave(tok string, count float64) {
	set := t.byCount[count]
	delete(set, tok)
	if len(set) > 0 {
		return
	}
	delete(t.byCount, count)
	if i := sort.SearchFloat64s(t.levels, count); i < len(t.levels) && t.levels[i] == count {
		t.levels = slices.Delete(t.levels, i, i+1)
	}
}

// profile returns the n highest-count terms weighted by their share of the
// cluster's tokens (ties at the cut by term name).
func (t *clusterTerms) profile(n int) map[string]float64 {
	if t == nil || t.total <= 0 {
		return nil
	}
	out := make(map[string]float64, min(n, len(t.counts)))
	for i := len(t.levels) - 1; i >= 0 && len(out) < n; i-- {
		count := t.levels[i]
		set := t.byCount[count]
		if len(out)+len(set) <= n {
			for tok := range set {
				out[tok] = count / t.total
			}
			continue
		}
		names := make([]string, 0, len(set))
		for tok := range set {
			names = append(names, tok)
		}
		sort.Strings(names)
		for _, tok := range names[:n-len(out)] {
			out[tok] = count / t.total
		}
	}
	return out
}

func (g *clusterGeneration) numClusters() int {
	if g.staged != nil {
		return g.staged.NumClusters()
	}
	return g.index.NumClusters()
}

func (g *clusterGeneration) members(cluster int) []string {
	if g.staged != nil {
		return g.staged.MemberIDs(cluster)
	}
	return g.index.GetClusterMemberIDsForCluster(cluster)
}

func (g *clusterGeneration) clusterOf(id string) (int, bool) {
	if g.staged != nil {
		return g.staged.ClusterOf(id)
	}
	return g.index.ClusterOf(id)
}

func (g *clusterGeneration) assign(id string) (int, bool, int, bool) {
	if g.staged != nil {
		return g.index.AssignToClustering(g.staged, id)
	}
	return g.index.AssignLive(id)
}

func (g *clusterGeneration) unassign(id string) (int, bool) {
	if g.staged != nil {
		return g.staged.Remove(id)
	}
	return g.index.Unassign(id)
}

func (g *clusterGeneration) touch(cluster int) {
	if g.dirty == nil {
		g.dirty = make(map[int]struct{})
	}
	g.dirty[cluster] = struct{}{}
}

func (g *clusterGeneration) graphAdd(cluster int, id string, lookup VectorLookup) {
	graph := g.graphs[cluster]
	if graph == nil || lookup == nil {
		return
	}
	if vec, ok := lookup(id); ok && len(vec) > 0 {
		_ = graph.Add(id, vec)
	}
}

func (g *clusterGeneration) graphRemove(cluster int, id string) {
	if graph := g.graphs[cluster]; graph != nil {
		graph.Remove(id)
	}
}

func (g *clusterGeneration) addTerms(cluster int, tokens map[string]float64, sign float64) {
	if len(tokens) == 0 {
		return
	}
	if g.terms == nil {
		g.terms = make(map[int]*clusterTerms)
	}
	terms := g.terms[cluster]
	if terms == nil {
		terms = newClusterTerms()
		g.terms[cluster] = terms
	}
	for tok, n := range tokens {
		terms.add(tok, sign*n)
	}
	if terms.total <= 0 || len(terms.counts) == 0 {
		delete(g.terms, cluster)
	}
	g.touch(cluster)
}

// addTokenDelta accumulates sign*tokens of one cluster into delta.
func addTokenDelta(delta map[int]map[string]float64, cluster int, tokens map[string]float64, sign float64) {
	if len(tokens) == 0 {
		return
	}
	d := delta[cluster]
	if d == nil {
		d = make(map[string]float64, len(tokens))
		delta[cluster] = d
	}
	for tok, n := range tokens {
		if v := d[tok] + sign*n; v != 0 {
			d[tok] = v
		} else {
			delete(d, tok)
		}
	}
}

// clusterAdmissionKind is what an admission changed.
type clusterAdmissionKind uint8

const (
	clusterVectorAdded   clusterAdmissionKind = iota // embedding added or updated
	clusterVectorRemoved                             // embedding removed
	clusterNodeChanged                               // a node (re)indexed or removed: its text may have changed
)

// clusterAdmission is one population change applied to a generation.
type clusterAdmission struct {
	kind clusterAdmissionKind
	id   string // vector ID, or node ID for clusterNodeChanged
	// withText: a vector change outside a node change carries its document's
	// tokens itself (backfill); inside one, the node change accounts for text.
	withText bool
	before   *clusterNodeChange // clusterNodeChanged: the node before the change
}

// clusterNodeChange is a node's member vectors and their document tokens in
// the published generation before a change, captured while the old text is
// still indexed.
type clusterNodeChange struct {
	gen     *clusterGeneration
	members []clusterMemberText
}

type clusterMemberText struct {
	cluster int
	tokens  map[string]float64
}

// clusterTextTokens analyzes the document a vector belongs to (the vector's
// own document, else its node's).
func (s *Service) clusterTextTokens(id string) map[string]float64 {
	s.mu.RLock()
	fulltext := s.fulltext()
	analyzer := normalizeAnalyzer(s.bm25Analyzer)
	s.mu.RUnlock()
	if fulltext == nil {
		return nil
	}
	text, ok := fulltext.GetDocument(id)
	if !ok {
		text, ok = fulltext.GetDocument(decodeVectorKey(id).node)
	}
	if !ok || text == "" {
		return nil
	}
	tokens := make(map[string]float64)
	for _, tok := range analyzer.Analyze(text) {
		tokens[tok]++
	}
	return tokens
}

// deriveClusterTerms counts the tokens of members' documents.
func (s *Service) deriveClusterTerms(members []string) *clusterTerms {
	counts := make(map[string]float64)
	for _, id := range members {
		for tok, n := range s.clusterTextTokens(id) {
			counts[tok] += n
		}
	}
	if len(counts) == 0 {
		return nil
	}
	terms := newClusterTerms()
	for tok, n := range counts {
		terms.add(tok, n)
	}
	return terms
}

func clusterProfileFromTerms(terms *clusterTerms) map[string]float64 {
	topTerms := envutil.GetInt("NORNICDB_VECTOR_HYBRID_ROUTING_LEX_TOP_TERMS", 64)
	if topTerms < 8 {
		topTerms = 8
	}
	return terms.profile(topTerms)
}

// nodeVectorIDsLocked lists a node's vector IDs from the node metadata
// (callers hold indexMu): the node itself, chunks, named and property vectors.
func (s *Service) nodeVectorIDsLocked(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := []string{nodeID}
	for _, id := range s.nodeChunkVectors[nodeID] {
		if id != nodeID {
			ids = append(ids, id)
		}
	}
	for _, id := range s.nodeNamedVector[nodeID] {
		ids = append(ids, id)
	}
	for _, id := range s.nodePropVector[nodeID] {
		ids = append(ids, id)
	}
	return ids
}

// applyClusterAdmission applies one population change to gen: membership,
// graph and lexical terms. On a staged generation lexical terms of affected
// clusters are re-derived at publication instead (the old text of a change
// is gone by then). Callers hold clusterStateMu.
func (s *Service) applyClusterAdmission(gen *clusterGeneration, op clusterAdmission) {
	published := gen.staged == nil
	switch op.kind {
	case clusterVectorAdded, clusterVectorRemoved:
		var tokens map[string]float64
		if op.withText && published && !gen.routingOnly {
			tokens = s.clusterTextTokens(op.id)
		}
		// A staged generation re-derives the terms of clusters whose
		// membership changed; a published one changes terms only by the
		// tokens a change carries (addTerms marks those clusters).
		if previous, wasMember := gen.unassign(op.id); wasMember {
			gen.graphRemove(previous, op.id)
			gen.addTerms(previous, tokens, -1)
			if !published {
				gen.touch(previous)
			}
		}
		if op.kind == clusterVectorRemoved {
			break
		}
		_, _, cluster, ok := gen.assign(op.id)
		if !ok {
			break
		}
		if gen.graphs != nil {
			gen.graphAdd(cluster, op.id, s.getVectorLookup())
		}
		gen.addTerms(cluster, tokens, 1)
		if !published {
			gen.touch(cluster)
		}
	case clusterNodeChanged:
		if gen.routingOnly {
			return
		}
		ids := s.nodeVectorIDsLocked(op.id)
		if !published {
			for _, id := range ids {
				if cluster, ok := gen.clusterOf(id); ok {
					gen.touch(cluster)
				}
			}
			return
		}
		// The node's tokens before and after the change, netted per
		// cluster: an unchanged text in an unchanged cluster changes nothing.
		delta := make(map[int]map[string]float64)
		if op.before != nil && op.before.gen == gen {
			for _, member := range op.before.members {
				addTokenDelta(delta, member.cluster, member.tokens, -1)
			}
		}
		for _, id := range ids {
			if cluster, ok := gen.clusterOf(id); ok {
				addTokenDelta(delta, cluster, s.clusterTextTokens(id), 1)
			}
		}
		for cluster, tokens := range delta {
			gen.addTerms(cluster, tokens, 1)
		}
	}
	if gen.routingOnly {
		gen.dirty = nil
		return
	}
	if published {
		for cluster := range gen.dirty {
			if profile := clusterProfileFromTerms(gen.terms[cluster]); profile != nil {
				gen.profiles[cluster] = profile
			} else {
				delete(gen.profiles, cluster)
			}
		}
		gen.dirty = nil
	}
}

// admitLocked applies op to the published generation and records it for a
// generation being built. Callers hold indexMu and clusterStateMu.
func (s *Service) admitLocked(op clusterAdmission) {
	if gen := s.clusterGen; gen != nil {
		s.applyClusterAdmission(gen, op)
	}
	if s.clusterStaging != nil {
		*s.clusterStaging = append(*s.clusterStaging, op)
	}
}

// admitClusterVectorLocked adds or updates id in the cluster population and
// the generation(s). Callers hold indexMu, the admission boundary.
func (s *Service) admitClusterVectorLocked(id string, vec []float32) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	if s.clusterIndex == nil {
		return
	}
	_ = s.clusterIndex.Add(id, vec) // Best effort
	s.admitLocked(clusterAdmission{kind: clusterVectorAdded, id: id})
}

// admitClusterVectorsLocked backfills vectors into the cluster population
// outside a node change; callers hold indexMu.
func (s *Service) admitClusterVectorsLocked(ids []string, vecs [][]float32) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	if s.clusterIndex == nil {
		return
	}
	_ = s.clusterIndex.AddBatch(ids, vecs) // Best effort
	for _, id := range ids {
		s.admitLocked(clusterAdmission{kind: clusterVectorAdded, id: id, withText: true})
	}
}

// retireClusterVectorLocked removes id from the cluster population and the
// generation(s); callers hold indexMu.
func (s *Service) retireClusterVectorLocked(id string) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	if s.clusterIndex == nil {
		return
	}
	s.admitLocked(clusterAdmission{kind: clusterVectorRemoved, id: id})
	s.clusterIndex.Remove(id)
}

// beginClusterNodeChangeLocked captures a node's member clusters and document
// tokens before the node changes; endClusterNodeChangeLocked applies the
// change. Callers hold indexMu across both.
func (s *Service) beginClusterNodeChangeLocked(nodeID string) *clusterNodeChange {
	s.clusterStateMu.RLock()
	defer s.clusterStateMu.RUnlock()
	gen := s.clusterGen
	if gen == nil || gen.routingOnly {
		return nil
	}
	change := &clusterNodeChange{gen: gen}
	for _, id := range s.nodeVectorIDsLocked(nodeID) {
		if cluster, ok := gen.clusterOf(id); ok {
			change.members = append(change.members, clusterMemberText{cluster: cluster, tokens: s.clusterTextTokens(id)})
		}
	}
	return change
}

func (s *Service) endClusterNodeChangeLocked(nodeID string, before *clusterNodeChange) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	if s.clusterGen == nil && s.clusterStaging == nil {
		return
	}
	s.admitLocked(clusterAdmission{kind: clusterNodeChanged, id: nodeID, before: before})
}

// openClusterStaging starts recording admissions for a generation about to
// be computed. The snapshot is taken afterwards, so replaying the log is
// idempotent: an add of a captured id or a remove of an absent id is a no-op
// in effect.
func (s *Service) openClusterStaging() {
	s.indexMu.Lock()
	s.clusterStaging = &[]clusterAdmission{}
	s.indexMu.Unlock()
}

// closeClusterStaging stops recording when no generation will be installed.
func (s *Service) closeClusterStaging() {
	s.indexMu.Lock()
	s.clusterStaging = nil
	s.indexMu.Unlock()
}

// errClusterGenerationSuperseded reports a generation computed for a cluster
// index that was replaced before it could be published; it is discarded.
var errClusterGenerationSuperseded = errors.New("cluster generation superseded")

// clusterPublishHook, set by tests only, runs after a generation's identity
// check and before it is published.
var clusterPublishHook func()

// clusterPublication is what publishClusterGeneration publishes: a new
// generation (k-means or restore), or the configured graph policy for the
// published one.
type clusterPublication struct {
	generation *clusterGeneration
	policy     bool
}

// publishClusterGeneration makes a generation current, atomically with
// respect to every admission (indexMu) and every replacement (clusterStateMu):
// it replays the admissions made since the snapshot, reconciles a restored
// generation with the population, re-derives the terms of affected clusters,
// applies the current graph policy, publishes routing and membership, then
// publishes the serving pipeline and advances the index revision.
func (s *Service) publishClusterGeneration(ctx context.Context, pub clusterPublication) error {
	s.indexMu.Lock()
	s.clusterStateMu.Lock()
	if pub.policy {
		// The policy is read under the owner's locks, so concurrent
		// reconfigurations leave the graphs in the latest one.
		var err error
		if gen := s.clusterGen; gen != nil {
			err = s.applyClusterPolicyLocked(ctx, gen, s.hnswConfig().Quantization)
		}
		s.clusterStateMu.Unlock()
		s.indexMu.Unlock()
		if err == nil {
			s.publishClusterServing()
		}
		return err
	}
	gen := pub.generation
	s.mu.RLock()
	current := s.clusterIndex
	s.mu.RUnlock()
	if current != gen.index {
		// A dimension change or EnableClustering replaced the cluster index
		// while this generation was built (neither excludes a clustering run);
		// every replacement holds clusterStateMu, so the index cannot change
		// between this check and the publication below.
		s.clusterStaging = nil
		s.clusterStateMu.Unlock()
		s.indexMu.Unlock()
		return errClusterGenerationSuperseded
	}
	if clusterPublishHook != nil {
		clusterPublishHook()
	}
	if s.clusterStaging != nil {
		for _, op := range *s.clusterStaging {
			s.applyClusterAdmission(gen, op)
		}
	}
	s.clusterStaging = nil
	if gen.reconcilePopulation {
		s.reconcileClusterPopulationLocked(gen)
	}
	if gen.terms == nil {
		gen.terms = make(map[int]*clusterTerms)
	}
	for cluster := range gen.dirty {
		if terms := s.deriveClusterTerms(gen.members(cluster)); terms != nil {
			gen.terms[cluster] = terms
		} else {
			delete(gen.terms, cluster)
		}
	}
	gen.dirty = nil
	gen.profiles = make(map[int]map[string]float64, len(gen.terms))
	for cluster, terms := range gen.terms {
		if profile := clusterProfileFromTerms(terms); profile != nil {
			gen.profiles[cluster] = profile
		}
	}
	err := s.applyClusterPolicyLocked(ctx, gen, s.hnswConfig().Quantization)
	gen.index.PublishClustering(gen.staged)
	gen.staged = nil
	s.clusterGen = gen
	s.clusterStateMu.Unlock()
	s.indexMu.Unlock()
	s.publishClusterServing()
	return err
}

// applyClusterPolicyLocked encodes gen's graphs with policy. A published
// generation is re-encoded only when the policy changed; a generation being
// published is always brought to it (restored graphs carry the policy they
// were saved with, and SetQuantization is a no-op for a graph already in it).
// Callers hold clusterStateMu.
func (s *Service) applyClusterPolicyLocked(ctx context.Context, gen *clusterGeneration, policy HNSWQuantizationConfig) error {
	if gen.staged == nil && gen.policy == policy {
		return nil
	}
	for _, graph := range gen.graphs {
		if err := graph.SetQuantization(ctx, policy); err != nil {
			return err
		}
	}
	gen.policy = policy
	return nil
}

// reconcileClusterPopulationLocked brings a restored generation in line with
// the index's population: vectors missing from it are assigned, members no
// longer in the population are removed, and each graph is made to hold
// exactly its cluster's members. Every cluster's terms are re-derived.
func (s *Service) reconcileClusterPopulationLocked(gen *clusterGeneration) {
	population := gen.index.NodeIDs()
	present := make(map[string]struct{}, len(population))
	for _, id := range population {
		present[id] = struct{}{}
		if _, ok := gen.clusterOf(id); !ok {
			s.applyClusterAdmission(gen, clusterAdmission{kind: clusterVectorAdded, id: id})
		}
	}
	lookup := s.getVectorLookup()
	for cluster := 0; cluster < gen.numClusters(); cluster++ {
		members := gen.members(cluster)
		want := make(map[string]struct{}, len(members))
		for _, id := range members {
			if _, ok := present[id]; !ok {
				s.applyClusterAdmission(gen, clusterAdmission{kind: clusterVectorRemoved, id: id})
				continue
			}
			want[id] = struct{}{}
		}
		if graph := gen.graphs[cluster]; graph != nil {
			for _, id := range graph.liveIDs() {
				if _, ok := want[id]; !ok {
					graph.Remove(id)
				}
				delete(want, id)
			}
			ids := make([]string, 0, len(want))
			for id := range want {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				gen.graphAdd(cluster, id, lookup)
			}
		}
		gen.touch(cluster)
	}
}

// retireClusterGeneration retires the published generation's derived state.
// A non-nil replacement also replaces the cluster index; clearIndex empties
// the index it retires (and releases it when replaced). An index that keeps
// its published routing keeps a routing-only generation, so its membership
// stays current until the next publication. Callers hold clusterStateMu and,
// when replacing or clearing the index, mu.
func (s *Service) retireClusterGeneration(replacement *gpu.ClusterIndex, clearIndex bool) {
	old := s.clusterIndex
	s.clusterGen = nil
	if replacement != nil {
		s.clusterIndex = replacement
	}
	if clearIndex && old != nil {
		old.Clear()
		if replacement != nil {
			old.Release()
		}
	}
	if replacement == nil && !clearIndex && old != nil && old.IsClustered() {
		s.clusterGen = &clusterGeneration{index: old, routingOnly: true}
	}
}

// publishClusterServing publishes the serving pipeline for the current
// cluster generation and advances the index revision. Callers hold none of
// the owner's locks.
func (s *Service) publishClusterServing() {
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()
	s.indexChanged()
}

// liveIDs lists the graph's live (not deleted) vector IDs.
func (h *HNSWIndex) liveIDs() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]string, 0, len(h.idToInternal))
	for id, internal := range h.idToInternal {
		if validHNSWIndex(internal, len(h.deleted)) && h.deleted[internal] {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}
