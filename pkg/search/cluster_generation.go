package search

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
)

// clusterGeneration is the one owner of what clustered routing serves:
// routing and membership (the generation's own Clustering),
// the per-cluster IVF-HNSW graphs, the lexical routing term counts and
// profiles, and the graph policy. It is kept current on every admission:
// only applyClusterAdmission, publishClusterGeneration and
// retireClusterGeneration change it, under clusterStateMu. A generation
// being built receives the same admissions through the staging log, replayed
// at publication by applyClusterAdmission.
//
// Its inputs are the canonical population: the cluster index holds exactly
// the canonical vector store's vectors (every admission reads the vector from
// the store under the admission boundary), and lexical terms are derived from
// the fulltext index the service serves.
//
// The generation owns its routing (centroids and membership): admissions
// change it through AssignToClustering/Remove whether it is published or
// still being built, and publication makes the index route through it. Once
// a newer generation is published or this one is retired, nothing changes
// it again, so a query that took it at its start (clusterSnapshot) finishes
// on it.
type clusterGeneration struct {
	index     *gpu.ClusterIndex // the population (embeddings)
	routing   *gpu.Clustering   // centroids and membership by node ID
	published bool
	// holders counts the queries serving from generations of index; a
	// retired index's GPU buffers are released after the last one finishes.
	holders  *sync.WaitGroup
	graphs   map[int]*HNSWIndex
	terms    map[int]*clusterTerms
	profiles map[int]map[string]float64
	dirty    map[int]struct{} // clusters whose profile (published) or terms (staged) must be re-derived
	// policy is the graph policy applied to every graph; it changes only once
	// all of them are in the new one, so a failed or cancelled policy change
	// leaves it different from the configured policy and the next
	// publication completes it.
	policy HNSWQuantizationConfig
}

// newClusterGeneration is an unpublished generation for clustering of index,
// with its graphs (nil without IVF-HNSW) and the terms of its clusters.
func newClusterGeneration(index *gpu.ClusterIndex, clustering *gpu.Clustering, graphs map[int]*HNSWIndex, terms map[int]*clusterTerms) *clusterGeneration {
	return &clusterGeneration{index: index, routing: clustering, graphs: graphs, terms: terms}
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

func (g *clusterGeneration) numClusters() int { return g.routing.NumClusters() }

func (g *clusterGeneration) members(cluster int) []string { return g.routing.MemberIDs(cluster) }

func (g *clusterGeneration) clusterOf(id string) (int, bool) { return g.routing.ClusterOf(id) }

// assign and unassign are the one membership path of a generation, published
// or being built.
func (g *clusterGeneration) assign(id string) (int, bool, int, bool) {
	return g.index.AssignToClustering(g.routing, id)
}

func (g *clusterGeneration) unassign(id string) (int, bool) { return g.routing.Remove(id) }

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
	kind     clusterAdmissionKind
	vectorID vectorEntryID // vector add/remove admissions
	nodeID   vectorNodeID  // document changes
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
	owner, valid := vectorEntryOwner(id)
	if !valid {
		return nil
	}
	text, ok := fulltext.GetDocument(owner)
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
// (callers hold indexMu): the main entry, chunks, named and property vectors.
func (s *Service) nodeVectorIDsLocked(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []string
	s.forEachNodeVectorKeyLocked(nodeID, func(key string) { ids = append(ids, key) })
	return ids
}

// applyClusterAdmission applies one population change to gen: membership,
// graph and lexical terms. On a staged generation lexical terms of affected
// clusters are re-derived at publication instead (the old text of a change
// is gone by then). Callers hold clusterStateMu.
func (s *Service) applyClusterAdmission(gen *clusterGeneration, op clusterAdmission) {
	published := gen.published
	switch op.kind {
	case clusterVectorAdded, clusterVectorRemoved:
		var tokens map[string]float64
		if op.withText && published {
			tokens = s.clusterTextTokens(op.vectorID.entryKeyString())
		}
		// A staged generation re-derives the terms of clusters whose
		// membership changed; a published one changes terms only by the
		// tokens a change carries (addTerms marks those clusters).
		if previous, wasMember := gen.unassign(op.vectorID.entryKeyString()); wasMember {
			gen.graphRemove(previous, op.vectorID.entryKeyString())
			gen.addTerms(previous, tokens, -1)
			if !published {
				gen.touch(previous)
			}
		}
		if op.kind == clusterVectorRemoved {
			break
		}
		_, _, cluster, ok := gen.assign(op.vectorID.entryKeyString())
		if !ok {
			break
		}
		if gen.graphs != nil {
			gen.graphAdd(cluster, op.vectorID.entryKeyString(), s.getVectorLookup())
		}
		gen.addTerms(cluster, tokens, 1)
		if !published {
			gen.touch(cluster)
		}
	case clusterNodeChanged:
		ids := s.nodeVectorIDsLocked(op.nodeID.nodeIDString())
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

// admitClusterVectorLocked brings id's entry in the cluster population and
// the generation(s) to the canonical vector store's current vector (just
// written by the caller). Callers hold indexMu, the admission boundary.
func (s *Service) admitClusterVectorLocked(id string) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	s.admitCanonicalLocked(id, false, false)
}

// admitClusterVectorsLocked reconciles the cluster population entries of ids
// with the canonical vector store outside a node change (backfill): each is
// resolved from the store now, under the admission boundary, so an entry
// listed before a later write is admitted with that write's vector, and one
// removed since is not resurrected. Callers hold indexMu.
func (s *Service) admitClusterVectorsLocked(ids []string) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	for _, id := range ids {
		s.admitCanonicalLocked(id, true, true)
	}
}

// admitCanonicalLocked admits id from the canonical store: added or updated
// with the store's vector when the store holds one, retired when it does
// not. onlyChanged skips an entry that already holds the store's vector;
// withText carries the vector's document tokens (outside a node change).
// Callers hold indexMu and clusterStateMu.
func (s *Service) admitCanonicalLocked(id string, withText, onlyChanged bool) {
	if s.clusterIndex == nil {
		return
	}
	vec, ok := s.getVectorLookup()(id)
	if !ok || len(vec) == 0 {
		if _, admitted := s.clusterIndex.Get(id); admitted {
			s.admitLocked(clusterAdmission{kind: clusterVectorRemoved, vectorID: vectorEntryIdentity(id), withText: withText})
			s.clusterIndex.Remove(id)
		}
		return
	}
	if onlyChanged {
		if current, admitted := s.clusterIndex.Get(id); admitted && slices.Equal(current, vec) {
			return
		}
	}
	if err := s.clusterIndex.Add(id, vec); err != nil {
		return
	}
	s.admitLocked(clusterAdmission{kind: clusterVectorAdded, vectorID: vectorEntryIdentity(id), withText: withText})
}

// retireClusterVectorLocked removes id from the cluster population and the
// generation(s); callers hold indexMu.
func (s *Service) retireClusterVectorLocked(id string) {
	s.clusterStateMu.Lock()
	defer s.clusterStateMu.Unlock()
	if s.clusterIndex == nil {
		return
	}
	s.admitLocked(clusterAdmission{kind: clusterVectorRemoved, vectorID: vectorEntryIdentity(id)})
	s.clusterIndex.Remove(id)
}

// beginClusterNodeChangeLocked captures a node's member clusters and document
// tokens before the node changes; endClusterNodeChangeLocked applies the
// change. Callers hold indexMu across both.
func (s *Service) beginClusterNodeChangeLocked(nodeID string) *clusterNodeChange {
	s.clusterStateMu.RLock()
	defer s.clusterStateMu.RUnlock()
	gen := s.clusterGen
	if gen == nil {
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
	s.admitLocked(clusterAdmission{kind: clusterNodeChanged, nodeID: vectorNodeIdentity(nodeID), before: before})
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

// stageStaleClusterMembers records, for a restored generation being built,
// an admission for every retained graph member whose saved coordinates are
// not the canonical store's (its vector changed after the save, possibly
// within the same cluster). Publication replays them through
// applyClusterAdmission like any admission made during the build: the
// member is re-placed and re-inserted with its current vector, so its
// quantized code and links describe where it is now.
func (s *Service) stageStaleClusterMembers(gen *clusterGeneration) {
	lookup := s.getVectorLookup()
	var stale []string
	for _, graph := range gen.graphs {
		stale = append(stale, graph.staleMembers(lookup)...)
	}
	if len(stale) == 0 {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if s.clusterStaging == nil {
		return
	}
	for _, id := range stale {
		*s.clusterStaging = append(*s.clusterStaging, clusterAdmission{kind: clusterVectorAdded, vectorID: vectorEntryIdentity(id)})
	}
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
// it replays the admissions made since the snapshot, re-derives the terms of
// affected clusters, applies the current graph policy, publishes routing and
// membership, then publishes the serving pipeline and advances the index
// revision. A policy publication brings the published generation's graphs to
// the configured policy; it compares against the policy the graphs were last
// fully brought to, so retrying a cancelled one completes it.
func (s *Service) publishClusterGeneration(ctx context.Context, pub clusterPublication) error {
	s.indexMu.Lock()
	s.clusterStateMu.Lock()
	if pub.policy {
		// The policy is read under the owner's locks, so concurrent
		// reconfigurations leave the graphs in the latest one.
		var changed bool
		var err error
		if gen := s.clusterGen; gen != nil {
			changed, err = s.applyClusterPolicyLocked(ctx, gen, s.hnswConfig().Quantization)
		}
		s.clusterStateMu.Unlock()
		s.indexMu.Unlock()
		if changed {
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
	// Graphs were brought to the policy when they were built; this applies a
	// policy configured since. A failure leaves gen.policy behind the
	// configured one, so the next policy publication completes it.
	_, err := s.applyClusterPolicyLocked(ctx, gen, s.hnswConfig().Quantization)
	// The index routes through the generation's own routing from now on; the
	// superseded generation keeps its routing for the queries holding it,
	// which share the index's holder count.
	gen.index.PublishClustering(gen.routing)
	gen.published = true
	if prev := s.clusterGen; prev != nil && prev.index == gen.index {
		gen.holders = prev.holders
	} else {
		gen.holders = &sync.WaitGroup{}
	}
	s.clusterGen = gen
	s.clusterStateMu.Unlock()
	s.indexMu.Unlock()
	s.publishClusterServing()
	return err
}

// applyClusterPolicyLocked brings gen's graphs to policy and reports whether
// any graph had to change. gen.policy records the policy every graph is in:
// it is set only once all graphs reached it, so a cancelled or failed apply
// is completed by the next one (SetQuantization is a no-op for a graph
// already in the policy). A generation being published is always checked.
// Callers hold clusterStateMu.
func (s *Service) applyClusterPolicyLocked(ctx context.Context, gen *clusterGeneration, policy HNSWQuantizationConfig) (bool, error) {
	if gen.published && gen.policy == policy {
		return false, nil
	}
	for _, graph := range gen.graphs {
		if err := graph.SetQuantization(ctx, policy); err != nil {
			return true, err
		}
	}
	gen.policy = policy
	return true, nil
}

// clusterIndexReleaseHook, set by tests only, runs when a retired cluster
// index is released, after its last query holder finished.
var clusterIndexReleaseHook func(*gpu.ClusterIndex)

// emptyClusterIndex replaces a cluster index with an empty one of the same
// settings, so the population is derived again from the canonical store.
func emptyClusterIndex(old *gpu.ClusterIndex) *gpu.ClusterIndex {
	if old == nil {
		return nil
	}
	return old.Empty()
}

// retireClusterGeneration retires the published generation and replaces the
// cluster index with replace(old); a nil replace leaves no index (Close).
// The retired index and generation are never changed in place: queries that
// took the generation at their start finish on it, and the index's GPU
// buffers are released only after the last of them (GPU subset scoring reads
// the device without the index's lock). Callers hold clusterStateMu and mu;
// they publish the serving pipeline (publishClusterServing) once they
// release the owner's locks.
func (s *Service) retireClusterGeneration(replace func(old *gpu.ClusterIndex) *gpu.ClusterIndex) {
	old, gen := s.clusterIndex, s.clusterGen
	var replacement *gpu.ClusterIndex
	if replace != nil {
		replacement = replace(old)
	}
	s.clusterGen = nil
	s.clusterIndex = replacement
	if old == nil || old == replacement {
		return
	}
	var holders *sync.WaitGroup
	if gen != nil && gen.index == old {
		holders = gen.holders
	}
	go func() {
		if holders != nil {
			holders.Wait()
		}
		if hook := clusterIndexReleaseHook; hook != nil {
			hook(old)
		}
		old.Release()
	}()
}

// publishClusterServing publishes the serving pipeline for the current
// cluster generation and advances the index revision. Callers hold none of
// the owner's locks. Clustered pipelines hold no generation: each query
// serves from the one it took at its start (clusterSnapshot).
func (s *Service) publishClusterServing() {
	s.publishVectorServing(func() {}, true)
}

// currentClusterIndex is the cluster index (the clustered population) for
// code outside the owner; its routing is read through a generation. Callers
// hold mu where they did for the field.
func (s *Service) currentClusterIndex() *gpu.ClusterIndex { return s.clusterIndex }

// clusterGenerationPublishedLocked reports whether a generation is
// published; callers hold clusterStateMu.
func (s *Service) clusterGenerationPublishedLocked() bool { return s.clusterGen != nil }

// clusterGenerationHasGraphsLocked reports whether the published generation
// has IVF-HNSW graphs; callers hold clusterStateMu.
func (s *Service) clusterGenerationHasGraphsLocked() bool {
	return s.clusterGen != nil && len(s.clusterGen.graphs) > 0
}

// clusterSnapshot is the cluster generation one query serves from, taken
// once at its start; nil gen when none was published.
type clusterSnapshot struct {
	gen *clusterGeneration
}

type clusterSnapshotKey struct{}

// acquireClusterSnapshot takes the published generation for one query (one
// load under clusterStateMu) and returns ctx carrying it. The query holds
// the generation until release; every candidate-generation call of the
// query (including adaptive overfetch rounds) routes on it.
func (s *Service) acquireClusterSnapshot(ctx context.Context) (context.Context, *clusterGeneration, func()) {
	s.clusterStateMu.RLock()
	gen := s.clusterGen
	if gen != nil {
		gen.holders.Add(1)
	}
	s.clusterStateMu.RUnlock()
	release := func() {}
	if gen != nil {
		var once sync.Once
		release = func() { once.Do(gen.holders.Done) }
	}
	return context.WithValue(ctx, clusterSnapshotKey{}, &clusterSnapshot{gen: gen}), gen, release
}

// vectorPipelineForQuery resolves the pipeline one query uses together with
// the cluster generation it serves from, carried in the returned ctx. A
// cached clustered pipeline that cannot serve that generation (retired, or
// replaced by a CPU/GPU mode change or a clear) is dropped and the current
// one resolved; release ends the query's hold on the generation.
func (s *Service) vectorPipelineForQuery(ctx context.Context) (context.Context, *VectorSearchPipeline, func(), error) {
	for attempt := 0; ; attempt++ {
		queryCtx, gen, release := s.acquireClusterSnapshot(ctx)
		pipeline, err := s.getOrCreateVectorPipeline(queryCtx)
		if err != nil {
			release()
			return ctx, nil, func() {}, err
		}
		if pipelineServesGeneration(pipeline, gen) || attempt == 2 {
			return queryCtx, pipeline, release, nil
		}
		release()
		s.pipelineMu.Lock()
		if s.servingPipeline() == pipeline {
			s.cacheDerivedVectorPipelineLocked(nil)
		}
		s.pipelineMu.Unlock()
	}
}

// pipelineServesGeneration reports whether pipeline can serve a query that
// holds gen: a clustered generator needs a generation, IVF-HNSW one with
// graphs. Any other strategy answers from the canonical store.
func pipelineServesGeneration(pipeline *VectorSearchPipeline, gen *clusterGeneration) bool {
	switch pipeline.candidateGen.(type) {
	case *IVFHNSWCandidateGen:
		return gen != nil && len(gen.graphs) > 0
	case *KMeansCandidateGen, *GPUKMeansCandidateGen:
		return gen != nil
	}
	return true
}

// clusterGenerationFor is the generation ctx's query serves from; outside a
// query it takes one for the call (release when done).
func (s *Service) clusterGenerationFor(ctx context.Context) (context.Context, *clusterGeneration, func()) {
	if snap, ok := ctx.Value(clusterSnapshotKey{}).(*clusterSnapshot); ok {
		return ctx, snap.gen, func() {}
	}
	return s.acquireClusterSnapshot(ctx)
}

// clusterQueryView is how a clustered generator serves one query: the
// routing of the generation the query holds, its probe count
// (NORNICDB_KMEANS_NPROBE, by default the IVF rule over its clusters), its
// graphs and its population.
func (s *Service) clusterQueryView(ctx context.Context) (context.Context, clusterView, func()) {
	ctx, gen, done := s.clusterGenerationFor(ctx)
	if gen == nil {
		return ctx, clusterView{}, done
	}
	numClusters := gen.routing.NumClusters()
	return ctx, clusterView{
		routing: gen.routing,
		index:   gen.index,
		probes:  clampInt(envutil.GetInt("NORNICDB_KMEANS_NPROBE", defaultListsToProbe(numClusters)), 1, max(1, numClusters)),
		graph:   func(cluster int) *HNSWIndex { return gen.graphs[cluster] },
	}, done
}

// clusterGenerationIn is the generation ctx's query serves from, else the
// published one; callers hold clusterStateMu (read).
func (s *Service) clusterGenerationIn(ctx context.Context) *clusterGeneration {
	if snap, ok := ctx.Value(clusterSnapshotKey{}).(*clusterSnapshot); ok {
		return snap.gen
	}
	return s.clusterGen
}

// clusterGenerationSave is a published generation's graphs, captured as one
// population for persistence.
type clusterGenerationSave struct {
	graphs map[int]*hnswIndexSnapshot
}

// snapshotClusterGenerationLocked captures every graph of the published
// generation under the owner's read lock, which excludes every admission and
// publication: the saved graphs hold one population. The disk writes run
// after the lock is released. Callers hold clusterStateMu (read).
func (s *Service) snapshotClusterGenerationLocked() *clusterGenerationSave {
	gen := s.clusterGen
	if gen == nil || len(gen.graphs) == 0 {
		return nil
	}
	save := &clusterGenerationSave{graphs: make(map[int]*hnswIndexSnapshot, len(gen.graphs))}
	for cid, graph := range gen.graphs {
		save.graphs[cid] = graph.snapshot()
	}
	return save
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
