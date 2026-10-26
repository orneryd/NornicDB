package search

import (
	"context"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Relationship search (#1065): optional keyword (BM25) and vector search
// over relationship properties, such as the text explaining why two things
// are linked. It keeps indexes of its own, so node search is unchanged, and
// is off unless enabled (NORNICDB_SEARCH_RELATIONSHIPS_ENABLED). A search
// with SearchOptions.Relationships returns relationships, each with its type
// and both endpoints.
//
// The keyword text of a relationship is taken as a node's is, with its type
// in place of labels: the NORNICDB_SEARCH_BM25_PROPERTIES values when set,
// otherwise its type and every property. Its vectors come from the embed
// function the database supplies, which builds the text as for nodes
// (NORNICDB_EMBEDDING_PROPERTIES_INCLUDE / EXCLUDE). The keyword index is
// built from storage when the service starts; the vectors are kept in a file
// beside the BM25 index, when search indexes are persisted, so a restart
// embeds only relationships that changed.

// relationshipVectorsFileSuffix names the relationship vector file, beside
// the BM25 index file.
const relationshipVectorsFileSuffix = ".relationships"

// relationshipEmbedRetryDelay is how long the embed worker waits after the
// embed function fails (no embedder loaded yet) before trying again.
var relationshipEmbedRetryDelay = 30 * time.Second

// relationshipVectorsFormat versions the relationship vector file.
// Format 2 added each entry's chunk spans: a format 1 file is dropped, so
// its relationships are embedded again and gain them.
const relationshipVectorsFormat = 2

// RelationshipEmbedFunc embeds a relationship's text, one vector per chunk.
// It returns no vectors for a relationship with no text to embed, and an
// error when it can't embed now; the relationship is then tried again later.
type RelationshipEmbedFunc func(ctx context.Context, edge *storage.Edge) (RelationshipEmbedding, error)

// RelationshipEmbedding is a relationship's chunk vectors and where each
// chunk lies in its string properties, as a node's embedding records them.
type RelationshipEmbedding struct {
	Vectors [][]float32
	Spans   []embeddingutil.ChunkSpan
}

// ErrRelationshipSearchDisabled is returned for a relationship search on a
// database whose relationship indexing is off.
var ErrRelationshipSearchDisabled = errors.New("relationship search is not enabled")

type relationshipEntry struct {
	// Source is a hash of the type and properties the entry was indexed
	// from; vectors are embedded again when it changes.
	Source uint64
	// Vectors are the relationship's chunk embeddings, normalized; nil until
	// embedded or when it has no text to embed.
	Vectors [][]float32
	// Spans are where each chunk lies in the relationship's string
	// properties, for its chunk matches; nil when not known.
	Spans []embeddingutil.ChunkSpan
}

type relationshipSearch struct {
	// The lifecycle lock pairs worker startup with Close's join. These
	// fields are separate from the loader's live-write ordering state.
	workerMu      sync.Mutex
	workerStarted bool
	done          chan struct{}
	embedMu       sync.Mutex
	saveErr       error
	// staged belongs to the embedding worker. Failed saves keep the batch
	// here so retrying persistence never buys the same embeddings again.
	staged map[string]relationshipEntry

	// beforeSave is a per-service persistence barrier used by tests.
	beforeSave func()

	// Directory durability follows storage's platform-specific contract.
	// Tests can hold or fail this boundary without replacing file writes.
	syncDirectory func(string) error

	mu       sync.RWMutex
	fulltext bm25Index
	entries  map[string]*relationshipEntry
	embed    RelationshipEmbedFunc
	// pending holds the relationships to embed, each with the copy its
	// write reported, read in place of the stored one when that isn't
	// readable yet (a buffered commit can report a write before it reads).
	pending map[string]*storage.Edge
	// policy hashes the embedding policy (SetRelationshipEmbedPolicy) into
	// every source; zero when none was set.
	policy uint64
	wake   chan struct{}
	ready  chan struct{}
	start  sync.Once

	// While the loader (runRelationshipIndex) applies the stored
	// relationships, live writes record their IDs in live and the loader
	// leaves those IDs to them: its stored copy may be older. Guarded by mu.
	loading bool
	live    map[string]struct{}
}

type relationshipVectorsFile struct {
	Format  int
	Entries map[string]*relationshipEntry
}

// EnableRelationshipSearch turns relationship indexing on for this service,
// with embed (nil: keyword search only) for its vectors. Call it once, after
// SetFulltextProperties and SetFulltextIndexPath, then
// StartRelationshipIndex.
func (s *Service) EnableRelationshipSearch(embed RelationshipEmbedFunc) {
	if s == nil || s.relationships.Load() != nil {
		return
	}
	fulltext, _ := newBM25IndexWithAnalyzer(s.bm25Engine, s.bm25Analyzer)
	s.relationships.Store(&relationshipSearch{
		fulltext: fulltext,
		entries:  make(map[string]*relationshipEntry),
		embed:    embed,
		pending:  make(map[string]*storage.Edge),
		wake:     make(chan struct{}, 1),
		ready:    make(chan struct{}),
		staged:   make(map[string]relationshipEntry),
		done:     make(chan struct{}),

		syncDirectory: storage.SyncDir,
	})
}

// RelationshipSearchEnabled reports whether this service indexes
// relationships.
func (s *Service) RelationshipSearchEnabled() bool {
	return s != nil && s.relationships.Load() != nil
}

// RelationshipEmbeddingCount reports how many relationships have vectors.
func (s *Service) RelationshipEmbeddingCount() int {
	rs := s.relationshipSearch()
	if rs == nil {
		return 0
	}
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	count := 0
	for _, entry := range rs.entries {
		if len(entry.Vectors) > 0 {
			count++
		}
	}
	return count
}

func (s *Service) relationshipSearch() *relationshipSearch {
	if s == nil {
		return nil
	}
	return s.relationships.Load()
}

// StartRelationshipIndex builds the relationship keyword index from storage,
// keeping the persisted vectors of relationships that haven't changed, then
// embeds the others in the background until the service closes. It returns
// at once; searches wait for the build.
func (s *Service) StartRelationshipIndex() {
	rs := s.relationshipSearch()
	if rs == nil {
		return
	}
	rs.workerMu.Lock()
	defer rs.workerMu.Unlock()
	if s.lifecycleCtx.Err() != nil {
		return
	}
	rs.workerStarted = true
	rs.start.Do(func() {
		rs.mu.Lock()
		rs.loading, rs.live = true, make(map[string]struct{})
		rs.mu.Unlock()
		go s.runRelationshipIndex(rs)
	})
}

func (s *Service) runRelationshipIndex(rs *relationshipSearch) {
	defer close(rs.done)
	ctx := s.lifecycleCtx
	persisted := s.loadRelationshipVectors()
	seen := make(map[string]bool)
	err := storage.StreamEdgesWithFallback(ctx, s.engine, 0, func(edge *storage.Edge) error {
		seen[string(edge.ID)] = true
		s.indexRelationship(rs, edge, persisted[string(edge.ID)], true)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		s.logPrintf("⚠️ Relationship search: building the index from storage failed: %v", err)
	}
	// A relationship indexed by a write during the build is kept; one only
	// in the file is gone.
	rs.mu.Lock()
	rs.loading, rs.live = false, nil
	rs.mu.Unlock()
	close(rs.ready)
	s.embedRelationships(ctx, rs)
}

// IndexRelationship indexes or re-indexes a relationship for relationship
// search; a no-op when it is off. During a build it is ordered with the build
// sequence (queueBuildWrite).
func (s *Service) IndexRelationship(edge *storage.Edge) {
	if rs := s.relationshipSearch(); rs != nil && edge != nil && edge.ID != "" {
		s.orderRelationshipUpdate(deferredIndexWrite{edge: edge, relationship: true}, func() {
			s.indexRelationship(rs, edge, nil, false)
		})
	}
}

// updateRelationshipNow applies a relationship-search update the build
// sequence ordered.
func (s *Service) updateRelationshipNow(write deferredIndexWrite) {
	rs := s.relationshipSearch()
	switch {
	case rs == nil:
	case write.edge != nil:
		s.indexRelationship(rs, write.edge, nil, false)
	default:
		s.removeRelationship(rs, storage.EdgeID(write.edgeID))
	}
}

func (s *Service) indexRelationship(rs *relationshipSearch, edge *storage.Edge, persisted *relationshipEntry, fromLoader bool) {
	id := string(edge.ID)
	text := s.relationshipSearchableText(edge)
	source := rs.source(edge)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if fromLoader {
		if _, live := rs.live[id]; live {
			return
		}
	} else if rs.loading {
		rs.live[id] = struct{}{}
	}
	if text == "" {
		rs.fulltext.Remove(id)
	} else {
		rs.fulltext.Index(id, text)
	}
	entry := rs.entries[id]
	if entry == nil && persisted != nil && persisted.Source == source {
		entry = &relationshipEntry{Source: source, Vectors: persisted.Vectors, Spans: persisted.Spans}
		rs.entries[id] = entry
	}
	if entry != nil && entry.Source == source {
		return
	}
	rs.entries[id] = &relationshipEntry{Source: source}
	if rs.embed != nil {
		rs.pending[id] = storage.CopyEdge(edge)
		select {
		case rs.wake <- struct{}{}:
		default:
		}
	}
}

// WakeRelationshipEmbedding has relationship search embed its pending
// relationships now, as when an embedder has just been loaded, instead of
// after a failed attempt's retry delay; a no-op when it is off.
func (s *Service) WakeRelationshipEmbedding() {
	if rs := s.relationshipSearch(); rs != nil {
		select {
		case rs.wake <- struct{}{}:
		default:
		}
	}
}

// RemoveRelationship drops a relationship from relationship search; a no-op
// when it is off. During a build it is ordered with the build
// sequence (queueBuildWrite).
func (s *Service) RemoveRelationship(id storage.EdgeID) {
	rs := s.relationshipSearch()
	if rs == nil {
		return
	}
	s.orderRelationshipUpdate(deferredIndexWrite{edgeID: string(id), relationship: true}, func() {
		s.removeRelationship(rs, id)
	})
}

func (s *Service) removeRelationship(rs *relationshipSearch, id storage.EdgeID) {
	rs.mu.Lock()
	if rs.loading {
		rs.live[string(id)] = struct{}{}
	}
	rs.fulltext.Remove(string(id))
	delete(rs.entries, string(id))
	delete(rs.pending, string(id))
	rs.mu.Unlock()
}

// relationshipSource hashes a relationship's type and properties, in key
// order, to tell when its embedding text may have changed.
// SetRelationshipEmbedPolicy records what selects a relationship's
// embedding text (its property lists, whether its type is included), as
// policy; a change of policy changes every relationship's source, so vectors
// saved under the earlier one are embedded again. Set it before the
// relationship index starts.
func (s *Service) SetRelationshipEmbedPolicy(policy string) {
	rs := s.relationshipSearch()
	if rs == nil {
		return
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(policy))
	s.orderWithBuild(func() {
		rs.mu.Lock()
		rs.policy = hash.Sum64()
		rs.mu.Unlock()
	})
}

// source is edge's source under the embedding policy.
func (rs *relationshipSearch) source(edge *storage.Edge) uint64 {
	rs.mu.RLock()
	policy := rs.policy
	rs.mu.RUnlock()
	if policy == 0 {
		return relationshipSource(edge)
	}
	return relationshipSource(edge) ^ policy
}

func relationshipSource(edge *storage.Edge) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(edge.Type))
	keys := make([]string, 0, len(edge.Properties))
	for key := range edge.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(key))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(propertyToString(edge.Properties[key])))
	}
	return hash.Sum64()
}

// embedRelationships embeds pending relationships until ctx ends, saving the
// vectors after each batch.
func (s *Service) embedRelationships(ctx context.Context, rs *relationshipSearch) {
	if rs.embed == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-rs.wake:
		}
		_, failed := s.embedPendingRelationships(ctx, rs)
		if failed {
			// Retried after the delay, or sooner when woken: an embedder
			// was loaded (WakeRelationshipEmbedding) or there is new work.
			select {
			case <-ctx.Done():
				return
			case <-time.After(relationshipEmbedRetryDelay):
			case <-rs.wake:
			}
			select {
			case rs.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Service) embedPendingRelationships(ctx context.Context, rs *relationshipSearch) (changed, failed bool) {
	rs.embedMu.Lock()
	defer rs.embedMu.Unlock()
	// Persist a completed batch even when cancellation or a provider failure
	// ends its drain. No vector/count/search reader sees it before the save.
	defer func() {
		if len(rs.staged) == 0 {
			return
		}
		rs.saveErr = s.saveRelationshipVectors(rs)
		if rs.saveErr != nil {
			failed = true
			return
		}
		rs.mu.Lock()
		for id, embedded := range rs.staged {
			if current := rs.entries[id]; current != nil && current.Source == embedded.Source {
				current.Vectors = embedded.Vectors
				current.Spans = embedded.Spans
				changed = true
			}
			delete(rs.staged, id)
		}
		rs.mu.Unlock()
	}()
	// A batch must finish even while writes keep arriving, otherwise its
	// staged results would never reach persistence or become searchable.
	// New writes leave a wake queued for the next batch.
	rs.mu.RLock()
	batchSize := len(rs.pending)
	rs.mu.RUnlock()
	for processed := 0; processed < batchSize && ctx.Err() == nil; processed++ {
		rs.mu.Lock()
		var id string
		var reported *storage.Edge
		for pendingID, pendingEdge := range rs.pending {
			id, reported = pendingID, pendingEdge
			break
		}
		delete(rs.pending, id)
		rs.mu.Unlock()
		if id == "" {
			return changed, false
		}
		if staged, ok := rs.staged[id]; ok && reported != nil && staged.Source == rs.source(reported) {
			continue
		}
		// The copy the index took is the version its entry stands for: it
		// is embedded even when a read of storage would still give an older
		// version (the write not visible yet, or a stale cached copy), whose
		// vectors the entry would refuse. Storage is read only without a
		// copy that matches; a delete removed the entry and its copy, so a
		// relationship that is gone isn't embedded.
		rs.mu.RLock()
		entry := rs.entries[id]
		rs.mu.RUnlock()
		edge := reported
		if reported == nil || entry == nil || rs.source(reported) != entry.Source {
			stored, err := s.engine.GetEdge(storage.EdgeID(id))
			if err != nil {
				if reported == nil {
					continue
				}
			} else {
				edge = stored
			}
		}
		embedding, err := rs.embed(ctx, edge)
		vectors := embedding.Vectors
		if err != nil {
			rs.mu.Lock()
			if _, indexed := rs.entries[id]; indexed {
				rs.pending[id] = reported
			}
			rs.mu.Unlock()
			return changed, true
		}
		normalized := make([][]float32, 0, len(vectors))
		for _, vector := range vectors {
			if len(vector) > 0 {
				normalized = append(normalized, normalizeVectorCopy(vector))
			}
		}
		// Spans name chunks by index: they hold only while every chunk kept
		// its vector.
		var spans []embeddingutil.ChunkSpan
		if len(normalized) == len(vectors) {
			spans = embedding.Spans
		}
		source := rs.source(edge)
		rs.mu.Lock()
		current := rs.entries[id]
		if current != nil && current.Source == source {
			rs.staged[id] = relationshipEntry{Source: source, Vectors: normalized, Spans: spans}
		}
		rs.mu.Unlock()
		if current != nil && current.Source != source {
			// A later write re-indexed it; its own pending copy is embedded.
			s.logPrintf("Relationship search: embedding of %s superseded by a later write", id)
		}
	}
	return changed, false
}

func normalizeVectorCopy(vector []float32) []float32 {
	var sum float64
	for _, value := range vector {
		sum += float64(value) * float64(value)
	}
	out := make([]float32, len(vector))
	if sum == 0 {
		return out
	}
	norm := math.Sqrt(sum)
	for index, value := range vector {
		out[index] = float32(float64(value) / norm)
	}
	return out
}

func (s *Service) relationshipVectorsPath() (string, error) {
	if s == nil || !s.persistEnabled.Load() || s.fulltextIndexPath == "" {
		return "", nil
	}
	return security.AbsCanonicalFilePath(s.fulltextIndexPath + relationshipVectorsFileSuffix)
}

func (s *Service) loadRelationshipVectors() map[string]*relationshipEntry {
	path, err := s.relationshipVectorsPath()
	if err != nil || path == "" {
		return nil
	}
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil
	}
	defer file.Close()
	var stored relationshipVectorsFile
	if err := gob.NewDecoder(file).Decode(&stored); err != nil || stored.Format != relationshipVectorsFormat {
		return nil
	}
	return stored.Entries
}

func (s *Service) saveRelationshipVectors(rs *relationshipSearch) error {
	path, err := s.relationshipVectorsPath()
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	rs.mu.RLock()
	entries := make(map[string]*relationshipEntry, len(rs.entries))
	for id, entry := range rs.entries {
		if staged, ok := rs.staged[id]; ok && staged.Source == entry.Source {
			entry = &staged
		}
		if len(entry.Vectors) > 0 {
			// Live writes replace entries rather than mutate their vectors;
			// the embedding owner cannot publish again until this save ends.
			// Reuse these stable records instead of copying each one twice.
			entries[id] = entry
		}
	}
	rs.mu.RUnlock()
	if rs.beforeSave != nil {
		rs.beforeSave()
	}
	temp := path + ".tmp"
	file, err := security.CreateRootedFile(temp, 0o600)
	if err != nil {
		s.logPrintf("⚠️ Relationship search: saving vectors to %s failed: %v", path, err)
		return err
	}
	err = gob.NewEncoder(file).Encode(relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: entries})
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = security.RenameRootedFile(temp, path)
	}
	if err == nil {
		// The file sync orders its contents; the directory sync makes the
		// replacement name durable before a reader is told it is complete.
		err = rs.syncDirectory(filepath.Dir(path))
	}
	if err != nil {
		_ = security.RemoveRootedPath(temp)
		s.logPrintf("⚠️ Relationship search: saving vectors to %s failed: %v", path, err)
	}
	return err
}

// joinRelationshipIndex pairs startup with Close, including a startup racing
// cancellation. The worker owns both embedding and its synchronous saves.
func (s *Service) joinRelationshipIndex() error {
	if rs := s.relationshipSearch(); rs != nil {
		rs.workerMu.Lock()
		done := rs.done
		started := rs.workerStarted
		rs.workerMu.Unlock()
		if started {
			<-done
			return rs.saveErr
		}
	}
	return nil
}

type relationshipCandidate struct {
	id         string
	rrf        float64
	similarity float64
	vectorRank int
	bm25Rank   int
}

// SearchRelationships ranks relationships by keyword and, with an
// embedding, vector similarity, fused by reciprocal rank as node search is.
// Each result names the relationship, its type and both endpoints, with its
// properties. Types filters by relationship type.
func (s *Service) SearchRelationships(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	rs := s.relationshipSearch()
	if rs == nil {
		return nil, localizedError(localization.SearchRelationshipsDisabled(), ErrRelationshipSearchDisabled)
	}
	if opts == nil {
		opts = DefaultSearchOptions()
	}
	select {
	case <-rs.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	depth := max(limit*5, 50)
	if opts.RelationshipWhere != nil {
		// A filter can reject any number of the best candidates: rank every
		// relationship, so the admitted ones aren't cut behind them.
		rs.mu.RLock()
		depth = max(depth, len(rs.entries))
		rs.mu.RUnlock()
	}
	rrfK := opts.RRFK
	if rrfK <= 0 {
		rrfK = 60
	}
	vectorWeight, bm25Weight := opts.VectorWeight, opts.BM25Weight
	if vectorWeight <= 0 {
		vectorWeight = 1
	}
	if bm25Weight <= 0 {
		bm25Weight = 1
	}
	candidates := make(map[string]*relationshipCandidate)
	candidate := func(id string) *relationshipCandidate {
		if existing := candidates[id]; existing != nil {
			return existing
		}
		created := &relationshipCandidate{id: id}
		candidates[id] = created
		return created
	}

	rs.mu.RLock()
	bm25 := rs.fulltext.Search(query, depth)
	type scored struct {
		id    string
		score float64
	}
	var vectorHits []scored
	// chunkScores are each vector hit's chunk similarities, for its matches.
	var chunkScores map[string][]float64
	var chunkSpans map[string][]embeddingutil.ChunkSpan
	if len(embedding) > 0 {
		normalizedQuery := normalizeVectorCopy(embedding)
		for id, entry := range rs.entries {
			best, found := -2.0, false
			var scores []float64
			for _, vector := range entry.Vectors {
				if len(vector) != len(normalizedQuery) {
					scores = append(scores, -2)
					continue
				}
				var dot float64
				for index, value := range vector {
					dot += float64(value) * float64(normalizedQuery[index])
				}
				scores = append(scores, dot)
				if dot > best {
					best, found = dot, true
				}
			}
			if found {
				vectorHits = append(vectorHits, scored{id: id, score: best})
				if !opts.SkipMatches {
					if chunkScores == nil {
						chunkScores = make(map[string][]float64)
						chunkSpans = make(map[string][]embeddingutil.ChunkSpan)
					}
					chunkScores[id] = scores
					chunkSpans[id] = entry.Spans
				}
			}
		}
	}
	var terms map[string]*queryTerm
	analyzer := normalizeAnalyzer(s.bm25Analyzer)
	if query != "" && !opts.SkipMatches {
		terms = matchQueryTerms(analyzer, query, rs.fulltext)
	}
	rs.mu.RUnlock()

	for rank, hit := range bm25 {
		c := candidate(hit.ID)
		c.bm25Rank = rank + 1
		c.rrf += bm25Weight / (rrfK + float64(rank+1))
	}
	minSimilarity := -1.0
	if threshold := s.resolveMinSimilarity(opts); threshold != nil {
		minSimilarity = *threshold
	}
	sort.Slice(vectorHits, func(i, j int) bool {
		if vectorHits[i].score != vectorHits[j].score {
			return vectorHits[i].score > vectorHits[j].score
		}
		return vectorHits[i].id < vectorHits[j].id
	})
	rank := 0
	for _, hit := range vectorHits {
		if hit.score < minSimilarity || rank >= depth {
			continue
		}
		rank++
		c := candidate(hit.id)
		c.vectorRank = rank
		c.similarity = hit.score
		c.rrf += vectorWeight / (rrfK + float64(rank))
	}

	ordered := make([]*relationshipCandidate, 0, len(candidates))
	for _, c := range candidates {
		ordered = append(ordered, c)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].rrf != ordered[j].rrf {
			return ordered[i].rrf > ordered[j].rrf
		}
		return ordered[i].id < ordered[j].id
	})
	types := make(map[string]bool, len(opts.Types))
	for _, relType := range opts.Types {
		types[relType] = true
	}
	projection := newResultPropertyProjection(opts.IncludeProperties, opts.ExcludeProperties)
	type admittedRelationship struct {
		edge      *storage.Edge
		candidate *relationshipCandidate
	}
	admitted := make([]admittedRelationship, 0, min(limit, len(ordered)))
	for _, c := range ordered {
		if len(admitted) >= limit {
			break
		}
		edge, err := s.engine.GetEdge(storage.EdgeID(c.id))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			} // deleted, or hidden from reads
			return nil, searchReadFailure(err)
		}
		if len(types) > 0 && !types[edge.Type] {
			continue
		}
		if opts.RelationshipWhere != nil {
			start, err := s.engine.GetNode(edge.StartNode)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, searchReadFailure(err)
			}
			end, err := s.engine.GetNode(edge.EndNode)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, searchReadFailure(err)
			}
			if !opts.RelationshipWhere(edge, start, end) {
				continue
			}
		}
		admitted = append(admitted, admittedRelationship{edge, c})
	}
	ctx, visible, release := storage.BeginQueryResultProjectionContext(ctx, s.engine)
	defer release()
	results := make([]SearchResult, 0, len(admitted))
	for _, hit := range admitted {
		c, edge := hit.candidate, visible.Edge(hit.edge)
		properties := projection.apply(edge.Properties)
		result := SearchResult{
			ID:         string(edge.ID),
			Properties: properties,
			Score:      c.rrf,
			Similarity: c.similarity,
			RRFScore:   c.rrf,
			VectorRank: c.vectorRank,
			BM25Rank:   c.bm25Rank,
			Relationship: &storage.Edge{
				ID:         edge.ID,
				Database:   edge.Database,
				Type:       edge.Type,
				StartNode:  edge.StartNode,
				EndNode:    edge.EndNode,
				Properties: properties,
			},
		}
		if title, ok := edge.Properties["title"].(string); ok {
			result.Title = title
		}
		if description, ok := edge.Properties["description"].(string); ok {
			result.Description = description
		}
		if content, ok := edge.Properties["content"].(string); ok {
			result.ContentPreview = truncate(content, 200)
		} else if text, ok := edge.Properties["text"].(string); ok {
			result.ContentPreview = truncate(text, 200)
		}
		// Matches, as a node hit's: the best chunks of a vector hit, then
		// the query terms in the properties the keyword index reads.
		if c.vectorRank > 0 {
			result.Matches = relationshipChunkMatches(chunkScores[c.id], chunkSpans[c.id], minSimilarity)
		}
		if c.bm25Rank > 0 && len(terms) > 0 {
			result.Matches = append(result.Matches, termMatches(&storage.Node{Properties: edge.Properties}, s.RelationshipFulltextProperties(), analyzer, terms)...)
		}
		if visible.Enabled() {
			result.Matches = visibleSearchMatches(result.Matches, edge.Properties)
		}
		results = append(results, result)
	}
	method := "relationship_bm25"
	switch {
	case len(embedding) > 0 && len(bm25) > 0:
		method = "relationship_rrf_hybrid"
	case len(embedding) > 0:
		method = "relationship_vector"
	}
	return &SearchResponse{
		Status:          "success",
		Query:           query,
		Results:         results,
		TotalCandidates: len(candidates),
		Returned:        len(results),
		SearchMethod:    method,
	}, nil
}
