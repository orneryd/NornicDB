package search

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (s *Service) fineGrainedSearchEngine(request *auth.FineGrainedRequest) (storage.Engine, error) {
	if request == nil {
		return s.engine, nil
	}
	database := storage.DatabaseOf(s.engine)
	if database == "" {
		database = s.continuationDatabase()
	}
	if !request.CanAccessDatabase(database) || storage.CompositeDatabaseOf(s.engine) != "" {
		return nil, auth.ErrFineGrainedForbidden
	}
	return request.Graph(s.engine, database), nil
}

// indexedSearchScope keeps authorization and the graph version local to a
// query. The service's indexes and lifecycle are never copied.
type indexedSearchScope struct {
	service   *Service
	principal *auth.FineGrainedRequest
	engine    storage.Engine
	versions  graphMutationVersionProvider
	version   uint64
	nodeView  atomic.Pointer[visibleFieldCorpus]
	edgeView  atomic.Pointer[visibleFieldCorpus]
}

type indexedSearchScopeKey struct{}
type visibleNodeBM25Key struct{}

func (s *Service) indexedScope(ctx context.Context) *indexedSearchScope {
	scope, _ := ctx.Value(indexedSearchScopeKey{}).(*indexedSearchScope)
	if scope != nil && scope.service == s && scope.principal == auth.FineGrainedRequestFromContext(ctx) {
		return scope
	}
	return nil
}

func (s *Service) withIndexedSearchScope(ctx context.Context) (context.Context, *indexedSearchScope, error) {
	if scope := s.indexedScope(ctx); scope != nil {
		return ctx, scope, nil
	}
	request := auth.FineGrainedRequestFromContext(ctx)
	if request == nil {
		return ctx, nil, nil
	}
	engine, err := s.fineGrainedSearchEngine(request)
	if err != nil {
		return ctx, nil, err
	}
	scope := &indexedSearchScope{service: s, principal: request, engine: engine}
	if versions, ok := engine.(graphMutationVersionProvider); ok {
		if version, supported := versions.GraphMutationVersion(); supported {
			scope.versions, scope.version = versions, version
		}
	}
	return context.WithValue(ctx, indexedSearchScopeKey{}, scope), scope, nil
}

// keywordRestricted determines whether shared text statistics are readable in this database.
func (scope *indexedSearchScope) keywordRestricted(relationships bool) bool {
	s := scope.service
	request := scope.principal
	database := storage.DatabaseOf(s.engine)
	if database == "" {
		database = s.continuationDatabase()
	}
	id := request.DatabaseID(database)
	allows := func(action string) bool {
		return request.Access.AllowsEntireGraphAction(action, id, request.HomeDatabaseID)
	}
	if !allows("graph.node.traverse") {
		return true
	}
	if relationships {
		return !allows("graph.relationship.traverse") || !allows("graph.relationship.property.read")
	}
	return !allows("graph.node.property.read")
}

// visibleBM25 projects the shared index population into request-local scoring
// state. No postings, vocabulary, or statistics from unreadable text enter it.
func visibleBM25(ctx context.Context, index bm25Index, read func(context.Context, []string) ([]FulltextBatchEntry, error)) (bm25Index, error) {
	shared, ok := index.(*FulltextIndexV2)
	if !ok {
		if _, disabled := index.(disabledBM25Index); disabled || index == nil {
			return index, nil
		}
		return nil, fmt.Errorf("search: cannot project restricted full-text index %T", index)
	}
	shared.mu.RLock()
	ids := append([]string(nil), shared.docNumToID...)
	analyzer, maxExpansions, minPrefix := shared.analyzer, shared.maxPrefixExpansions, shared.minPrefixLength
	shared.mu.RUnlock()
	view := NewFulltextIndexV2WithAnalyzer(analyzer)
	view.maxPrefixExpansions, view.minPrefixLength = maxExpansions, minPrefix
	batch := make([]FulltextBatchEntry, 0, min(len(ids), fulltextBuildBatchSize))
	// Bound authorized storage reads while sharing each batch's read capture.
	const readBatchSize = 256
	for start := 0; start < len(ids); start += readBatchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+readBatchSize, len(ids))
		entries, err := read(ctx, ids[start:end])
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.ID == "" || entry.Text == "" {
				continue
			}
			batch = append(batch, entry)
			if len(batch) == cap(batch) {
				view.IndexBatch(batch)
				clear(batch)
				batch = batch[:0]
			}
		}
	}
	view.IndexBatch(batch)
	return view, nil
}

func (scope *indexedSearchScope) nodeBM25(ctx context.Context, index bm25Index) (bm25Ranker, error) {
	if shared, ok := index.(*FulltextIndexV2); ok {
		view, err := scope.service.visibleCorpus(ctx, scope, shared, false)
		if err == nil {
			scope.nodeView.Store(view)
			return visibleBM25Index{corpus: view}, nil
		}
		if !errors.Is(err, errMissingFieldProvenance) {
			return nil, err
		}
	}
	return visibleBM25(ctx, index, func(readCtx context.Context, ids []string) ([]FulltextBatchEntry, error) {
		nodeIDs := make([]storage.NodeID, 0, len(ids))
		for _, id := range ids {
			if id != "" {
				nodeIDs = append(nodeIDs, storage.NodeID(id))
			}
		}
		nodes, err := scope.engine.BatchGetNodes(readCtx, nodeIDs)
		if err != nil {
			return nil, err
		}
		entries := make([]FulltextBatchEntry, 0, len(nodes))
		for _, id := range ids {
			if node := nodes[storage.NodeID(id)]; node != nil {
				entries = append(entries, FulltextBatchEntry{ID: id, Text: scope.service.extractSearchableText(node)})
			}
		}
		return entries, nil
	})
}

func (scope *indexedSearchScope) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope.versions != nil {
		if err := checkContinuationGraphVersion(scope.versions, scope.version); err != nil {
			return err
		}
	}
	for _, view := range [...](*visibleFieldCorpus){scope.nodeView.Load(), scope.edgeView.Load()} {
		if view == nil {
			continue
		}
		view.index.mu.RLock()
		stable := view.index.version == view.version && !view.stale.Load()
		view.index.mu.RUnlock()
		if !stable {
			return resultstream.ErrInvalidated
		}
	}
	return nil
}

func (s *Service) searchReadEngine(ctx context.Context) storage.Engine {
	if scope := s.indexedScope(ctx); scope != nil {
		return scope.engine
	}
	return s.engine
}

// indexedVectorReadBatchSize bounds the candidate node images one authorized
// vector-candidate read requests at a time.
const indexedVectorReadBatchSize = 256

// filterIndexedVectors admits indexed candidates through the request's
// projected graph. Stable graph and policy revisions reuse bounded admission
// decisions; cold reads batch repeated owners or use point reads when owners
// are mostly distinct. Candidate order and each entry's source visibility are
// preserved. Unreadable and missing nodes are skipped.
func (s *Service) filterIndexedVectors(ctx context.Context, candidates []indexResult) ([]indexResult, error) {
	scope := s.indexedScope(ctx)
	if scope == nil {
		return candidates, nil
	}
	visibility, err := s.vectorVisibilityForScope(scope)
	if err != nil {
		return nil, err
	}
	var candidateKeys []vectorDecisionKey
	if visibility != nil {
		if cached, complete := visibility.lookup(candidates); complete {
			return cached, scope.check(ctx)
		}
		candidateKeys = make([]vectorDecisionKey, len(candidates))
		for i := range candidates {
			candidateKeys[i] = decisionKey(candidates[i])
		}
	}
	admitted := candidates[:0]
	for start := 0; start < len(candidates); start += indexedVectorReadBatchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := candidates[start:min(start+indexedVectorReadBatchSize, len(candidates))]
		// Badger's batch API materializes result maps and projected node images.
		// When almost every candidate has a different owner, point reads cost
		// less memory; batching pays off when several vectors share one node.
		owners := make(map[string]struct{}, len(batch))
		for _, candidate := range batch {
			owners[candidate.ID] = struct{}{}
		}
		if len(owners)*4 > len(batch)*3 {
			for _, candidate := range batch {
				entry, ok := decodeVectorEntryKey(candidate.MatchID)
				if !ok || entry.NodeID != candidate.ID {
					continue
				}
				node, err := scope.engine.GetNode(ctx, storage.NodeID(candidate.ID))
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if err != nil {
					return nil, err
				}
				if indexedVectorVisible(node, entry) {
					admitted = append(admitted, candidate)
				}
			}
			continue
		}
		// entries holds the decoded key of each candidate in batch order; a
		// zero entry names no node and no source, so it is never read.
		entries := make([]vectorEntry, len(batch))
		nodeIDs := make([]storage.NodeID, 0, len(batch))
		clear(owners)
		requested := owners
		for index, candidate := range batch {
			entry, ok := decodeVectorEntryKey(candidate.MatchID)
			if !ok || entry.NodeID != candidate.ID {
				continue
			}
			entries[index] = entry
			if _, seen := requested[candidate.ID]; seen {
				continue
			}
			requested[candidate.ID] = struct{}{}
			nodeIDs = append(nodeIDs, storage.NodeID(candidate.ID))
		}
		if len(nodeIDs) == 0 {
			continue
		}
		nodes, err := scope.engine.BatchGetNodes(ctx, nodeIDs)
		if err != nil {
			return nil, err
		}
		for index, candidate := range batch {
			// A projected batch omits the nodes this request may not see, so a
			// missing image keeps the entry out of the page without disclosing
			// whether the node or its source exists.
			node := nodes[storage.NodeID(candidate.ID)]
			if node != nil && entries[index].Kind != 0 && indexedVectorVisible(node, entries[index]) {
				admitted = append(admitted, candidate)
			}
		}
	}
	if visibility != nil {
		if err := scope.check(ctx); err != nil {
			return nil, err
		}
		visibility.remember(candidateKeys, admitted)
	}
	return admitted, nil
}

func indexedVectorVisible(node *storage.Node, entry vectorEntry) bool {
	switch entry.Kind {
	case vectorEntryNamed:
		return len(node.NamedEmbeddings[entry.Name]) != 0
	case vectorEntryProperty:
		_, ok := node.Properties[entry.Name]
		return ok
	case vectorEntryMain, vectorEntryChunk:
		return entry.Chunk < len(node.ChunkEmbeddings) && len(node.ChunkEmbeddings[entry.Chunk]) != 0
	default:
		return false
	}
}
