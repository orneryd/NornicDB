package search

import (
	"context"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/auth"
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

func (scope *indexedSearchScope) nodeBM25(ctx context.Context, index bm25Index) (bm25Index, error) {
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
		return checkContinuationGraphVersion(scope.versions, scope.version)
	}
	return nil
}

func (s *Service) searchReadEngine(ctx context.Context) storage.Engine {
	if scope := s.indexedScope(ctx); scope != nil {
		return scope.engine
	}
	return s.engine
}

func (s *Service) filterIndexedVectors(ctx context.Context, candidates []indexResult) ([]indexResult, error) {
	scope := s.indexedScope(ctx)
	if scope == nil {
		return candidates, nil
	}
	admitted := candidates[:0]
	for _, candidate := range candidates {
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
