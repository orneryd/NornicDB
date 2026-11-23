package search

import (
	"context"
	"errors"

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
