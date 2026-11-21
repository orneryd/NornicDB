package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// fineGrainedView builds a private corpus from the authorized snapshot. Raw
// postings and raw corpus statistics cannot influence projected scores. The
// caller owns this service and never publishes it as the database index.
func (s *Service) fineGrainedView(ctx context.Context, request *auth.FineGrainedRequest) (*Service, error) {
	database := storage.DatabaseOf(s.engine)
	if database == "" {
		database = s.continuationDatabase()
	}
	if !request.CanAccessDatabase(database) || storage.CompositeDatabaseOf(s.engine) != "" {
		return nil, auth.ErrFineGrainedForbidden
	}
	options := DefaultServiceOptions(database)
	options.SearchResultCacheEntries = 0
	options.MemoryPolicy = s.memoryPolicy
	options.VectorStorageMode = "memory"
	dimensions := s.VectorIndexDimensions()
	view := NewService(request.Graph(s.engine, database), dimensions, &options)
	view.fineGrainedOwner = request
	if versioned, ok := view.engine.(graphMutationVersionProvider); ok {
		if version, supported := versioned.GraphMutationVersion(); supported {
			view.fineGrainedVersion, view.fineGrainedVersions = version, versioned
		}
	}
	view.SetIndexFlags(s.BM25Enabled(), s.VectorEnabled())
	view.SetFulltextProperties(s.FulltextProperties())
	view.SetFulltextLabels(s.FulltextLabels())
	view.bm25Analyzer = s.bm25Analyzer
	view.setFulltext(NewFulltextIndexV2WithAnalyzer(s.bm25Analyzer))
	if err := view.BuildIndexes(ctx); err != nil {
		_ = view.Close()
		return nil, err
	}
	if s.RelationshipSearchEnabled() {
		view.EnableRelationshipSearch(nil)
		view.StartRelationshipIndex()
	}
	if view.fineGrainedVersions != nil {
		if err := checkContinuationGraphVersion(view.fineGrainedVersions, view.fineGrainedVersion); err != nil {
			_ = view.Close()
			return nil, err
		}
	}
	return view, nil
}

func (s *Service) requestedFineGrainedView(ctx context.Context) *auth.FineGrainedRequest {
	request := auth.FineGrainedRequestFromContext(ctx)
	if request == nil || request == s.fineGrainedOwner {
		return nil
	}
	return request
}
