package nornicgrpc

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"golang.org/x/text/language"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EmbedQueryFunc embeds a query string into a vector.
// Returning (nil, nil) is treated as "embeddings unavailable".
type EmbedQueryFunc func(ctx context.Context, query string) ([]float32, error)

// ChunkQueryFunc splits a query string into embedder-safe chunks.
type ChunkQueryFunc func(ctx context.Context, query string) ([]string, error)

// Searcher is the minimal interface this service needs from the search layer.
type Searcher interface {
	Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error)
}

type continuationSearcher interface {
	SearchTextContinuation(context.Context, string, *search.SearchOptions, search.SearchContinuationRequest, search.ChunkQueryFunc, search.EmbedQueryFunc, search.SearchQueryFunc, search.ChunkedSearchErrorPolicy) (*search.SearchContinuationPage, error)
}

// DatabaseDependencies binds native search operations to one canonical database.
type DatabaseDependencies struct {
	Searcher   Searcher
	EmbedQuery EmbedQueryFunc
	ChunkQuery ChunkQueryFunc
}

// Service implements the NornicDB-native gRPC search API.
type Service struct {
	gen.UnimplementedNornicSearchServer

	defaultDatabase string
	maxLimit        int
	rerankEnabled   bool

	embedQuery       EmbedQueryFunc
	chunkQuery       ChunkQueryFunc
	searcher         Searcher
	localizer        *localization.Manager
	ownerFromContext func(context.Context) string
	allowDatabase    func(context.Context, string) error
	resolveDatabase  func(context.Context, string) (DatabaseDependencies, error)
	resolveQID       func(owner, qid, requestedDatabase string) (string, error)
}

type Config struct {
	DefaultDatabase string
	MaxLimit        int
	// RerankEnabled enables Stage-2 reranking for search when a reranker is configured.
	RerankEnabled bool
	// Localizer renders human-readable status errors. Nil uses en-US.
	Localizer *localization.Manager
	// OwnerFromContext returns a trusted authenticated principal identifier.
	OwnerFromContext func(context.Context) string
	// AllowDatabase enforces read access before search dependencies are resolved.
	AllowDatabase func(context.Context, string) error
	// ResolveDatabase returns search dependencies bound to a canonical database.
	ResolveDatabase func(context.Context, string) (DatabaseDependencies, error)
	// ResolveContinuationDatabase validates a qid and returns its database binding.
	ResolveContinuationDatabase func(owner, qid, requestedDatabase string) (string, error)
}

// NewService creates a NornicDB-native search service.
func NewService(cfg Config, embedQuery EmbedQueryFunc, chunkQuery ChunkQueryFunc, searcher Searcher) (*Service, error) {
	if cfg.Localizer == nil {
		var err error
		cfg.Localizer, err = localization.NewManager(nil, nil)
		if err != nil {
			return nil, status.Error(codes.Internal, localization.GRPCLocalizationInitializationFailed(err).Fallback)
		}
	}
	if searcher == nil {
		service := &Service{localizer: cfg.Localizer}
		return nil, service.localizedStatus(context.Background(), codes.InvalidArgument, localization.SearcherRequired())
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = 1000
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "nornic"
	}
	return &Service{
		defaultDatabase:  cfg.DefaultDatabase,
		maxLimit:         cfg.MaxLimit,
		rerankEnabled:    cfg.RerankEnabled,
		embedQuery:       embedQuery,
		chunkQuery:       chunkQuery,
		searcher:         searcher,
		localizer:        cfg.Localizer,
		ownerFromContext: cfg.OwnerFromContext,
		allowDatabase:    cfg.AllowDatabase,
		resolveDatabase:  cfg.ResolveDatabase,
		resolveQID:       cfg.ResolveContinuationDatabase,
	}, nil
}

func (s *Service) SearchText(ctx context.Context, req *gen.SearchTextRequest) (*gen.SearchTextResponse, error) {
	start := time.Now()

	if req == nil {
		return nil, s.localizedStatus(ctx, codes.InvalidArgument, localization.RequestRequired())
	}
	continuationRequested := req.N > 0 || req.Qid != "" || req.Discard || req.Mode != "" || req.GroupBy != "" || req.RankedLimit != nil
	if req.Query == "" && req.Qid == "" && req.Mode != string(search.SearchContinuationID) {
		return nil, s.localizedStatus(ctx, codes.InvalidArgument, localization.QueryRequired())
	}
	owner := "anonymous"
	if s.ownerFromContext != nil {
		owner = s.ownerFromContext(ctx)
	}
	database := req.Database
	if req.Qid != "" && s.resolveQID != nil {
		var err error
		database, err = s.resolveQID(owner, req.Qid, database)
		if err != nil {
			return nil, s.continuationStatus(ctx, err)
		}
	}
	if database == "" {
		database = s.defaultDatabase
	}
	if s.allowDatabase != nil {
		if err := s.allowDatabase(ctx, database); err != nil {
			return nil, err
		}
	}
	dependencies := DatabaseDependencies{Searcher: s.searcher, EmbedQuery: s.embedQuery, ChunkQuery: s.chunkQuery}
	if s.resolveDatabase != nil {
		var err error
		dependencies, err = s.resolveDatabase(ctx, database)
		if err != nil {
			return nil, s.localizedStatus(ctx, codes.NotFound, localization.SearchFailed(err))
		}
	} else if database != s.defaultDatabase {
		return nil, s.localizedStatus(ctx, codes.InvalidArgument, localization.SearchFailed(errors.New("database-specific search is unavailable")))
	}
	if dependencies.Searcher == nil {
		return nil, s.localizedStatus(ctx, codes.Unavailable, localization.SearcherRequired())
	}
	if continuationRequested {
		continuable, ok := dependencies.Searcher.(continuationSearcher)
		if !ok {
			return nil, status.Error(codes.Unimplemented, "search continuation is unavailable")
		}
		n := int(req.N)
		if req.Qid != "" && n == 0 {
			n = 50
		}
		continuation := search.SearchContinuationRequest{
			Owner: owner, Database: database, QID: req.Qid, N: n, Discard: req.Discard,
			Mode: search.SearchContinuationMode(req.Mode), GroupBy: req.GroupBy,
		}
		if req.MaxResults != nil {
			continuation.MaxResults = int(*req.MaxResults)
		}
		if req.RankedLimit != nil {
			continuation.RankedLimit = int(*req.RankedLimit)
		}
		page, err := continuable.SearchTextContinuation(
			ctx, req.Query, searchOptions(req, s.maxLimit, s.rerankEnabled), continuation,
			search.ChunkQueryFunc(dependencies.ChunkQuery), search.EmbedQueryFunc(dependencies.EmbedQuery), dependencies.Searcher.Search,
			search.ChunkedSearchErrorPolicy{Transport: "grpc"},
		)
		if err != nil {
			return nil, s.continuationStatus(ctx, err)
		}
		if provider, ok := any(page.SearchResponse()).(interface{ ResponseTrailers() map[string]string }); ok {
			for key, value := range provider.ResponseTrailers() {
				grpc.SetTrailer(ctx, metadata.Pairs(key, value))
			}
		}
		return grpcContinuationResponse(page, time.Since(start)), nil
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}
	if limit > s.maxLimit {
		limit = s.maxLimit
	}

	opts := search.DefaultSearchOptions()
	opts.Limit = limit
	opts.RerankEnabled = s.rerankEnabled
	if len(req.Labels) > 0 {
		opts.Types = req.Labels
	}
	if req.MinSimilarity != nil {
		v := float64(*req.MinSimilarity)
		opts.MinSimilarity = &v
	}

	chunkQuery := search.ChunkQueryFunc(nil)
	if dependencies.ChunkQuery != nil {
		chunkQuery = func(ctx context.Context, query string) ([]string, error) {
			chunks, err := dependencies.ChunkQuery(ctx, query)
			if err != nil {
				return nil, s.localizedStatus(ctx, codes.InvalidArgument, localization.QueryChunkFailed(err))
			}
			return chunks, nil
		}
	}
	resp, err := search.SearchTextChunksWithErrorPolicy(
		ctx, req.Query, opts, chunkQuery, search.EmbedQueryFunc(dependencies.EmbedQuery), dependencies.Searcher.Search,
		search.ChunkedSearchErrorPolicy{Transport: "grpc"},
	)
	if err != nil {
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return nil, s.localizedStatus(ctx, codes.Internal, localization.SearchFailed(err))
	}

	out := make([]*gen.SearchHit, 0, len(resp.Results))
	for _, r := range resp.Results {
		out = append(out, grpcSearchHit(r))
	}

	return &gen.SearchTextResponse{
		SearchMethod:      resp.SearchMethod,
		Hits:              out,
		FallbackTriggered: resp.FallbackTriggered,
		FallbackReason:    string(resp.FallbackReason),
		Message:           resp.Message,
		TimeSeconds:       time.Since(start).Seconds(),
	}, nil
}

func (s *Service) continuationStatus(ctx context.Context, err error) error {
	code := codes.InvalidArgument
	switch {
	case errors.Is(err, resultstream.ErrDisabled):
		code = codes.FailedPrecondition
	case errors.Is(err, resultstream.ErrCapacity):
		code = codes.ResourceExhausted
	case errors.Is(err, resultstream.ErrExpiredQID), errors.Is(err, resultstream.ErrGoneQID), errors.Is(err, resultstream.ErrInvalidated):
		code = codes.NotFound
	case errors.Is(err, resultstream.ErrClosed):
		code = codes.Unavailable
	}
	return s.localizedStatus(ctx, code, localization.SearchFailed(err))
}

func searchOptions(req *gen.SearchTextRequest, maxLimit int, rerank bool) *search.SearchOptions {
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	opts := search.DefaultSearchOptions()
	opts.Limit = limit
	opts.RerankEnabled = rerank
	opts.Types = append([]string(nil), req.Labels...)
	if req.MinSimilarity != nil {
		value := float64(*req.MinSimilarity)
		opts.MinSimilarity = &value
	}
	return opts
}

func grpcContinuationResponse(page *search.SearchContinuationPage, elapsed time.Duration) *gen.SearchTextResponse {
	response := &gen.SearchTextResponse{
		SearchMethod: page.SearchMethod, FallbackTriggered: page.FallbackTriggered,
		FallbackReason: string(page.FallbackReason),
		Qid:            page.QID, HasMore: page.HasMore, Position: page.Position,
		Returned: uint32(page.Returned), Released: page.Released, TimeSeconds: elapsed.Seconds(),
		Mode: string(page.Mode), RankedCount: int64(page.RankedCount),
		RankedPoolExhausted: page.RankedPoolExhausted, CollectionExhausted: page.CollectionExhausted,
		Completion: page.Completion,
	}
	if page.Total != nil {
		total := *page.Total
		response.Total = &total
	}
	if !page.ExpiresAt.IsZero() {
		response.ExpiresAt = timestamppb.New(page.ExpiresAt)
	}
	if page.EligibleCount != nil {
		eligibleCount := int64(*page.EligibleCount)
		response.EligibleCount = &eligibleCount
	}
	response.Hits = make([]*gen.SearchHit, 0, len(page.Results))
	for index := range page.Results {
		response.Hits = append(response.Hits, grpcSearchHit(page.Results[index]))
	}
	return response
}

func grpcSearchHit(r search.SearchResult) *gen.SearchHit {
	props, _ := structpb.NewStruct(r.Properties)
	return &gen.SearchHit{
		NodeId: string(r.NodeID), Labels: r.Labels, Properties: props,
		Score: float32(r.Score), RrfScore: float32(r.RRFScore),
		VectorRank: int32(r.VectorRank), Bm25Rank: int32(r.BM25Rank),
		Phase: r.Phase, GroupKey: r.GroupKey, Passages: grpcSearchPassages(r.Passages),
	}
}

func grpcSearchPassages(passages []search.SearchPassage) []*gen.SearchHit {
	out := make([]*gen.SearchHit, len(passages))
	for index, passage := range passages {
		out[index] = grpcSearchHit(passage)
	}
	return out
}

func (s *Service) localizedStatus(ctx context.Context, code codes.Code, message localization.Message) error {
	preferences := []language.Tag(nil)
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		values := incoming.Get("accept-language")
		if len(values) > 0 {
			preferences, _, _ = language.ParseAcceptLanguage(strings.Join(values, ","))
		}
	}
	if len(preferences) > 0 {
		match := s.localizer.Resolve("grpc", preferences...)
		ctx = localization.WithPreferences(ctx, match.Tag)
	}
	text, _, err := s.localizer.Render(ctx, message)
	if err != nil {
		text = message.Fallback
	}
	return status.Error(code, text)
}
