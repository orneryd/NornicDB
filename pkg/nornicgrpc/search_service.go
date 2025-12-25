package nornicgrpc

import (
	"context"
	"time"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// EmbedQueryFunc embeds a query string into a vector.
// Returning (nil, nil) is treated as "embeddings unavailable".
type EmbedQueryFunc func(ctx context.Context, query string) ([]float32, error)

// Searcher is the minimal interface this service needs from the search layer.
type Searcher interface {
	Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error)
}

// Service implements the NornicDB-native gRPC search API.
type Service struct {
	gen.UnimplementedNornicSearchServer

	defaultDatabase string
	maxLimit        int

	embedQuery EmbedQueryFunc
	searcher   Searcher
}

type Config struct {
	DefaultDatabase string
	MaxLimit        int
}

// NewService creates a NornicDB-native search service.
func NewService(cfg Config, embedQuery EmbedQueryFunc, searcher Searcher) (*Service, error) {
	if searcher == nil {
		return nil, status.Error(codes.InvalidArgument, "searcher is required")
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = 1000
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "nornic"
	}
	return &Service{
		defaultDatabase: cfg.DefaultDatabase,
		maxLimit:        cfg.MaxLimit,
		embedQuery:      embedQuery,
		searcher:        searcher,
	}, nil
}

func (s *Service) SearchText(ctx context.Context, req *gen.SearchTextRequest) (*gen.SearchTextResponse, error) {
	start := time.Now()

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Query == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
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
	if len(req.Labels) > 0 {
		opts.Types = req.Labels
	}
	if req.MinSimilarity != nil {
		v := float64(*req.MinSimilarity)
		opts.MinSimilarity = &v
	}

	var embedding []float32
	if s.embedQuery != nil {
		emb, err := s.embedQuery(ctx, req.Query)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "embed query: %v", err)
		}
		embedding = emb
	}

	resp, err := s.searcher.Search(ctx, req.Query, embedding, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "search: %v", err)
	}

	out := make([]*gen.SearchHit, 0, len(resp.Results))
	for _, r := range resp.Results {
		props, _ := structpb.NewStruct(r.Properties)
		out = append(out, &gen.SearchHit{
			NodeId:     string(r.NodeID),
			Labels:     r.Labels,
			Properties: props,
			Score:      float32(r.Score),
			RrfScore:   float32(r.RRFScore),
			VectorRank: int32(r.VectorRank),
			Bm25Rank:   int32(r.BM25Rank),
		})
	}

	return &gen.SearchTextResponse{
		SearchMethod:      resp.SearchMethod,
		Hits:              out,
		FallbackTriggered: resp.FallbackTriggered,
		Message:           resp.Message,
		TimeSeconds:       time.Since(start).Seconds(),
	}, nil
}
