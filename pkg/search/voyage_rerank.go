package search

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	voyageapi "github.com/orneryd/nornicdb/pkg/voyage"
)

// VoyageRerankConfig configures the Voyage AI rerank provider.
type VoyageRerankConfig struct {
	Enabled  bool
	APIURL   string
	APIKey   string
	Model    string
	TopK     int
	Timeout  time.Duration
	MinScore float64
}

// DefaultVoyageRerankConfig returns a Voyage reranker configuration with
// provider defaults and API key discovery from VOYAGE_API_KEY.
func DefaultVoyageRerankConfig() *VoyageRerankConfig {
	return &VoyageRerankConfig{
		Enabled: true,
		APIURL:  voyageapi.DefaultBaseURL,
		APIKey:  os.Getenv("VOYAGE_API_KEY"),
		Model:   voyageapi.DefaultRerankModel,
		TopK:    100,
		Timeout: 30 * time.Second,
	}
}

// VoyageReranker reranks search candidates with Voyage AI's native rerank API.
type VoyageReranker struct {
	config *VoyageRerankConfig
	client *voyageapi.Client
}

// NewVoyageReranker creates a Voyage reranker.
func NewVoyageReranker(config *VoyageRerankConfig) (*VoyageReranker, error) {
	if config == nil {
		config = DefaultVoyageRerankConfig()
	}
	cfg := *config
	if strings.TrimSpace(cfg.APIURL) == "" {
		cfg.APIURL = voyageapi.DefaultBaseURL
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		cfg.APIKey = os.Getenv("VOYAGE_API_KEY")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("Voyage rerank requires an API key")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = voyageapi.DefaultRerankModel
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	client, err := voyageapi.NewClient(voyageapi.Config{
		APIKey:  cfg.APIKey,
		BaseURL: cfg.APIURL,
		Timeout: cfg.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return &VoyageReranker{config: &cfg, client: client}, nil
}

func (r *VoyageReranker) Name() string { return "voyage" }

func (r *VoyageReranker) Enabled() bool {
	return r != nil && r.config != nil && r.config.Enabled && r.client != nil
}

func (r *VoyageReranker) IsAvailable(context.Context) bool {
	return r.Enabled()
}

func (r *VoyageReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if !r.Enabled() {
		return passThroughRerank(candidates), nil
	}
	if len(candidates) == 0 {
		return []RerankResult{}, nil
	}
	topK := len(candidates)
	if r.config.TopK > 0 && r.config.TopK < topK {
		topK = r.config.TopK
	}
	head := candidates[:topK]
	documents := make([]string, len(head))
	for i, candidate := range head {
		documents[i] = candidate.Content
	}
	resp, err := r.client.Rerank(ctx, query, documents, voyageapi.RerankOptions{
		Model:           r.config.Model,
		TopK:            topK,
		ReturnDocuments: false,
		Truncation:      true,
	})
	if err != nil {
		return passThroughRerank(head), nil
	}
	results := make([]RerankResult, 0, len(resp.Data))
	seenValidResult := false
	for rank, item := range resp.Data {
		if item.Index < 0 || item.Index >= len(head) {
			continue
		}
		seenValidResult = true
		candidate := head[item.Index]
		if r.config.MinScore > 0 && item.RelevanceScore < r.config.MinScore {
			continue
		}
		results = append(results, RerankResult{
			ID:           candidate.ID,
			Content:      candidate.Content,
			OriginalRank: item.Index + 1,
			NewRank:      rank + 1,
			BiScore:      candidate.Score,
			CrossScore:   item.RelevanceScore,
			FinalScore:   item.RelevanceScore,
		})
	}
	if !seenValidResult {
		return passThroughRerank(head), nil
	}
	return results, nil
}

func passThroughRerank(candidates []RerankCandidate) []RerankResult {
	results := make([]RerankResult, len(candidates))
	for i, candidate := range candidates {
		results[i] = RerankResult{
			ID:           candidate.ID,
			Content:      candidate.Content,
			OriginalRank: i + 1,
			NewRank:      i + 1,
			BiScore:      candidate.Score,
			CrossScore:   candidate.Score,
			FinalScore:   candidate.Score,
		}
	}
	return results
}
