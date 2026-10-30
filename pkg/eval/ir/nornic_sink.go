package ir

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// NodeIndexer is the search index operation required after a benchmark write.
type NodeIndexer interface {
	IndexNode(*storage.Node) error
}

// NornicDocumentSink imports one BEIR document as one NornicDB retrieval unit.
// The beir_id property is the lossless persistent map when storage namespaces
// rewrite the physical node ID.
type NornicDocumentSink struct {
	Engine  storage.Engine
	Indexer NodeIndexer
}

// QueryEmbedder produces a query embedding for vector and hybrid benchmark variants.
type QueryEmbedder func(context.Context, string) ([]float32, error)

// NornicRetriever adapts the existing search service to the benchmark retriever.
// A nil Embedder intentionally executes BM25-only retrieval.
type NornicRetriever struct {
	Service  *search.Service
	Embedder QueryEmbedder
	Options  *search.SearchOptions
}

// Retrieve executes one NornicDB retrieval query and returns BEIR document IDs.
func (r *NornicRetriever) Retrieve(ctx context.Context, query string, topK int) ([]RunResult, error) {
	if r == nil || r.Service == nil {
		return nil, fmt.Errorf("search service is required")
	}
	if topK < 1 {
		return nil, fmt.Errorf("top-k must be positive")
	}
	var embedding []float32
	var err error
	if r.Embedder != nil {
		embedding, err = r.Embedder(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("embed query: %w", err)
		}
	}
	options := search.DefaultSearchOptions()
	if r.Options != nil {
		copy := *r.Options
		options = &copy
	}
	options.Limit = topK
	response, err := r.Service.Search(ctx, query, embedding, options)
	if err != nil {
		return nil, err
	}
	return RunResultsFromSearchResults(response.Results)
}

// StoreDocument persists and indexes a BEIR document with its original ID.
func (s *NornicDocumentSink) StoreDocument(ctx context.Context, document Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.Engine == nil || s.Indexer == nil {
		return fmt.Errorf("storage engine and node indexer are required")
	}
	node := &storage.Node{
		ID:     storage.NodeID(document.ID),
		Labels: []string{"BEIRDocument"},
		Properties: map[string]any{
			"beir_id": document.ID,
			"title":   document.Title,
			"text":    document.Text,
		},
	}
	_, err := s.Engine.CreateNode(node)
	if err != nil {
		return err
	}
	return s.Indexer.IndexNode(node)
}

// RunResultsFromSearchResults converts hydrated NornicDB results into
// evaluation-facing BEIR IDs. Every benchmark result must carry beir_id.
func RunResultsFromSearchResults(results []search.SearchResult) ([]RunResult, error) {
	runResults := make([]RunResult, 0, len(results))
	for index, result := range results {
		beirID, ok := result.Properties["beir_id"].(string)
		if !ok || beirID == "" {
			return nil, fmt.Errorf("search result %d (%q) is missing beir_id", index, result.ID)
		}
		runResults = append(runResults, RunResult{DocumentID: beirID, Score: result.Score})
	}
	return runResults, nil
}
