package ir

import (
	"context"
	"fmt"
	"io"
)

// Retriever executes a benchmark query and returns evaluation-facing BEIR IDs.
type Retriever interface {
	Retrieve(context.Context, string, int) ([]RunResult, error)
}

// RunStats reports one manifest-driven retrieval execution.
type RunStats struct {
	Queries int64
}

// RunManifest executes only the query IDs in manifest and writes a TREC run in
// manifest order. Queries must contain every selected official query ID.
func RunManifest(ctx context.Context, manifest QueryManifest, queries []Query, retriever Retriever, topK int, tag string, writer io.Writer) (RunStats, error) {
	if retriever == nil || writer == nil {
		return RunStats{}, fmt.Errorf("retriever and run writer are required")
	}
	if topK < 1 {
		return RunStats{}, fmt.Errorf("top-k must be positive")
	}
	queryText := make(map[string]string, len(queries))
	for _, query := range queries {
		queryText[query.ID] = query.Text
	}
	stats := RunStats{}
	for _, queryID := range manifest.QueryIDs {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		text, exists := queryText[queryID]
		if !exists {
			return stats, fmt.Errorf("manifest query %q is absent from queries.jsonl", queryID)
		}
		results, err := retriever.Retrieve(ctx, text, topK)
		if err != nil {
			return stats, fmt.Errorf("retrieve query %q: %w", queryID, err)
		}
		if len(results) > topK {
			results = results[:topK]
		}
		if err := WriteRun(writer, queryID, results, tag); err != nil {
			return stats, err
		}
		stats.Queries++
	}
	return stats, nil
}
