package search

// Root-cause diagnostic round 3, campaign lane B (test-only). G-hybrid-rrf:
// the error rrfHybridSearch returns when both the vector backend and the
// lexical filter fail, whether the caller treats it as final, and the filter
// failure alone as a control.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCBHybridErrorPrecedence(t *testing.T) {
	filterErr := errors.New("the lexical source's where failed")
	for _, vectorFails := range []bool{true, false} {
		for _, direct := range []bool{true, false} {
			engine := newNamespacedEngine(t)
			svc := NewService(engine, 2, nil)
			for _, node := range []*storage.Node{
				{ID: "lexical-only", Labels: []string{"Src"}, Properties: map[string]any{"content": "walrus notes"}},
				{ID: "vector-only", Labels: []string{"Src"}, Properties: map[string]any{"content": "unrelated topic"}, ChunkEmbeddings: [][]float32{{1, 0}}},
			} {
				if _, err := engine.CreateNode(t.Context(), node); err != nil {
					t.Fatal(err)
				}
				if err := svc.IndexNode(node); err != nil {
					t.Fatal(err)
				}
			}
			var firstErr error
			if vectorFails {
				firstErr = errors.New("vector backend temporarily unavailable")
			}
			svc.vectorPipeline = NewVectorSearchPipeline(&errorThenCandidateGenerator{firstErr: firstErr, candidate: Candidate{ID: "vector-only", Score: 1}}, &IdentityExactScorer{})
			opts := DefaultSearchOptions()
			opts.Limit = 10
			opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
				if node.ID == "lexical-only" {
					return false, filterErr
				}
				return true, nil
			}
			if direct {
				var recovered any
				var err error
				func() {
					defer func() { recovered = recover() }()
					_, err = svc.rrfHybridSearch(t.Context(), "walrus", []float32{1, 0}, opts)
				}()
				t.Logf("RCBRRF vectorFails=%v rrfHybridSearch err=%v isFilterErr=%v finalSearchError=%v panic=%v", vectorFails, err, errors.Is(err, filterErr), finalSearchError(err), recovered)
			} else {
				response, err := svc.Search(t.Context(), "walrus", []float32{1, 0}, opts)
				results, fallback := -1, ""
				if response != nil {
					results, fallback = len(response.Results), fmt.Sprint(response.FallbackReason)
				}
				t.Logf("RCBRRF vectorFails=%v Search err=%v isFilterErr=%v results=%d fallback=%q", vectorFails, err, errors.Is(err, filterErr), results, fallback)
			}
			_ = svc.Close()
		}
	}
}
