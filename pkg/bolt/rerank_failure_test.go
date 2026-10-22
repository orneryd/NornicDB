package bolt

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type boltRerankFailureProvider struct {
	results []search.RerankResult
	err     error
}

func (*boltRerankFailureProvider) Name() string                     { return "bolt_failure" }
func (*boltRerankFailureProvider) Enabled() bool                    { return true }
func (*boltRerankFailureProvider) IsAvailable(context.Context) bool { return true }
func (p *boltRerankFailureProvider) Rerank(context.Context, string, []search.RerankCandidate) ([]search.RerankResult, error) {
	return append([]search.RerankResult(nil), p.results...), p.err
}

func TestBoltDbRerankProviderFailuresReturnFailure(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	server, port := startBoltIntegrationServer(t, store)
	service := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	server.executor.(*cypherQueryExecutor).executor.SetSearchService(service)

	for name, provider := range map[string]*boltRerankFailureProvider{
		"timeout":   {err: context.DeadlineExceeded},
		"duplicate": {results: []search.RerankResult{{ID: "a", OriginalRank: 1, CrossScore: 0.9, FinalScore: 0.9}, {ID: "a", OriginalRank: 1, CrossScore: 0.8, FinalScore: 0.8}}},
		"nan":       {results: []search.RerankResult{{ID: "a", OriginalRank: 1, CrossScore: math.NaN(), FinalScore: math.NaN()}}},
	} {
		t.Run(name, func(t *testing.T) {
			service.SetReranker(provider)
			conn := openBoltTestConn(t, port)
			defer conn.Close()
			require.NoError(t, SendRun(t, conn, "CALL db.rerank({query: 'alpha', candidates: [{id: 'a', content: 'alpha', score: 0.03}, {id: 'b', content: 'beta', score: 0.02}]}) YIELD id RETURN id", nil, nil))
			_, message, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Contains(t, message, "rerank failed")
		})
	}
}
