package nornicgrpc

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGRPCContinuationResultLimitPublic(t *testing.T) {
	contract.RunResultLimit(t, func(t *testing.T, vector bool) contract.LimitFixture {
		engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "ceiling")
		service := search.NewServiceWithDimensions(engine, 2)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		var embed EmbedQueryFunc
		if vector {
			embed = func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
		}
		rpc, err := NewService(Config{DefaultDatabase: "ceiling"}, embed, nil, service)
		require.NoError(t, err)
		request := func(ctx context.Context, req *gen.SearchTextRequest) (contract.Page, error) {
			wire, err := rpc.SearchText(ctx, req)
			if err != nil {
				return contract.Page{}, err
			}
			result := contract.Page{QID: wire.Qid, More: wire.HasMore, Completion: wire.Completion, Collection: wire.CollectionExhausted, Ranked: wire.RankedPoolExhausted}
			for _, hit := range wire.Hits {
				result.IDs = append(result.IDs, hit.NodeId)
			}
			return result, nil
		}
		return contract.LimitFixture{Fixture: contract.Fixture{Service: service, Engine: engine, Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
			return request(ctx, &gen.SearchTextRequest{Qid: qid, N: uint32(n)})
		}}, Start: func(ctx context.Context, r contract.LimitRequest) (contract.Page, error) {
			ceiling := uint64(r.MaxResults)
			return request(ctx, &gen.SearchTextRequest{Query: "ceilingtoken", Limit: uint32(r.Limit), N: uint32(r.N), MaxResults: &ceiling, Mode: string(r.Mode)})
		}}
	})
}
