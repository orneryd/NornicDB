package nornicgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/streamretrytest"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamRetryGRPC(t *testing.T) {
	streamretrytest.Run(t, func(t *testing.T) streamretrytest.Fixture {
		memory := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, memory.Close()) })
		engine := storage.NewNamespacedEngine(memory, "retry")
		service := search.NewService(engine)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		rpc, err := NewService(Config{DefaultDatabase: "retry", OwnerFromContext: func(context.Context) string { return "retry" }}, nil, nil, service)
		require.NoError(t, err)
		return streamretrytest.Fixture{Service: service, Engine: engine, Owner: "retry", Database: "retry",
			Pull: func(ctx context.Context, qid string, n int) (streamretrytest.Page, error) {
				wire, err := rpc.SearchText(ctx, &gen.SearchTextRequest{Qid: qid, N: uint32(n)})
				if err != nil {
					if ctx.Err() != nil {
						return streamretrytest.Page{}, ctx.Err()
					}
					return streamretrytest.Page{}, err
				}
				page := streamretrytest.Page{More: wire.HasMore}
				for _, hit := range wire.Hits {
					page.IDs = append(page.IDs, hit.NodeId)
				}
				return page, nil
			}}
	})
}
