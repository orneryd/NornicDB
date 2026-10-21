package nornicgrpc

import (
	"context"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCContinuationGeneratedExecutionMatrix(t *testing.T) {
	contract.Run(t, func(t *testing.T, _ int, _ contract.Policy) contract.Fixture {
		engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "matrix")
		service := search.NewServiceWithDimensions(engine, 2)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		rpc, err := NewService(Config{DefaultDatabase: "matrix", OwnerFromContext: func(context.Context) string { return "matrix" }}, nil, nil, service)
		require.NoError(t, err)
		return contract.Fixture{Service: service, Engine: engine, Owner: "matrix", Database: "matrix",
			Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
				wire, err := rpc.SearchText(ctx, &gen.SearchTextRequest{Qid: qid, N: uint32(n)})
				if err != nil && ctx.Err() != nil {

					return contract.Page{}, ctx.Err()
				}
				if status.Code(err) == codes.ResourceExhausted {
					return contract.Page{}, resultstream.ErrCapacity
				}
				if err != nil {
					return contract.Page{}, err
				}
				page := contract.Page{QID: wire.Qid, More: wire.HasMore, Completion: wire.Completion, Collection: wire.CollectionExhausted, Ranked: wire.RankedPoolExhausted}
				for _, hit := range wire.Hits {
					page.IDs = append(page.IDs, hit.NodeId)
				}
				return page, nil
			}}
	})
}
