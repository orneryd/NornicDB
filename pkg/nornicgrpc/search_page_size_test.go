package nornicgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"net"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSearchContinuationPageSizeGRPC(t *testing.T) {
	for _, limit := range []int{500, 2, 600} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "nornic")
			for i := 0; i < limit+2; i++ {
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("page-%04d", i))})
				require.NoError(t, err)
			}
			options := search.DefaultServiceOptions("nornic")
			if limit != 500 {
				options.SearchCursorMaxPageSize = limit
			}
			svc := search.NewService(store, 3, &options)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			api, err := NewService(Config{}, nil, nil, svc)
			require.NoError(t, err)
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			gen.RegisterNornicSearchServer(server, api)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient("passthrough:///page-size", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			client := gen.NewNornicSearchClient(conn)
			ctx := context.Background()
			assertOversized := func(request *gen.SearchTextRequest) {
				for _, locale := range []struct {
					tag  string
					want string
				}{
					{"en-US", fmt.Sprintf("page_size_exceeded: search continuation n exceeds max_page_size=%d", limit)},
					{"en-XA", fmt.Sprintf("[!! page_size_exceeded: search continuation n exceeds max_page_size=%d !!]", limit)},
					{"es-ES", fmt.Sprintf("page_size_exceeded: n de continuación de búsqueda supera max_page_size=%d", limit)},
				} {
					localized := metadata.NewOutgoingContext(ctx, metadata.Pairs("accept-language", locale.tag))
					_, err := client.SearchText(localized, request)
					require.Equal(t, codes.InvalidArgument, status.Code(err))
					require.Equal(t, locale.want, status.Convert(err).Message(), locale.tag)
					details := status.Convert(err).Details()
					require.Len(t, details, 1)
					info, ok := details[0].(*errdetails.ErrorInfo)
					require.True(t, ok)
					require.Equal(t, "PAGE_SIZE_EXCEEDED", info.Reason)
					require.Equal(t, "page_size_exceeded", info.Metadata["request_status"])
					require.Equal(t, fmt.Sprint(limit), info.Metadata["max_page_size"])
				}
			}
			assertOversized(&gen.SearchTextRequest{Mode: "id", N: uint32(limit + 1)})
			var header metadata.MD
			first, err := client.SearchText(ctx, &gen.SearchTextRequest{Mode: "id", N: 1}, grpc.Header(&header))
			require.NoError(t, err)
			require.Equal(t, []string{fmt.Sprint(limit)}, header.Get("max_page_size"))
			assertOversized(&gen.SearchTextRequest{Qid: first.Qid, N: uint32(limit + 1)})
			exact, err := client.SearchText(ctx, &gen.SearchTextRequest{Qid: first.Qid, N: uint32(limit)})
			require.NoError(t, err)
			require.Len(t, exact.Hits, limit)
			exact, err = client.SearchText(ctx, &gen.SearchTextRequest{Mode: "id", N: uint32(limit)})
			require.NoError(t, err)
			require.Len(t, exact.Hits, limit)
			_, err = client.SearchText(ctx, &gen.SearchTextRequest{Qid: "bad-qid", N: 1})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Empty(t, status.Convert(err).Details())
			require.NotContains(t, err.Error(), "page_size_exceeded")
		})
	}
}
