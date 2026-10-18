package qdrantgrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSearchStreamLifecycleGRPCResponseSendFailure(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	svc := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	svc.SetContinuationRegistry(registry)
	api, err := nornicgrpc.NewService(nornicgrpc.Config{}, nil, nil, svc)
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.StatsHandler(searchContinuationStats{}), grpc.MaxSendMsgSize(64))
	gen.RegisterNornicSearchServer(server, api)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	conn, err := grpc.NewClient("passthrough:///stream-test", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = gen.NewNornicSearchClient(conn).SearchText(ctx, &gen.SearchTextRequest{Mode: "id", N: 1})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Eventually(t, func() bool {
		page, err := svc.SearchTextContinuation(ctx, "", search.DefaultSearchOptions(), search.SearchContinuationRequest{Owner: "replacement", Database: "nornic", Mode: search.SearchContinuationID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
		if err != nil { return false }
		return registry.Discard(resultstream.Scope{Owner: "replacement", Database: "nornic"}, page.QID) == nil
	}, time.Second, 10*time.Millisecond, "gRPC send failure must release the only registry slot")
}
