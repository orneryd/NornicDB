package server

import (
	"context"
	"log/slog"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestQdrantGRPCUsesConfiguredRequestLogger(t *testing.T) {
	var output synchronizedLogBuffer
	var level slog.LevelVar
	srv, authenticator := setupTestServerWithConfig(t, func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: &level}))
		cfg.EmbeddingEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0"}
	})
	require.NoError(t, srv.startQdrantGRPC())
	t.Cleanup(srv.qdrantGRPCServer.Stop)
	conn, err := grpc.NewClient(srv.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := qpb.NewCollectionsClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	authed := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+getAuthToken(t, authenticator, "admin")))
	_, err = client.List(authed, &qpb.ListCollectionsRequest{})
	require.NoError(t, err)
	require.NotContains(t, string(output.Bytes()), `"event_id":"qdrant.grpc.request"`)
	level.Set(slog.LevelDebug)
	_, err = client.List(authed, &qpb.ListCollectionsRequest{})
	require.NoError(t, err)
	record := findJSONLogRecord(t, output.Bytes(), "qdrant.grpc.request", "gRPC request completed")
	require.Equal(t, "DEBUG", record["level"])
	require.Equal(t, "OK", record["grpc_code"])
	require.Equal(t, "qdrantgrpc", record["component"])
	output.mu.Lock()
	output.buffer.Reset()
	output.mu.Unlock()
	level.Set(slog.LevelInfo)
	_, err = client.List(ctx, &qpb.ListCollectionsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	record = findJSONLogRecord(t, output.Bytes(), "qdrant.grpc.request", "gRPC request completed")
	require.Equal(t, "WARN", record["level"])
	require.Equal(t, "Unauthenticated", record["grpc_code"])
	require.Equal(t, "failure", record["outcome"])
}
