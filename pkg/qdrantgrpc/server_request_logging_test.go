package qdrantgrpc

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type requestLogCapture struct {
	mu        sync.Mutex
	threshold slog.LevelVar
	records   []slog.Record
}

func (h *requestLogCapture) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.threshold.Level()
}
func (h *requestLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *requestLogCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *requestLogCapture) WithGroup(_ string) slog.Handler      { return h }
func (h *requestLogCapture) take() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.records
	h.records = nil
	return r
}

// Exercise the actual registered interceptors through unary and streaming RPCs.
func TestServerRequestLoggingPublicRPCs(t *testing.T) {
	capture := &requestLogCapture{}
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Logger = slog.New(capture)
	srv, err := NewServer(cfg, newTestCollectionStore(t), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil, nil)
	require.NoError(t, err)
	unary := func(_ interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
		req := new(wrapperspb.Int32Value)
		if err := dec(req); err != nil {
			return nil, err
		}
		handler := func(context.Context, interface{}) (interface{}, error) {
			return &emptypb.Empty{}, status.Error(codes.Code(req.Value), "test outcome")
		}
		return interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/test.Logging/Unary"}, handler)
	}
	require.NoError(t, srv.RegisterAdditionalServices(func(reg *grpc.Server) {
		reg.RegisterService(&grpc.ServiceDesc{
			ServiceName: "test.Logging", HandlerType: (*interface{})(nil),
			Methods: []grpc.MethodDesc{{MethodName: "Unary", Handler: unary}},
			Streams: []grpc.StreamDesc{{StreamName: "Stream", ServerStreams: true, Handler: func(_ interface{}, stream grpc.ServerStream) error {
				req := new(wrapperspb.Int32Value)
				if err := stream.RecvMsg(req); err != nil {
					return err
				}
				return status.Error(codes.Code(req.Value), "test outcome")
			}}},
		}, struct{}{})
	}))
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	for _, kind := range []string{"Unary", "Stream"} {
		for _, tc := range []struct {
			name      string
			threshold slog.Level
			code      codes.Code
			want      slog.Level
			logged    bool
		}{
			{"success suppressed", slog.LevelInfo, codes.OK, slog.LevelDebug, false},
			{"success debug", slog.LevelDebug, codes.OK, slog.LevelDebug, true},
			{"client failure", slog.LevelInfo, codes.InvalidArgument, slog.LevelWarn, true},
			{"authentication failure", slog.LevelInfo, codes.Unauthenticated, slog.LevelWarn, true},
			{"internal failure", slog.LevelInfo, codes.Internal, slog.LevelError, true},
			{"unknown failure", slog.LevelInfo, codes.Unknown, slog.LevelError, true},
			{"data loss", slog.LevelInfo, codes.DataLoss, slog.LevelError, true},
			{"service unavailable", slog.LevelInfo, codes.Unavailable, slog.LevelError, true},
			{"warning suppressed", slog.LevelError, codes.InvalidArgument, slog.LevelWarn, false},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				capture.threshold.Set(tc.threshold)
				capture.take()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if kind == "Unary" {
					err = conn.Invoke(ctx, "/test.Logging/Unary", wrapperspb.Int32(int32(tc.code)), new(emptypb.Empty))
				} else {
					var stream grpc.ClientStream
					stream, err = conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/test.Logging/Stream")
					require.NoError(t, err)
					require.NoError(t, stream.SendMsg(wrapperspb.Int32(int32(tc.code))))
					require.NoError(t, stream.CloseSend())
					err = stream.RecvMsg(new(emptypb.Empty))
					if err == io.EOF {
						err = nil
					}
				}
				require.Equal(t, tc.code, status.Code(err))
				records := capture.take()
				if !tc.logged {
					require.Empty(t, records)
					return
				}
				require.Len(t, records, 1)
				require.Equal(t, tc.want, records[0].Level)
				attrs := map[string]any{}
				records[0].Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.Any(); return true })
				require.Equal(t, "/test.Logging/"+kind, attrs["rpc_method"])
				require.Equal(t, tc.code.String(), attrs["grpc_code"])
				require.Contains(t, attrs, "duration")
			})
		}
	}
}
