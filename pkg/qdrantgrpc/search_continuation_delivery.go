package qdrantgrpc

import (
	"context"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"google.golang.org/grpc/stats"
)

// RPC End observes response-send failures that unary interceptors cannot see.
type searchContinuationStats struct{}

func (searchContinuationStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if info.FullMethodName == gen.NornicSearch_SearchText_FullMethodName {
		return resultstream.WithDelivery(ctx, &resultstream.Delivery{})
	}
	return ctx
}

func (searchContinuationStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if end, ok := event.(*stats.End); ok {
		if delivery := resultstream.DeliveryFromContext(ctx); delivery != nil {
			if end.Error != nil { delivery.Fail() } else { delivery.Delivered() }
		}
	}
}

func (searchContinuationStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (searchContinuationStats) HandleConn(context.Context, stats.ConnStats) {}
