package observability

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// BaggageAllowList is the set of baggage keys forwarded through the system
// (SEC-02). Default is empty — all unknown baggage keys are dropped at the
// HTTP edge. Operators configure allowed keys via NORNICDB_BAGGAGE_ALLOW_LIST.
var BaggageAllowList = map[string]bool{}

// FilteredBaggagePropagator wraps propagation.Baggage and strips keys not in
// BaggageAllowList on Extract. Inject is unmodified (we only control inbound).
type FilteredBaggagePropagator struct {
	propagation.Baggage
}

func (f FilteredBaggagePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = f.Baggage.Extract(ctx, carrier)
	bag := baggage.FromContext(ctx)
	members := bag.Members()
	if len(members) == 0 || len(BaggageAllowList) == 0 {
		// Empty allow-list means drop all baggage.
		if len(BaggageAllowList) == 0 && len(members) > 0 {
			return baggage.ContextWithBaggage(ctx, baggage.Baggage{})
		}
		return ctx
	}
	var kept []baggage.Member
	for _, m := range members {
		if BaggageAllowList[m.Key()] {
			kept = append(kept, m)
		}
	}
	filtered, _ := baggage.New(kept...)
	return baggage.ContextWithBaggage(ctx, filtered)
}
