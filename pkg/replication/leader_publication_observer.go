package replication

import (
	"context"
	"sync/atomic"
)

// Tests can bind a decision to the route's existing local context. Production
// has no hook. Inbound/lifetime context binding remains a separate interface
// obligation; this observer receives no command, payload or action.
var leaderPublicationContextHook atomic.Pointer[func(context.Context) context.Context]

func observeLeaderPublicationContext(ctx context.Context) context.Context {
	if hook := leaderPublicationContextHook.Load(); hook != nil {
		return (*hook)(ctx)
	}
	return ctx
}
