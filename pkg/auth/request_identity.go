package auth

import "context"

type requestIdentityKey struct{}

// RequestIdentity is projected only from verified authentication by the host.
// It carries identity, not a second permission decision.
type RequestIdentity struct {
	ID       string
	Username string
}

func WithRequestIdentity(ctx context.Context, identity RequestIdentity) context.Context {
	return context.WithValue(ctx, requestIdentityKey{}, identity)
}
func RequestIdentityFromContext(ctx context.Context) RequestIdentity {
	value, _ := ctx.Value(requestIdentityKey{}).(RequestIdentity)
	return value
}
