package fabric

import (
	"context"
	"time"
)

type subTransactionContextKey struct{}
type fabricTransactionContextKey struct{}
type recordBindingsContextKey struct{}
type transactionLifetimeContextKey struct{}

// WithTransactionLifetime carries the protocol-owned transaction lifetime on a
// statement context. Request/statement cancellation still governs query work;
// participant completion belongs to lifetime instead.
func WithTransactionLifetime(ctx, lifetime context.Context) context.Context {
	return context.WithValue(ctx, transactionLifetimeContextKey{}, lifetime)
}

func transactionLifetime(ctx context.Context) context.Context {
	if lifetime, ok := ctx.Value(transactionLifetimeContextKey{}).(context.Context); ok {
		return transactionLifetimeValues{Context: context.WithoutCancel(ctx), lifetime: lifetime}
	}
	return ctx
}

// Keep request identity/tracing values while only the owner controls lifetime.
type transactionLifetimeValues struct {
	context.Context
	lifetime context.Context
}

func (c transactionLifetimeValues) Deadline() (time.Time, bool) { return c.lifetime.Deadline() }
func (c transactionLifetimeValues) Done() <-chan struct{}       { return c.lifetime.Done() }
func (c transactionLifetimeValues) Err() error                  { return c.lifetime.Err() }
func (c transactionLifetimeValues) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.lifetime, f)
}

// WithSubTransaction returns a context carrying the active fabric sub-transaction.
func WithSubTransaction(ctx context.Context, sub *SubTransaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, subTransactionContextKey{}, sub)
}

// SubTransactionFromContext returns the active fabric sub-transaction from context.
func SubTransactionFromContext(ctx context.Context) (*SubTransaction, bool) {
	if ctx == nil {
		return nil, false
	}
	sub, ok := ctx.Value(subTransactionContextKey{}).(*SubTransaction)
	return sub, ok && sub != nil
}

// WithFabricTransaction returns a context carrying the active Fabric transaction.
func WithFabricTransaction(ctx context.Context, tx *FabricTransaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, fabricTransactionContextKey{}, tx)
}

// FabricTransactionFromContext returns the active Fabric transaction from context.
func FabricTransactionFromContext(ctx context.Context) (*FabricTransaction, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(fabricTransactionContextKey{}).(*FabricTransaction)
	return tx, ok && tx != nil
}

// WithRecordBindings stores correlated outer-row bindings for Fabric APPLY execution.
func WithRecordBindings(ctx context.Context, bindings map[string]interface{}) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, recordBindingsContextKey{}, bindings)
}

// RecordBindingsFromContext returns correlated outer-row bindings if present.
func RecordBindingsFromContext(ctx context.Context) (map[string]interface{}, bool) {
	if ctx == nil {
		return nil, false
	}
	v, ok := ctx.Value(recordBindingsContextKey{}).(map[string]interface{})
	return v, ok && v != nil
}

type unresolvedRemoteModeKey struct{}

func WithUnresolvedRemoteMode(ctx context.Context) context.Context {
 return context.WithValue(ctx,unresolvedRemoteModeKey{},true)
}

func unresolvedRemoteMode(ctx context.Context) bool {
 unresolved,_:=ctx.Value(unresolvedRemoteModeKey{}).(bool)
 return unresolved
}