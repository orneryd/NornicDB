package fabric

import (
	"context"
	"time"
)

type subTransactionContextKey struct{}
type fabricTransactionContextKey struct{}
type recordBindingsContextKey struct{}
type transactionLifetimeContextKey struct{}

// WithTransactionLifetime binds the immutable protocol owner before BEGIN.
// Statements keep their own cancellation; participant commit uses this owner.
func WithTransactionLifetime(ctx, lifetime context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if lifetime == nil {
		return ctx
	}
	return context.WithValue(ctx, transactionLifetimeContextKey{}, lifetime)
}

// TransactionLifetimeFromContext returns the owner captured at protocol BEGIN.
func TransactionLifetimeFromContext(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	lifetime, _ := ctx.Value(transactionLifetimeContextKey{}).(context.Context)
	return lifetime
}

// transactionLifetime keeps embedded BEGIN cancellation when no protocol
// lifetime was supplied; protocol callers use the same completion wrapper.
func transactionLifetime(ctx context.Context) context.Context {
	if TransactionLifetimeFromContext(ctx) == nil {
		return ctx
	}
	return TransactionCompletionContext(ctx)
}

// TransactionCompletionContext retains statement identity/tracing values while
// taking cancellation only from the transaction's owner. With no protocol owner
// it preserves the existing detached completion contract for embedded callers.
// Rollback cleanup uses its own independent context, even after this owner ends.
func TransactionCompletionContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	lifetime := TransactionLifetimeFromContext(ctx)
	values := context.WithoutCancel(ctx)
	if lifetime == nil {
		return values
	}
	return transactionCompletionContext{Context: values, lifetime: lifetime}
}

type transactionCompletionContext struct {
	context.Context
	lifetime context.Context
}

func (c transactionCompletionContext) Deadline() (time.Time, bool) { return c.lifetime.Deadline() }
func (c transactionCompletionContext) Done() <-chan struct{}       { return c.lifetime.Done() }
func (c transactionCompletionContext) Err() error                  { return c.lifetime.Err() }
func (c transactionCompletionContext) AfterFunc(f func()) func() bool {
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
	return context.WithValue(ctx, unresolvedRemoteModeKey{}, true)
}

func unresolvedRemoteMode(ctx context.Context) bool {
	unresolved, _ := ctx.Value(unresolvedRemoteModeKey{}).(bool)
	return unresolved
}
