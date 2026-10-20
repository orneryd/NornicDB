package fabric

import (
	"context"
	"time"
)

type executionValuesKey struct{}
type transactionLifetimeContextKey struct{}

// executionValues carries one Fabric invocation. Scope and records are bound
// together; prepareExecDispatch fills routing metadata before handing it to a
// physical executor. Subsequent bindings copy the snapshot rather than changing
// their parent. A standard value context keeps unrelated native evaluator
// lookups from recursing through a custom Context.Value at every fragment.
type executionValues struct {
	frame   *LogicalFrame
	scope   CallScope
	records map[string]interface{}
	graph   string
	sub     *SubTransaction
	tx      *FabricTransaction
}

var emptyExecutionValues = &executionValues{}

func executionValuesFromContext(ctx context.Context) *executionValues {
	if ctx != nil {
		if values, ok := ctx.Value(executionValuesKey{}).(*executionValues); ok {
			return values
		}
	}
	return emptyExecutionValues
}

func withExecutionValues(ctx context.Context, values executionValues) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionValuesKey{}, &values)
}

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
	values := *executionValuesFromContext(ctx)
	values.sub = sub
	return withExecutionValues(ctx, values)
}

// SubTransactionFromContext returns the active fabric sub-transaction from context.
func SubTransactionFromContext(ctx context.Context) (*SubTransaction, bool) {
	sub := executionValuesFromContext(ctx).sub
	return sub, sub != nil
}

// WithFabricTransaction returns a context carrying the active Fabric transaction.
func WithFabricTransaction(ctx context.Context, tx *FabricTransaction) context.Context {
	values := *executionValuesFromContext(ctx)
	values.tx = tx
	return withExecutionValues(ctx, values)
}

// FabricTransactionFromContext returns the active Fabric transaction from context.
func FabricTransactionFromContext(ctx context.Context) (*FabricTransaction, bool) {
	tx := executionValuesFromContext(ctx).tx
	return tx, tx != nil
}

// WithRecordBindings stores correlated outer-row bindings for Fabric APPLY execution.
func WithRecordBindings(ctx context.Context, bindings map[string]interface{}) context.Context {
	values := *executionValuesFromContext(ctx)
	values.records = bindings
	return withExecutionValues(ctx, values)
}

// RecordBindingsFromContext returns correlated outer-row bindings if present.
func RecordBindingsFromContext(ctx context.Context) (map[string]interface{}, bool) {
	bindings := executionValuesFromContext(ctx).records
	return bindings, bindings != nil
}
