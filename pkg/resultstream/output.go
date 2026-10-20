package resultstream

import (
	"context"
	"errors"
)

// Consumer is the execution row handoff. False stops a read upstream; an
// error fails execution. Transformations compose consumers rather than tables.
type Consumer func(columns []string, row []any) (bool, error)

// Producer enumerates rows within the lifetime of the executing statement.
type Producer func(context.Context, Consumer) error

type consumerKey struct{}

func WithConsumer(ctx context.Context, consumer Consumer) context.Context {
	return context.WithValue(ctx, consumerKey{}, consumer)
}

func ConsumerFromContext(ctx context.Context) Consumer {
	consumer, _ := ctx.Value(consumerKey{}).(Consumer)
	return consumer
}

// WithoutConsumer separates a statement's final output from intermediate
// operations. Routing forwards the consumer; a transforming operator supplies
// its own consumer to its upstream, so filtering precedes output admission.
func WithoutConsumer(ctx context.Context) context.Context {
	return WithConsumer(ctx, nil)
}

// ErrValueTooLarge is returned by a codec before allocating an oversized value.
var ErrValueTooLarge = errors.New("result value exceeds output byte budget")

// Budget is the one execution output admission/retention owner. Codec converts
// native values and measures the representation required by its consumer.
// A budget is driven synchronously by one final row pipeline.
type Budget struct {
	MaxRows, MaxBytes int
	Columns           []string
	Rows              [][]any
	Reasons           []string
	remaining         int
	codec             func([]any, int) ([]any, int, error)
}

func NewBudget(rows, bytes int, codec func([]any, int) ([]any, int, error)) *Budget {
	return &Budget{MaxRows: rows, MaxBytes: bytes, remaining: bytes, codec: codec, Rows: [][]any{}, Reasons: []string{}}
}

func (b *Budget) Mark(reason string) {
	for _, current := range b.Reasons {
		if current == reason {
			return
		}
	}
	b.Reasons = append(b.Reasons, reason)
}

func (b *Budget) Consume(columns []string, row []any) (bool, error) {
	if b.Columns == nil {
		b.Columns = append([]string{}, columns...)
	}
	if len(b.Reasons) > 0 {
		return false, nil
	}
	if len(b.Rows) >= b.MaxRows {
		b.Mark("max_rows")
		return false, nil
	}
	converted, cost, err := b.codec(row, b.remaining)
	if errors.Is(err, ErrValueTooLarge) || err == nil && cost > b.remaining {
		b.Mark("max_bytes")
		return false, nil
	}
	if err != nil {
		return false, err
	}
	b.remaining -= cost
	b.Rows = append(b.Rows, converted)
	return true, nil
}

type budgetKey struct{}

func WithBudget(ctx context.Context, budget *Budget) context.Context {
	return context.WithValue(ctx, budgetKey{}, budget)
}

func BudgetFromContext(ctx context.Context) *Budget {
	budget, _ := ctx.Value(budgetKey{}).(*Budget)
	return budget
}

// StreamHeader requests incremental constituent transport, without changing
// statement text, permissions, or the result's relational semantics.
const StreamHeader = "NornicDB-Result-Stream"

type drainKey struct{}

func WithDrain(ctx context.Context, drain bool) context.Context {
	return context.WithValue(ctx, drainKey{}, drain)
}
func Drain(ctx context.Context) bool { drain, _ := ctx.Value(drainKey{}).(bool); return drain }
