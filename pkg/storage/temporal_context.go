package storage

import (
	"context"
	"time"
)

// TemporalNodeAsOfWithContext dispatches temporal lookup to a contextual owner.
// Legacy extension implementations are checked before and after their call.
func TemporalNodeAsOfWithContext(ctx context.Context, engine TemporalLookupEngine, label, key string, value interface{}, start, end string, at time.Time) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetTemporalNodeAsOfWithContext(context.Context, string, string, interface{}, string, string, time.Time) (*Node, error)
	}); ok {
		return reader.GetTemporalNodeAsOfWithContext(ctx, label, key, value, start, end, at)
	}
	node, err := engine.GetTemporalNodeAsOf(label, key, value, start, end, at)
	if err == nil {
		err = ctx.Err()
	}
	return node, err
}

// IsCurrentTemporalNodeWithContext dispatches the existing temporal visibility
// decision while retaining the statement's cancellation through preparation.
func IsCurrentTemporalNodeWithContext(ctx context.Context, engine TemporalCurrentNodeEngine, node *Node, at time.Time) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if reader, ok := engine.(interface {
		IsCurrentTemporalNodeWithContext(context.Context, *Node, time.Time) (bool, error)
	}); ok {
		return reader.IsCurrentTemporalNodeWithContext(ctx, node, at)
	}
	current, err := engine.IsCurrentTemporalNode(node, at)
	if err == nil {
		err = ctx.Err()
	}
	return current, err
}
