package multidb

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type directionalContextSpy struct {
	storage.Engine
	cancel context.CancelFunc
	got    context.Context
	id     storage.NodeID
	calls  int
}

func (s *directionalContextSpy) read(ctx context.Context, id storage.NodeID) error {
	s.got, s.id = ctx, id
	s.calls++
	s.cancel()
	return ctx.Err()
}
func (s *directionalContextSpy) GetOutgoingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return nil, s.read(ctx, id)
}
func (s *directionalContextSpy) GetIncomingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return nil, s.read(ctx, id)
}
func (s *directionalContextSpy) GetAdjacentEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return nil, nil, s.read(ctx, id)
}

func TestDirectionalContextForwarding(t *testing.T) {
	for _, direction := range []string{"outgoing", "incoming", "both"} {
		t.Run(direction, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			spy := &directionalContextSpy{cancel: cancel}
			engine := &sizeTrackingEngine{Engine: spy}
			var err error
			switch direction {
			case "outgoing":
				_, err = engine.GetOutgoingEdgesWithContext(ctx, "subject")
			case "incoming":
				_, err = engine.GetIncomingEdgesWithContext(ctx, "subject")
			default:
				_, _, err = engine.GetAdjacentEdgesWithContext(ctx, "subject")
			}
			require.Same(t, ctx, spy.got)
			require.Equal(t, storage.NodeID("subject"), spy.id)
			require.Equal(t, 1, spy.calls)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func (s *directionalContextSpy) OwnsDirectionalReads(engine storage.Engine) bool { return engine == s }
