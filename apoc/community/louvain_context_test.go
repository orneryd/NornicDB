package community

// Author: Louvain context API lane. Deterministic cancellation, deadline and
// work-boundary fixtures for the APOC community Louvain adapter.

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"reflect"
	"testing"
	"time"
)

func TestLouvainCancelledCallerPublishesNothing(t *testing.T) {
	nodes, rels := louvainScaleGraph()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Louvain(canceled, nodes, rels, DefaultLouvainConfig())
	if err != context.Canceled || result != nil {
		t.Fatalf("got %+v, %v", result, err)
	}
}

// A cancellation the adapter and engine observe at any work boundary returns
// ctx's error and no community, and the next live call still returns the same
// partition as before it.
func TestLouvainStopsAtItsWorkBoundaries(t *testing.T) {
	nodes, rels := louvainScaleGraph()
	live, err := Louvain(context.Background(), nodes, rels, DefaultLouvainConfig())
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	stopped := 0
	for budget := 1; budget <= 4096; budget++ {
		ctx := fixtureowner.NewCheckpointContext(budget, nil)
		result, err := Louvain(ctx, nodes, rels, DefaultLouvainConfig())
		if ctx.Context.Err() == nil {
			if err != nil || !reflect.DeepEqual(live, result) {
				t.Fatalf("budget %d: live call differs (%v)", budget, err)
			}
			break
		}
		if err != context.Canceled {
			t.Fatalf("budget %d: got %v, want context.Canceled", budget, err)
		}
		if result != nil {
			t.Fatalf("budget %d: cancelled call published %d communities", budget, len(result))
		}
		if ctx.Checks() != budget {
			t.Fatalf("budget %d: cancelled call reached %d boundaries", budget, ctx.Checks())
		}
		stopped++
	}
	if stopped < 64 {
		t.Fatalf("only %d cancellation boundaries exercised", stopped)
	}
	again, err := Louvain(context.Background(), nodes, rels, DefaultLouvainConfig())
	if err != nil || !reflect.DeepEqual(live, again) {
		t.Fatalf("live call after cancellation: %v", err)
	}
}

// An expired caller deadline is the context's own error, not a fixture's: the
// adapter stops at its first boundary and returns no community.
func TestLouvainExpiredDeadlineStopsWithoutResult(t *testing.T) {
	nodes, rels := louvainScaleGraph()
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	result, err := Louvain(ctx, nodes, rels, DefaultLouvainConfig())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	if result != nil {
		t.Fatalf("expired call published %d communities", len(result))
	}
}
