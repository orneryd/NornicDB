package resolvers

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

type solObservedContext struct {
	context.Context
	entered   chan struct{}
	once      sync.Once
	watcherID string
}

func (c *solObservedContext) Done() <-chan struct{} {
	c.once.Do(func() {
		stack := make([]byte, 1024)
		stack = stack[:runtime.Stack(stack, false)]
		c.watcherID = strings.Fields(string(stack))[1]
		close(c.entered)
	})
	return c.Context.Done()
}

// Adapted from the independent review: track the exact watcher goroutine,
// rather than a function name which changes when cleanup is centralized.
func TestSolBrokerCloseStopsWatcher(t *testing.T) {
	t.Run("NodeCreated", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan *models.Node {
			return b.SubscribeNodeCreated(ctx, nil)
		})
	})
	t.Run("NodeUpdated", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan *models.Node {
			return b.SubscribeNodeUpdated(ctx, nil, nil)
		})
	})
	t.Run("NodeDeleted", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan string {
			return b.SubscribeNodeDeleted(ctx, nil)
		})
	})
	t.Run("RelationshipCreated", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan *models.Relationship {
			return b.SubscribeRelationshipCreated(ctx, nil)
		})
	})
	t.Run("RelationshipUpdated", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan *models.Relationship {
			return b.SubscribeRelationshipUpdated(ctx, nil, nil)
		})
	})
	t.Run("RelationshipDeleted", func(t *testing.T) {
		checkBrokerCloseStopsWatcher(t, func(b *EventBroker, ctx context.Context) <-chan string {
			return b.SubscribeRelationshipDeleted(ctx, nil)
		})
	})
}

func checkBrokerCloseStopsWatcher[T comparable](t *testing.T, subscribe func(*EventBroker, context.Context) <-chan T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &solObservedContext{Context: ctx, entered: make(chan struct{})}
	broker := NewEventBroker()
	defer broker.Close()
	ch := subscribe(broker, observed)
	select {
	case <-observed.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not start")
	}
	broker.Close()
	var zero T
	readBrokerEvent(t, ch, zero, false)
	if ctx.Err() != nil {
		t.Fatal("broker shutdown unexpectedly cancelled the caller context")
	}
	stack := make([]byte, 1<<20)
	stack = stack[:runtime.Stack(stack, true)]
	for _, goroutine := range strings.Split(string(stack), "\n\n") {
		if strings.HasPrefix(goroutine, "goroutine "+observed.watcherID+" ") &&
			(strings.Contains(goroutine, "[chan receive]") || strings.Contains(goroutine, "[select]")) {
			t.Fatal("broker Close leaves its subscription watcher blocked on the uncancelled context")
		}
	}
}
