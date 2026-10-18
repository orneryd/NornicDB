package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// Every publisher pauses after copying its subscribers. Cancellation or Close
// must finish before the publisher is allowed to touch that stale copy.
func TestEventBroker_PublishAfterSnapshotClosed(t *testing.T) {
	node1 := &models.Node{ID: "before", Labels: []string{"Person"}}
	node2 := &models.Node{ID: "after", Labels: []string{"Person"}}
	rel1 := &models.Relationship{ID: "before", Type: "KNOWS"}
	rel2 := &models.Relationship{ID: "after", Type: "KNOWS"}
	t.Run("NodeCreated", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan *models.Node {
			return b.SubscribeNodeCreated(ctx, []string{"Person"})
		}, (*EventBroker).PublishNodeCreated, node1, node2)
	})
	t.Run("NodeUpdated", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan *models.Node {
			return b.SubscribeNodeUpdated(ctx, nil, []string{"Person"})
		}, (*EventBroker).PublishNodeUpdated, node1, node2)
	})
	t.Run("NodeDeleted", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan string {
			// Deletions deliberately ignore filters: only the ID is known.
			return b.SubscribeNodeDeleted(ctx, []string{"Other"})
		}, (*EventBroker).PublishNodeDeleted, "before", "after")
	})
	t.Run("RelationshipCreated", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan *models.Relationship {
			return b.SubscribeRelationshipCreated(ctx, []string{"KNOWS"})
		}, (*EventBroker).PublishRelationshipCreated, rel1, rel2)
	})
	t.Run("RelationshipUpdated", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan *models.Relationship {
			return b.SubscribeRelationshipUpdated(ctx, nil, []string{"KNOWS"})
		}, (*EventBroker).PublishRelationshipUpdated, rel1, rel2)
	})
	t.Run("RelationshipDeleted", func(t *testing.T) {
		checkBrokerSnapshotClose(t, func(b *EventBroker, ctx context.Context) <-chan string {
			return b.SubscribeRelationshipDeleted(ctx, []string{"OTHER"})
		}, (*EventBroker).PublishRelationshipDeleted, "before", "after")
	})
}

func checkBrokerSnapshotClose[T comparable](t *testing.T, subscribe func(*EventBroker, context.Context) <-chan T, publish func(*EventBroker, T), before, after T) {
	t.Helper()
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			broker := NewEventBroker()
			t.Cleanup(broker.Close)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			liveCtx, liveCancel := context.WithCancel(context.Background())
			t.Cleanup(liveCancel)
			dead := subscribe(broker, ctx)
			live := subscribe(broker, liveCtx)
			publish(broker, before)

			copied := make(chan struct{})
			resume := make(chan struct{})
			broker.afterSnapshot = func() {
				close(copied)
				<-resume
			}
			finished := make(chan any, 1)
			go func() {
				defer func() { finished <- recover() }()
				publish(broker, after)
			}()
			select {
			case <-copied:
			case <-time.After(5 * time.Second):
				t.Fatal("publisher did not reach snapshot barrier")
			}
			if mode == "cancel" {
				cancel()
			} else {
				broker.Close()
			}
			// Observe closure before resuming, retaining already queued data.
			readBrokerEvent(t, dead, before, true)
			var zero T
			readBrokerEvent(t, dead, zero, false)
			close(resume)
			select {
			case panicValue := <-finished:
				if panicValue != nil {
					t.Fatalf("publisher panicked after channel closed: %v", panicValue)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("publisher blocked on a dead subscriber")
			}
			broker.afterSnapshot = nil
			readBrokerEvent(t, live, before, true)
			if mode == "cancel" {
				readBrokerEvent(t, live, after, true)
				// A full live buffer keeps earlier events and drops overflow.
				for range 11 {
					publish(broker, before)
				}
				for range 10 {
					readBrokerEvent(t, live, before, true)
				}
				select {
				case <-live:
					t.Fatal("full subscriber buffer did not drop the overflow")
				default:
				}
			} else {
				readBrokerEvent(t, live, zero, false)
				broker.Close()
			}
		})
	}
}

func readBrokerEvent[T comparable](t *testing.T, ch <-chan T, want T, wantOpen bool) {
	t.Helper()
	select {
	case got, open := <-ch:
		if got != want || open != wantOpen {
			t.Fatalf("event = (%v, %v), want (%v, %v)", got, open, want, wantOpen)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not deliver buffered event or close")
	}
}

func BenchmarkEventBrokerPublish(b *testing.B) {
	for _, subscribers := range []int{1, 16} {
		for _, drain := range []bool{true, false} {
			b.Run(fmt.Sprintf("subscribers=%d/drain=%t", subscribers, drain), func(b *testing.B) {
				broker := NewEventBroker()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				defer broker.Close()
				channels := make([]<-chan *models.Node, subscribers)
				for i := range channels {
					channels[i] = broker.SubscribeNodeCreated(ctx, []string{"Person"})
				}
				event := &models.Node{ID: "node", Labels: []string{"Person"}}
				if !drain {
					for range 10 {
						broker.PublishNodeCreated(event)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					broker.PublishNodeCreated(event)
					if drain {
						for _, ch := range channels {
							<-ch
						}
					}
				}
				b.StopTimer()
			})
		}
	}
}
