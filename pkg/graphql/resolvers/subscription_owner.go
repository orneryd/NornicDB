package resolvers

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/util/eventchannel"
)

// subscriptionChannel adds the caller-context watcher to the shared delivery
// owner. Watcher bookkeeping stays out of the publisher's filter snapshots.
type subscriptionChannel interface {
	Close()
	watch(context.Context, func())
	wait()
}

type subscriptionOwner[T any] struct {
	*eventchannel.Channel[T]
	watcherDone chan struct{}
}

func newSubscriptionOwner[T any]() *subscriptionOwner[T] {
	return &subscriptionOwner[T]{
		Channel:     eventchannel.New[T](10),
		watcherDone: make(chan struct{}),
	}
}

func (s *subscriptionOwner[T]) watch(ctx context.Context, unsubscribe func()) {
	go func() {
		defer close(s.watcherDone)
		select {
		case <-ctx.Done():
			unsubscribe()
		case <-s.Done():
		}
	}()
}

func (s *subscriptionOwner[T]) wait() { <-s.watcherDone }
