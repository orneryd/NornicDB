// Package eventchannel owns buffered, nonblocking event delivery and closure.
package eventchannel

import "sync"

// Channel serializes sends with Close so publishers may safely retain an owner
// after a subscriber is removed from a broker's list. Do not copy a Channel.
type Channel[T any] struct {
	mu     sync.Mutex
	events chan T
	closed bool
}

// New creates a channel with the requested event buffer capacity.
//
//	queue := eventchannel.New[string](10)
//	queue.TrySend("created")
//	event := <-queue.Events()
func New[T any](capacity int) *Channel[T] {
	return &Channel[T]{events: make(chan T, capacity)}
}

// Events returns the receive-only event stream. Closing the owner preserves
// buffered events for readers, followed by the usual closed-channel indication.
func (c *Channel[T]) Events() <-chan T { return c.events }

// TrySend queues an event without waiting for a reader. It returns false when
// the buffer is full or the owner is closed, leaving existing events intact.
func (c *Channel[T]) TrySend(event T) bool {
	// A full buffer can reject immediately: no send can race with Close.
	// Unbuffered channels still need the select to find a waiting reader.
	if cap(c.events) > 0 && len(c.events) == cap(c.events) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	select {
	case c.events <- event:
		return true
	default:
		return false
	}
}

// Close ends delivery, including from stale publisher snapshots. It is safe to
// call repeatedly or concurrently with TrySend; it never waits for a reader.
func (c *Channel[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.events)
	}
}
