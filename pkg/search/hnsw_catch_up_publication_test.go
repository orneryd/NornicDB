package search

// Author: hnsw-stall implementation lane (Claude Opus 5.5), 9 Nov 2026.
// TestHNSWCatchUpPublicationDoesNotDrainBacklogUnderLocks began as the
// independent review's (r1) TestReview1773PublisherDoesNotDrainBacklogWhileWaiting.
// It now reads the publication step's result before the held insert completes
// (Claude Fable 5.2, 10 Nov 2026), so a step that waits for that insert fails
// the test whatever it then returns.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The step that publication runs while it holds the global locks waits for
// nothing. One change is kept between its pop and its insert, in flight, and
// 100 more are pending. The step must return while that insert is still in
// flight, refusing to publish and applying nothing.
func TestHNSWCatchUpPublicationDoesNotDrainBacklogUnderLocks(t *testing.T) {
	var c hnswCatchUp
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	c.begin(graph, map[string][]float32{"initial": {1, 0}}, nil)
	// On every exit the catch-up is detached, which lets a step that waits
	// return, and the publisher is then joined.
	var publisher sync.WaitGroup
	defer publisher.Wait()
	defer c.end()
	c.mu.Lock()
	id, value, ok := c.popLocked()
	c.mu.Unlock()
	require.True(t, ok)
	const backlog = 100
	for i := 0; i < backlog; i++ {
		require.True(t, c.liveUpdate(fmt.Sprintf("pending-%d", i), []float32{0, 1}))
	}
	publication := make(chan bool, 1)
	publisher.Go(func() { finished, _ := c.finishLocked(); publication <- finished })
	select {
	case finished := <-publication:
		require.False(t, finished, "publication finished with an insert in flight and %d changes pending", backlog)
	case <-time.After(catchUpPrompt):
		t.Fatal("the step that publication runs under the locks waits for an insert in flight")
	}
	require.Equal(t, 0, graph.Size(), "a refused publication applied changes")
	// The insert completes afterwards; the catch-up is still usable.
	require.NoError(t, applyCatchUpChange(graph, id, value))
	c.mu.Lock()
	c.completeLocked(id, nil)
	c.mu.Unlock()
	c.pay(backlog)
	require.Equal(t, 1+backlog, graph.Size())
}

// Publication quiesces the catch-up before it takes the global locks: payers
// stop inserting (they wait, holding no lock) and the inserts in flight
// finish. Under the locks it waits for nothing and applies nothing while more
// than the tail is pending; resuming lets the payers pay, after which
// publication succeeds.
func TestHNSWCatchUpPublicationQuiescesPaymentsOutsideTheLocks(t *testing.T) {
	var c hnswCatchUp
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	c.begin(graph, map[string][]float32{"initial": {1, 0}}, nil)
	// On every exit the catch-up is detached, which lets a quiescing publisher
	// and a waiting payer return, and both are then joined.
	var waiters sync.WaitGroup
	defer waiters.Wait()
	defer c.end()
	c.mu.Lock()
	id, value, ok := c.popLocked() // an insert in flight
	c.mu.Unlock()
	require.True(t, ok)
	const backlog = 100
	for i := 0; i < backlog; i++ {
		require.True(t, c.liveUpdate(fmt.Sprintf("pending-%d", i), []float32{0, 1}))
	}
	quiesced := make(chan bool, 1)
	waiters.Go(func() { quiesced <- c.quiesce() })
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.publishing
	}, 5*time.Second, time.Millisecond)
	paid := make(chan struct{})
	waiters.Go(func() { c.pay(backlog); close(paid) })
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, graph.Size(), "a payer inserted while publication was quiescing")
	select {
	case <-quiesced:
		t.Fatal("quiesce returned with an insert in flight")
	case <-paid:
		t.Fatal("the payer returned without paying")
	default:
	}
	require.NoError(t, applyCatchUpChange(graph, id, value))
	c.mu.Lock()
	c.completeLocked(id, nil)
	c.mu.Unlock()
	require.True(t, <-quiesced)
	// What publication does under the global locks: more than the tail is
	// pending, so it applies nothing and reports false.
	finished, err := c.finishLocked()
	require.NoError(t, err)
	require.False(t, finished)
	require.Equal(t, 1, graph.Size(), "publication applied changes beyond its tail")
	c.resume()
	select {
	case <-paid:
	case <-time.After(5 * time.Second):
		t.Fatal("the payer did not resume")
	}
	require.Equal(t, 1+backlog, graph.Size())
	require.True(t, c.quiesce())
	finished, err = c.finishLocked()
	require.NoError(t, err)
	require.True(t, finished)
	require.False(t, c.liveUpdate("late", []float32{1, 1}), "a finished catch-up takes no changes")
}
