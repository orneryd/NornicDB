package heimdall

import (
	"sync"
	"testing"
	"time"
)

type solEventHook struct {
	once     sync.Once
	entered  chan struct{}
	resume   chan struct{}
	received chan *DatabaseEvent
}

func (h *solEventHook) OnDatabaseEvent(event *DatabaseEvent) {
	h.once.Do(func() { close(h.entered); <-h.resume })
	h.received <- event
}

// Joining ingress must not make Stop wait for a plugin callback. Its already
// accepted buffered events remain owned by the closing queue and drain in order.
func TestSolPluginFIFOBackpressure(t *testing.T) {
	dispatcher := &dbEventDispatcher{}
	dispatcher.start()
	defer dispatcher.stop()
	hook := &solEventHook{
		entered:  make(chan struct{}),
		resume:   make(chan struct{}),
		received: make(chan *DatabaseEvent, defaultDatabaseEventQueue+2),
	}
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(hook.resume) }) }
	defer release()
	first := &DatabaseEvent{Type: EventNodeCreated, NodeID: "first"}
	dispatcher.enqueueEvent("slow", hook, first)
	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not enter hook")
	}
	want := []*DatabaseEvent{first}
	finished := make(chan struct{})
	go func() {
		for range defaultDatabaseEventQueue {
			event := &DatabaseEvent{Type: EventNodeCreated}
			want = append(want, event)
			dispatcher.enqueueEvent("slow", hook, event)
		}
		dispatcher.enqueueEvent("slow", hook, &DatabaseEvent{Type: EventNodeCreated, NodeID: "overflow"})
		dispatcher.stop()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("full queue or Stop waited for the blocked plugin hook")
	}
	release()
	for i, expected := range want {
		select {
		case actual := <-hook.received:
			if actual != expected {
				t.Fatalf("event %d reordered", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("buffered event %d lost at Stop", i)
		}
	}
	select {
	case <-hook.received:
		t.Fatal("overflow was delivered")
	default:
	}
}
