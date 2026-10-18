package heimdall

import (
	"testing"
	"time"
)

func TestEventQueue_PublishAfterSnapshotClosed(t *testing.T) {
	dispatcher := &dbEventDispatcher{}
	dispatcher.start()
	t.Cleanup(dispatcher.stop)
	hook := &coverageEventPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("snapshot-close"),
		events:                make(chan *DatabaseEvent, 1),
	}
	copied := make(chan struct{})
	resume := make(chan struct{})
	dispatcher.afterQueueSnapshot = func() {
		close(copied)
		<-resume
	}
	finished := make(chan any, 1)
	go func() {
		defer func() { finished <- recover() }()
		dispatcher.enqueueEvent(hook.Name(), hook, &DatabaseEvent{Type: EventNodeCreated})
	}()
	select {
	case <-copied:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not reach queue snapshot barrier")
	}
	dispatcher.stop()
	close(resume)
	select {
	case panicValue := <-finished:
		if panicValue != nil {
			t.Fatalf("publisher panicked after queue closed: %v", panicValue)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publisher blocked on stopped plugin queue")
	}
	select {
	case <-hook.events:
		t.Fatal("stopped queue delivered a late event")
	default:
	}
}
