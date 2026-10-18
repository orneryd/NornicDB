package eventchannel

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestChannelBufferedDelivery(t *testing.T) {
	queue := New[int](2)
	if !queue.TrySend(1) || !queue.TrySend(2) || queue.TrySend(3) {
		t.Fatal("expected two queued events and one dropped overflow")
	}
	queue.Close()
	queue.Close()
	if queue.TrySend(4) {
		t.Fatal("closed owner accepted an event")
	}
	for _, want := range []int{1, 2} {
		if got, open := <-queue.Events(); !open || got != want {
			t.Fatalf("buffered event = (%d, %v), want %d", got, open, want)
		}
	}
	if _, open := <-queue.Events(); open {
		t.Fatal("event stream remains open")
	}
}

func TestChannelConcurrentSendClose(t *testing.T) {
	queue := New[int](64)
	start := make(chan struct{})
	var accepted atomic.Int64
	var workers sync.WaitGroup
	for event := range 64 {
		workers.Go(func() {
			<-start
			if queue.TrySend(event) {
				accepted.Add(1)
			}
		})
	}
	for range 4 {
		workers.Go(func() {
			<-start
			queue.Close()
		})
	}
	close(start)
	workers.Wait()
	received := 0
	for range queue.Events() {
		received++
	}
	if int64(received) != accepted.Load() {
		t.Fatalf("received %d events, accepted %d", received, accepted.Load())
	}
	if queue.TrySend(65) {
		t.Fatal("stale publisher sent after Close completed")
	}
}
