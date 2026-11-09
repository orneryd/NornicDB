package testlatch

import (
	"sync"
	"testing"
)

func TestLatchReleaseIsIdempotentAndConcurrentSafe(t *testing.T) {
	l := New()
	select {
	case <-l.Done():
		t.Fatal("latch released before Release")
	default:
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.Release() }()
	}
	wg.Wait()
	l.Release()
	l.Wait()
	<-l.Done()
}
