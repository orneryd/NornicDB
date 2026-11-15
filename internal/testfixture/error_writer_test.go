package testfixture

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestErrorWriterConcurrentAttempts(t *testing.T) {
	const workers, attempts = 32, 100
	failure := errors.New("test-owned sink failure")
	writer := &ErrorWriter{Err: failure}
	var badResults atomic.Int64
	var done sync.WaitGroup
	for range workers {
		done.Go(func() {
			for range attempts {
				n, err := writer.Write([]byte("record"))
				if n != 0 || err != failure {
					badResults.Add(1)
				}
			}
		})
	}
	done.Wait()
	if n := badResults.Load(); n != 0 {
		t.Errorf("%d writes changed accepted bytes or error identity", n)
	}
	if got := writer.Calls.Load(); got != workers*attempts {
		t.Errorf("attempt count = %d, want %d", got, workers*attempts)
	}
}
