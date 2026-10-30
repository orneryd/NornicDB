package fabric_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// countingCloseIterator blocks in Next until its first Close and counts how
// many times the native close actually runs.
type countingCloseIterator struct {
	classRetirementIterator
	closes atomic.Int32
}

func (b *countingCloseIterator) Close() error {
	b.closes.Add(1)
	return b.classRetirementIterator.Close()
}

// Shutdown matrix S2, prefetch iterator: concurrent and repeated Close callers
// join the one native close and all receive its outcome.
func TestOwnerMatrixPrefetchConcurrentClose(t *testing.T) {
	sentinel := errors.New("iterator native close")
	base := &countingCloseIterator{classRetirementIterator: classRetirementIterator{
		entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}), closeErr: sentinel,
	}}
	it := fabric.NewPrefetchRowIterator(context.Background(), base, 1)
	select {
	case <-base.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("public prefetch did not start")
	}
	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- it.Close() }()
	}
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, sentinel) {
				t.Errorf("Close caller %d omitted the native outcome: %v", i, err)
			}
			select {
			case <-base.returned:
			default:
				t.Errorf("Close caller %d returned before the producer ended", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent Close did not complete")
		}
	}
	if err := it.Close(); !errors.Is(err, sentinel) {
		t.Errorf("repeat Close = %v, want the same outcome", err)
	}
	if n := base.closes.Load(); n != 1 {
		t.Errorf("native close ran %d times, want exactly once", n)
	}
}
