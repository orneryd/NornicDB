package fabric

import (
	"context"
	"testing"
	"time"
)

// closeSignalRowIterator reports when the prefetch producer closes it.
type closeSignalRowIterator struct {
	RowIterator
	closed chan struct{}
}

func (it *closeSignalRowIterator) Close() error {
	close(it.closed)
	return it.RowIterator.Close()
}

// A canceled prefetch stops producing while its buffer is full and closes the
// underlying iterator.
func TestPrefetchRowIterator_StopsWhenCanceledWithFullBuffer(t *testing.T) {
	base := &closeSignalRowIterator{
		RowIterator: NewResultRowIterator(&ResultStream{Columns: []string{"a"}, Rows: [][]interface{}{{1}, {2}, {3}}}),
		closed:      make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	it := NewPrefetchRowIterator(ctx, base, 1)
	cancel()
	select {
	case <-base.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch producer didn't stop after cancel")
	}
	_ = it.Close()
}
