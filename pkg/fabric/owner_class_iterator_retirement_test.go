// Author: Codex (#1636 lane). Public iterator lifetime/outcome population probe.
package fabric_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"sync"
	"testing"
	"time"
)

type classRetirementIterator struct {
	entered, release, returned chan struct{}
	once                       sync.Once
	nextErr, closeErr          error
}

func (b *classRetirementIterator) Next() bool {
	if b.entered != nil {
		close(b.entered)
		<-b.release
		close(b.returned)
	}
	return false
}
func (b *classRetirementIterator) Row() []interface{} { return nil }
func (b *classRetirementIterator) Err() error         { return b.nextErr }
func (b *classRetirementIterator) Close() error {
	if b.release != nil {
		b.once.Do(func() { close(b.release) })
	}
	return b.closeErr
}
func TestOwnerClassIteratorActualCompletion(t *testing.T) {
	for _, kind := range []string{"plain", "converting", "distinct", "concat", "caller-canceled"} {
		t.Run(kind, func(t *testing.T) {
			sentinel := errors.New("iterator native close")
			base := &classRetirementIterator{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}), closeErr: sentinel}
			var inner fabric.RowIterator = base
			switch kind {
			case "converting":
				inner = fabric.NewConvertingRowIterator(base, nil)
			case "distinct":
				inner = fabric.NewDistinctRowIterator(base)
			case "concat":
				inner = fabric.NewConcatRowIterator(base)
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			it := fabric.NewPrefetchRowIterator(parent, inner, 1)
			defer base.Close()
			select {
			case <-base.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("public prefetch did not start")
			}
			if kind == "caller-canceled" {
				cancel()
			}
			closed := make(chan error, 1)
			go func() { closed <- it.Close() }()
			select {
			case err := <-closed:
				if !errors.Is(err, sentinel) {
					t.Errorf("iterator Close omitted native outcome: %v", err)
				}
				select {
				case <-base.returned:
				default:
					t.Error("iterator Close returned before its accepted producer ended")
				}
			case <-time.After(3 * time.Second):
				base.Close()
				t.Fatal("iterator Close did not signal before joining producer")
			}
			base.Close()
			select {
			case <-base.returned:
			case <-time.After(3 * time.Second):
				t.Fatal("producer did not finish controlled escape")
			}
		})
	}
}
func TestOwnerClassIteratorAllOutcomes(t *testing.T) {
	a, b := errors.New("producer A"), errors.New("native close B")
	base := &classRetirementIterator{nextErr: a, closeErr: b}
	it := fabric.NewPrefetchRowIterator(context.Background(), base, 1)
	for it.Next() {
	}
	result := errors.Join(it.Err(), it.Close())
	if !errors.Is(result, a) || !errors.Is(result, b) {
		t.Errorf("prefetch lost producer/close sibling: %v", result)
	}
	c := fabric.NewConcatRowIterator(&classRetirementIterator{closeErr: a}, &classRetirementIterator{closeErr: b})
	result = c.Close()
	if !errors.Is(result, a) || !errors.Is(result, b) {
		t.Errorf("concat lost a sibling close: %v", result)
	}
}

func TestOwnerClassIteratorParentStop(t *testing.T) {
	base := &classRetirementIterator{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := fabric.NewPrefetchRowIterator(parent, base, 1)
	defer base.Close()
	select {
	case <-base.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not enter")
	}
	cancel()
	select {
	case <-base.returned:
	case <-time.After(100 * time.Millisecond):
		base.Close()
		t.Error("parent lifetime stop did not signal the accepted iterator producer")
	}
	if err := it.Close(); err != nil {
		t.Error(err)
	}
}
