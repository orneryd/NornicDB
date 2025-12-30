package gpu

import (
	"runtime"
	"sync"
	"testing"
)

func TestEmbeddingIndex_ScoreSubset_ConcurrentRemoveDoesNotPanic(t *testing.T) {
	runtime.GOMAXPROCS(2)

	dims := 4
	idx := NewEmbeddingIndex(nil, &EmbeddingIndexConfig{
		Dimensions:     dims,
		InitialCap:     2,
		GPUEnabled:     false,
		AutoSync:       false,
		BatchThreshold: 0,
	})

	if err := idx.Add("a", []float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("Add(a) error: %v", err)
	}
	if err := idx.Add("b", []float32{0, 1, 0, 0}); err != nil {
		t.Fatalf("Add(b) error: %v", err)
	}

	query := []float32{0, 1, 0, 0}
	ids := []string{"b"}

	var wg sync.WaitGroup
	panicCh := make(chan any, 1)
	errCh := make(chan error, 1)

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				select {
				case panicCh <- r:
				default:
				}
			}
		}()

		for i := 0; i < 50000; i++ {
			results, err := idx.ScoreSubset(query, ids)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if len(results) > 1 {
				select {
				case errCh <- errUnexpectedResultsLen:
				default:
				}
				return
			}
			if len(results) == 1 && results[0].ID != "b" {
				select {
				case errCh <- errUnexpectedResultID:
				default:
				}
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				select {
				case panicCh <- r:
				default:
				}
			}
		}()

		vec := []float32{0, 1, 0, 0}
		for i := 0; i < 50000; i++ {
			idx.Remove("b")
			_ = idx.Add("b", vec)
		}
	}()

	wg.Wait()

	select {
	case p := <-panicCh:
		t.Fatalf("panic: %v", p)
	default:
	}
	select {
	case err := <-errCh:
		t.Fatalf("error: %v", err)
	default:
	}
}

var (
	errUnexpectedResultsLen = &testErr{s: "unexpected results length"}
	errUnexpectedResultID  = &testErr{s: "unexpected result id"}
)

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }

