// Author: Codex (#1636 lane). Public reader, callback and actual-disposal class.
package search_test

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync/atomic"
	"testing"
	"time"
)

type classReaderReranker struct {
	name      func() string
	available func(context.Context) bool
	run       func(context.Context)
}

func (r *classReaderReranker) Name() string {
	if r.name != nil {
		return r.name()
	}
	return "class-reader"
}
func (r *classReaderReranker) Enabled() bool { return true }
func (r *classReaderReranker) IsAvailable(ctx context.Context) bool {
	if r.available != nil {
		return r.available(ctx)
	}
	return true
}
func (r *classReaderReranker) Rerank(ctx context.Context, _ string, _ []search.RerankCandidate) ([]search.RerankResult, error) {
	r.run(ctx)
	return nil, ctx.Err()
}

func TestOwnerClassSearchReaderCallbackReentry(t *testing.T) {
	for _, name := range []string{"name", "available"} {
		t.Run(name, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			defer engine.Close()
			svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
			defer svc.Close()
			child := make(chan struct{}, 1)
			var blocked atomic.Bool
			callback := func() {
				go func() { svc.SetReranker(nil); child <- struct{}{} }()
				select {
				case <-child:
					child <- struct{}{}
				case <-time.After(100 * time.Millisecond):
					blocked.Store(true)
				}
			}
			r := &classReaderReranker{name: func() string { callback(); return "reentrant" }, available: func(context.Context) bool { callback(); return true }}
			svc.SetReranker(r)
			if name == "name" {
				svc.RerankerName()
			} else {
				svc.RerankerAvailable(context.Background())
			}
			select {
			case <-child:
			case <-time.After(3 * time.Second):
				t.Fatal("reader callback did not finish after controlled escape")
			}
			if blocked.Load() {
				t.Error("service state lock held across a foreign reader callback")
			}
		})
	}
}

func TestOwnerClassSearchReaderActualCompletion(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer engine.Close()
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	entered, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	svc.SetReranker(&classReaderReranker{run: func(ctx context.Context) {
		close(entered)
		select {
		case <-ctx.Done():
			close(canceled)
		case <-time.After(time.Second):
		}
		<-release
	}})
	called := make(chan struct{})
	go func() {
		defer close(called)
		_, _ = svc.RerankCandidates(context.Background(), "lifetime", []search.RerankCandidate{{ID: "n", Content: "lifetime"}}, nil)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("public rerank did not enter provider")
	}
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()
	early := false
	select {
	case <-closed:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-canceled:
	case <-time.After(1200 * time.Millisecond):
		t.Error("service stop did not reach accepted reader context")
	}
	close(release)
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not return")
	}
	if !early {
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not join actual reader completion")
		}
	}
	if early {
		t.Error("service disposed before its accepted reader returned")
	}
}
