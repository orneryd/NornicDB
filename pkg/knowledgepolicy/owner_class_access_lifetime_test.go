// Author: Codex (#1636 lane). Public worker-generation and outcome class.
package knowledgepolicy_test

import (
	"context"
	"errors"
	kp "github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type classAccessStore struct {
	entered, release  chan struct{}
	once              sync.Once
	readErr, writeErr error
	writes            atomic.Int32
}

func (s *classAccessStore) ReadAccessMeta(r kp.AccessMetaReadRequest) (kp.AccessMetaReadResult, error) {
	if r.EntityID == "ns:read" {
		return kp.AccessMetaReadResult{}, s.readErr
	}
	return kp.AccessMetaReadResult{}, nil
}
func (s *classAccessStore) PutAccessMeta(string, *kp.AccessMetaEntry) error {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	s.writes.Add(1)
	return s.writeErr
}
func classAccessStop(f *kp.AccessFlusher) error {
	if owner, ok := any(f).(interface{ Stop() error }); ok {
		return owner.Stop()
	}
	f.Stop()
	return nil
}

type classAccessContext struct {
	context.Context
	once   sync.Once
	onDone func()
}

func (c *classAccessContext) Done() <-chan struct{} { c.once.Do(c.onDone); return nil }
func TestOwnerClassAccessLifetimeConstruction(t *testing.T) {
	acc := kp.NewAccessAccumulator(true, 0)
	f := kp.NewAccessFlusher(acc, &classAccessStore{}, time.Hour)
	child := make(chan struct{})
	var blocked atomic.Bool
	parent := &classAccessContext{Context: context.Background()}
	parent.onDone = func() {
		go func() { f.Start(context.Background()); close(child) }()
		select {
		case <-child:
		case <-time.After(100 * time.Millisecond):
			blocked.Store(true)
		}
	}
	f.Start(parent)
	select {
	case <-child:
	case <-time.After(3 * time.Second):
		t.Fatal("reentrant Start did not return after controlled construction escape")
	}
	if err := classAccessStop(f); err != nil {
		t.Error(err)
	}
	if blocked.Load() {
		t.Error("flusher holds its host lock across foreign context construction")
	}
}
func TestOwnerClassAccessLifetimeCompletion(t *testing.T) {
	acc := kp.NewAccessAccumulator(true, 0)
	store := &classAccessStore{entered: make(chan struct{}), release: make(chan struct{})}
	f := kp.NewAccessFlusher(acc, store, time.Hour)
	f.Start(context.Background())
	acc.IncrementAccess("ns:write")
	stopped := make(chan error, 1)
	go func() { stopped <- classAccessStop(f) }()
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		close(store.release)
		t.Fatal("final flush did not enter native store")
	}
	later, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.Start(later)
	close(store.release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("a new Start replaced the unfinished worker's completion owner")
		return
	}
	// A subsequent real generation must also start and stop normally.
	f.Start(context.Background())
	if err := classAccessStop(f); err != nil {
		t.Error(err)
	}
}
func TestOwnerClassAccessLifetimeOutcomes(t *testing.T) {
	a, b := errors.New("native access read A"), errors.New("native access write B")
	store := &classAccessStore{readErr: a, writeErr: b}
	acc := kp.NewAccessAccumulator(true, 0)
	f := kp.NewAccessFlusher(acc, store, time.Hour)
	acc.IncrementAccess("ns:read")
	acc.IncrementAccess("ns:write")
	f.Start(context.Background())
	err := classAccessStop(f)
	if !errors.Is(err, a) || !errors.Is(err, b) {
		t.Errorf("final access flush omitted sibling completion outcomes: %v", err)
	}
	if store.writes.Load() == 0 {
		t.Error("native write outcome was not exercised")
	}
}

type classAccessStopContext struct {
	context.Context
	calls  atomic.Int32
	onStop func()
}

func (c *classAccessStopContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 2 {
		c.onStop()
	}
	return nil
}
func TestOwnerClassAccessLifetimeLateDispatch(t *testing.T) {
	// One P, a fresh scheduling quantum and no GC keep Start-to-Stop in one
	// synchronous admission sequence; the watchdog is only a failure bound.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	f := kp.NewAccessFlusher(kp.NewAccessAccumulator(true, 0), &classAccessStore{}, time.Hour)
	parent := &classAccessStopContext{Context: context.Background()}
	parent.onStop = func() { f.Start(context.Background()) }
	completed := make(chan struct{})
	go func() {
		select {
		case <-completed:
		case <-time.After(time.Second):
			panic("access lifetime: Stop joined a completion signal replaced before worker dispatch")
		}
	}()
	runtime.Gosched()
	f.Start(parent)
	err := classAccessStop(f)
	close(completed)
	if err != nil {
		t.Error(err)
	}
	if parent.calls.Load() < 2 {
		t.Error("public stop did not exercise the foreign cancellation boundary")
	}
}
