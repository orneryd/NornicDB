package testfixture

import (
	"sync"
	"testing"
	"time"
)

// hookSlot is a local stand-in for a package's installable test hook. install
// swaps the hook and returns the swap-back, so the fixture's installation and
// restoration are observable without any dependency on the package that owns
// the hook.
type hookSlot struct {
	mu       sync.Mutex
	hook     func()
	restores int
}

func (s *hookSlot) install(hook func()) func() {
	s.mu.Lock()
	previous := s.hook
	s.hook = hook
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.hook = previous
		s.restores++
		s.mu.Unlock()
	}
}

// fire runs the installed hook, if any, the way the observed package would.
func (s *hookSlot) fire() {
	s.mu.Lock()
	hook := s.hook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func TestObserveHookSignal(t *testing.T) {
	t.Run("installs the reporting callback", func(t *testing.T) {
		slot := &hookSlot{}
		signals := ObserveHookSignal(t, slot.install, 2)
		if cap(signals) != 2 {
			t.Fatalf("buffer capacity = %d, want 2", cap(signals))
		}
		slot.fire()
		slot.fire()
		if len(signals) != 2 {
			t.Fatalf("buffered %d reports, want 2", len(signals))
		}
	})

	t.Run("saturating the buffer never blocks the hook", func(t *testing.T) {
		slot := &hookSlot{}
		signals := ObserveHookSignal(t, slot.install, 1)
		fired := make(chan struct{})
		go func() {
			slot.fire()
			slot.fire()
			slot.fire()
			close(fired)
		}()
		select {
		case <-fired:
		case <-time.After(5 * time.Second):
			t.Fatal("a report blocked on a full buffer")
		}
		if len(signals) != 1 {
			t.Fatalf("buffered %d reports, want 1", len(signals))
		}
		<-signals
		if len(signals) != 0 {
			t.Fatalf("%d reports remain after draining the single buffered report", len(signals))
		}
	})

	t.Run("cleanup restores the previous hook", func(t *testing.T) {
		slot := &hookSlot{}
		var baseline int
		// A hook installed before the observed one must be back afterwards.
		slot.install(func() { baseline++ })
		t.Run("observing subtest", func(t *testing.T) {
			signals := ObserveHookSignal(t, slot.install, 1)
			slot.fire()
			if len(signals) != 1 {
				t.Fatalf("buffered %d reports, want 1", len(signals))
			}
			if baseline != 0 {
				t.Fatalf("the reporting hook did not replace the previous hook: baseline ran %d times", baseline)
			}
		})
		if slot.restores != 1 {
			t.Fatalf("restores = %d, want 1 after the observed test ended", slot.restores)
		}
		slot.fire()
		if baseline != 1 {
			t.Fatalf("baseline hook ran %d times after restoration, want 1", baseline)
		}
	})
}
