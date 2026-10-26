package storage

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// CapabilityWork measures actual structural discovery and type checks during
// an opt-in qualification run. It does not estimate work from row counts.
type CapabilityWork struct {
	ProviderVisits, TypeChecks atomic.Uint64
	Trace                      bool
	mu                         sync.Mutex
	sites                      map[string]uint64
}

func (w *CapabilityWork) record(kind string) {
	if !w.Trace {
		return
	}
	var pcs [12]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.Function, "engineCapability[") && !strings.Contains(frame.Function, "OwnedCapability[") {
			names = append(names, frame.Function)
		}
		if !more || len(names) == 4 {
			break
		}
	}
	key := kind + ":" + strings.Join(names, " <- ")
	w.mu.Lock()
	if w.sites == nil {
		w.sites = map[string]uint64{}
	}
	w.sites[key]++
	w.mu.Unlock()
}
func (w *CapabilityWork) Sites() map[string]uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]uint64{}
	for key, value := range w.sites {
		out[key] = value
	}
	return out
}

var capabilityWorkObserver atomic.Pointer[CapabilityWork]

// ObserveCapabilityWork installs one process-local observer. The caller must
// serialize observations and release it after all work in the schedule joins.
func ObserveCapabilityWork(work *CapabilityWork) func() {
	previous := capabilityWorkObserver.Swap(work)
	return func() { capabilityWorkObserver.Store(previous) }
}
func capabilityTypeCheck() {
	if work := capabilityWorkObserver.Load(); work != nil {
		work.TypeChecks.Add(1)
		work.record("type")
	}
}
func capabilityProviderVisit() {
	if work := capabilityWorkObserver.Load(); work != nil {
		work.ProviderVisits.Add(1)
		work.record("provider")
	}
}
