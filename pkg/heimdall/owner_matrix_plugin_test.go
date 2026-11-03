package heimdall

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// initWaitsForStopPlugin's Initialize waits for a private worker that only its
// documented Stop releases (the be90 F2 shape).
type initWaitsForStopPlugin struct {
	*coverageMinimalPlugin
	entered  chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	stops    atomic.Int32
}

func (p *initWaitsForStopPlugin) Initialize(SubsystemContext) error {
	close(p.entered)
	<-p.stop
	return nil
}

func (p *initWaitsForStopPlugin) Stop() error {
	p.stops.Add(1)
	p.stopOnce.Do(func() { close(p.stop) })
	return nil
}

func newMatrixSubsystemManager() *SubsystemManager {
	return &SubsystemManager{
		plugins:    make(map[string]*LoadedHeimdallPlugin),
		actions:    make(map[string]ActionFunc),
		orderDirty: true,
	}
}

// Shutdown matrix S4, SubsystemManager legacy RegisterPlugin (the be90 F2
// reproduction): CloseAdmission then StopAll signals a plugin still inside
// Initialize, so StopAll returns instead of waiting on it forever.
func TestOwnerMatrixRegisterPluginInitializerIsSignalled(t *testing.T) {
	for _, entry := range []string{"RegisterPlugin", "RegisterStartedPlugin"} {
		t.Run(entry, func(t *testing.T) {
			m := newMatrixSubsystemManager()
			p := &initWaitsForStopPlugin{coverageMinimalPlugin: newCoverageMinimalPlugin("f2-" + entry), entered: make(chan struct{}), stop: make(chan struct{})}
			registered := make(chan error, 1)
			go func() {
				if entry == "RegisterPlugin" {
					registered <- m.RegisterPlugin(p, "", false)
				} else {
					registered <- m.RegisterStartedPlugin(t.Context(), p, "", false)
				}
			}()
			select {
			case <-p.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("Initialize did not start")
			}
			m.CloseAdmission()
			stopped := make(chan error, 1)
			go func() { stopped <- m.StopAll() }()
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				p.Stop()
				t.Fatal("StopAll waited on an initializing plugin it never signalled")
			}
			require.GreaterOrEqual(t, p.stops.Load(), int32(1), "the initializing plugin was not stopped")
			select {
			case <-registered:
			case <-time.After(3 * time.Second):
				t.Fatal("registration did not return after its plugin was stopped")
			}
			// The refused registration's outcome is reported by the stop; the
			// stop must return, whatever it reports.
			shut := make(chan struct{})
			go func() { _ = m.ShutdownAll(); close(shut) }()
			select {
			case <-shut:
			case <-time.After(3 * time.Second):
				t.Fatal("ShutdownAll did not return")
			}
		})
	}
}
