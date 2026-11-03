package heimdall

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/stretchr/testify/require"
)

// publicIngressPlugin pauses the ingress worker inside the per-plugin enrollment
// call (Name) and then pauses its delivery hook, so the test controls both
// boundaries without sleeping.
type publicIngressPlugin struct {
	*coverageMinimalPlugin
	nameOnce    sync.Once
	nameEntered chan struct{}
	nameResume  chan struct{}
	hookEntered chan struct{}
	hookResume  chan struct{}
	delivered   chan *DatabaseEvent
}

func (p *publicIngressPlugin) Name() string {
	p.nameOnce.Do(func() {
		close(p.nameEntered)
		<-p.nameResume
	})
	return p.coverageMinimalPlugin.Name()
}

func (p *publicIngressPlugin) OnDatabaseEvent(event *DatabaseEvent) {
	close(p.hookEntered)
	<-p.hookResume
	p.delivered <- event
}

// TestPublicEmitDatabaseEventDeliversAcceptedIngressAfterHostStop proves the
// public EmitDatabaseEvent path retains the admitted ingress context: the host
// closes root admission after ingress accepted the event but before any plugin
// enrolled, that accepted event still enrolls and delivers its child after root
// admission is refused, and host quiescence waits for the blocked hook to
// actually return. If ingress replaced its admitted context with
// context.Background(), the child enrollment would be refused and the event
// would never reach the hook.
func TestPublicEmitDatabaseEventDeliversAcceptedIngressAfterHostStop(t *testing.T) {
	oldManager, oldDispatcher := globalManager, globalEventDispatcher

	const name = "public-ingress"
	owner := lifecycle.NewWorkLifetime(context.Background())
	plugin := &publicIngressPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin(name),
		nameEntered:           make(chan struct{}),
		nameResume:            make(chan struct{}),
		hookEntered:           make(chan struct{}),
		hookResume:            make(chan struct{}),
		delivered:             make(chan *DatabaseEvent, 1),
	}
	loaded := &LoadedHeimdallPlugin{Plugin: plugin}
	manager := &SubsystemManager{
		plugins:        map[string]*LoadedHeimdallPlugin{name: loaded},
		actions:        make(map[string]ActionFunc),
		initialized:    true,
		orderedPlugins: []*LoadedHeimdallPlugin{loaded},
	}
	manager.SetHostContext(SubsystemHost{Config: Config{}, Lifetime: owner})

	dispatcher := &dbEventDispatcher{}
	globalManager, globalEventDispatcher = manager, dispatcher

	var nameRelease, hookRelease sync.Once
	releaseName := func() { nameRelease.Do(func() { close(plugin.nameResume) }) }
	releaseHook := func() { hookRelease.Do(func() { close(plugin.hookResume) }) }
	hostStopping := false

	// Cleanup joins this test's worker and host stop even when an
	// assertion failed, without sleeping: every gate is released first, then the
	// already-retired dispatcher and the delivery worker are joined.
	t.Cleanup(func() {
		releaseName()
		releaseHook()
		StopEventDispatcher()
		dispatcher.mu.RLock()
		queue := loaded.eventQueue
		dispatcher.mu.RUnlock()
		if queue != nil {
			select {
			case <-queue.worker.Done():
			case <-time.After(5 * time.Second):
				t.Error("the plugin delivery worker did not finish")
			}
		}
		if hostStopping {
			select {
			case <-owner.Stopped():
			case <-time.After(5 * time.Second):
				t.Error("the host lifetime did not finish its stop")
			}
		}
		globalManager, globalEventDispatcher = oldManager, oldDispatcher
	})

	StartEventDispatcher()

	event := &DatabaseEvent{Type: EventNodeCreated, NodeID: "public-ingress"}
	EmitDatabaseEvent(event)
	select {
	case <-plugin.nameEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted ingress never reached per-plugin enrollment")
	}

	// The host stops while its accepted event is still only in ingress.
	hostStopping = true
	owner.BeginStop(nil)
	_, _, accepted := manager.acquireWork(context.Background())
	require.False(t, accepted, "root admission stayed open after the host began stopping")

	releaseName()
	select {
	case <-plugin.hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted ingress did not enroll its child delivery after root admission closed")
	}

	// The blocked hook still owns host quiescence.
	select {
	case <-owner.Quiescent():
		t.Fatal("host quiescence ignored a blocked accepted delivery")
	default:
	}

	releaseHook()
	select {
	case delivered := <-plugin.delivered:
		require.Same(t, event, delivered)
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted event was not delivered to its child")
	}
	select {
	case <-owner.Quiescent():
	case <-time.After(5 * time.Second):
		t.Fatal("host quiescence did not complete after the hook returned")
	}
	require.Equal(t, 0, owner.Active(), "an accepted delivery left host work unreleased")
}
