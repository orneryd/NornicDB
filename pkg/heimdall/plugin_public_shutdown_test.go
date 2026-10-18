package heimdall

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type solPausedDispatchPlugin struct {
	*coverageMinimalPlugin
	entered  chan struct{}
	resume   chan struct{}
	received chan *DatabaseEvent
	once     sync.Once
}

func (p *solPausedDispatchPlugin) Name() string {
	p.once.Do(func() {
		close(p.entered)
		<-p.resume
	})
	return "sol-in-flight"
}

func (p *solPausedDispatchPlugin) OnDatabaseEvent(event *DatabaseEvent) { p.received <- event }

// The reviewer's Name barrier pauses the real ingress worker before queue
// creation. Stop must wait for that dispatch and refuse its retired generation.
func TestSolPublicStopOwnsInFlightDispatch(t *testing.T) {
	oldManager, oldDispatcher := globalManager, globalEventDispatcher
	plugin := &solPausedDispatchPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("sol-in-flight"),
		entered:               make(chan struct{}),
		resume:                make(chan struct{}),
		received:              make(chan *DatabaseEvent, 1),
	}
	loaded := &LoadedHeimdallPlugin{Plugin: plugin}
	globalManager = &SubsystemManager{
		plugins:        map[string]*LoadedHeimdallPlugin{"sol-in-flight": loaded},
		initialized:    true,
		orderedPlugins: []*LoadedHeimdallPlugin{loaded},
	}
	dispatcher := &dbEventDispatcher{}
	globalEventDispatcher = dispatcher
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(plugin.resume) }) }
	earlyStop := false
	defer func() {
		release()
		if earlyStop {
			// Baseline cleanup: synchronize with the stranded queue's delivery.
			select {
			case <-plugin.received:
			case <-time.After(5 * time.Second):
			}
		}
		StopEventDispatcher()
		dispatcher.mu.Lock()
		for _, queue := range dispatcher.pluginQueues {
			queue.ch.Close()
		}
		dispatcher.mu.Unlock()
		globalManager, globalEventDispatcher = oldManager, oldDispatcher
	}()
	StartEventDispatcher()
	dispatcher.mu.RLock()
	retired := dispatcher.done
	dispatcher.mu.RUnlock()
	EmitDatabaseEvent(&DatabaseEvent{Type: EventNodeCreated})
	select {
	case <-plugin.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ingress worker did not select event")
	}
	stopped := make(chan struct{})
	go func() {
		StopEventDispatcher()
		close(stopped)
	}()
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not retire its generation")
	}
	// Observe Stop either returning (the baseline bug) or actually parked
	// waiting for ingress. Scheduling is controlled; no sleep decides success.
	deadline := time.Now().Add(5 * time.Second)
	joining := false
	for time.Now().Before(deadline) {
		select {
		case <-stopped:
			earlyStop = true
			t.Fatal("Stop returned before the in-flight ingress dispatch finished")
		default:
		}
		stack := make([]byte, 1<<20)
		stack = stack[:runtime.Stack(stack, true)]
		for _, goroutine := range strings.Split(string(stack), "\n\n") {
			if strings.Contains(goroutine, "(*dbEventDispatcher).stop(") &&
				strings.Contains(strings.SplitN(goroutine, "\n", 2)[0], "[chan receive]") {
				joining = true
				break
			}
		}
		if joining {
			break
		}
		runtime.Gosched()
	}
	if !joining {
		t.Fatal("Stop did not join the ingress worker")
	}
	release()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not finish after ingress was released")
	}
	dispatcher.mu.RLock()
	queues, running := len(dispatcher.pluginQueues), dispatcher.running
	dispatcher.mu.RUnlock()
	if running || queues != 0 {
		t.Fatalf("Stop left running=%v replacement queues=%d", running, queues)
	}
	select {
	case <-plugin.received:
		t.Fatal("retired ingress dispatch delivered to a replacement queue")
	default:
	}
}
