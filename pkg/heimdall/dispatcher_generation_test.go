package heimdall

import (
	"testing"
	"time"
)

func TestDispatcherRetiredGenerationCannotCreateQueue(t *testing.T) {
	dispatcher := &dbEventDispatcher{}
	dispatcher.start()
	t.Cleanup(dispatcher.stop)
	generation := dispatcher.done
	hook := &coverageEventPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("generation"),
		events:                make(chan *DatabaseEvent, 1),
	}
	dispatcher.stop()
	dispatcher.enqueueEvent(hook.Name(), hook, &DatabaseEvent{Type: EventNodeCreated})
	dispatcher.ensurePluginQueueForPlugin(&LoadedHeimdallPlugin{Plugin: hook})
	if len(dispatcher.pluginQueues) != 0 {
		t.Fatal("stopped dispatcher created a queue")
	}
	dispatcher.start()
	stale := &DatabaseEvent{Type: EventNodeDeleted}
	dispatcher.enqueueEventForGeneration(hook.Name(), hook, stale, generation)
	if len(dispatcher.pluginQueues) != 0 {
		t.Fatal("retired generation attached a queue to the restarted dispatcher")
	}
	current := &DatabaseEvent{Type: EventNodeUpdated}
	dispatcher.enqueueEvent(hook.Name(), hook, current)
	dispatcher.stop()
	select {
	case event := <-hook.events:
		if event != current {
			t.Fatal("restarted dispatcher delivered a stale event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("current generation lost its queued event at Stop")
	}
}
