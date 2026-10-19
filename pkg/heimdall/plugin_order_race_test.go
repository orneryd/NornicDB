package heimdall

import (
	"runtime"
	"sync"
	"testing"
)

type yieldingOrderedPlugin struct {
	*MockHeimdallPlugin
}

func (p *yieldingOrderedPlugin) Priority() int {
	// Keep the first cache fill in progress long enough for every reader that
	// started at the barrier to enter the same read-locked path.
	for range 100 {
		runtime.Gosched()
	}
	return 0
}

func (p *yieldingOrderedPlugin) Before() []string { return nil }
func (p *yieldingOrderedPlugin) After() []string  { return nil }

func TestSubsystemManagerConcurrentOrderCacheFill(t *testing.T) {
	plugin := &yieldingOrderedPlugin{MockHeimdallPlugin: NewMockPlugin("ordered")}
	manager := &SubsystemManager{
		plugins: map[string]*LoadedHeimdallPlugin{
			"ordered": {Plugin: plugin, Builtin: true},
		},
		actions:    make(map[string]ActionFunc),
		orderDirty: true,
	}

	globalManagerMu.Lock()
	previous := globalManager
	globalManager = manager
	globalManagerMu.Unlock()
	t.Cleanup(func() {
		globalManagerMu.Lock()
		globalManager = previous
		globalManagerMu.Unlock()
	})

	const readers = 32
	start := make(chan struct{})
	results := make(chan []*LoadedHeimdallPlugin, readers)
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			<-start
			results <- ListHeimdallPlugins()
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for plugins := range results {
		if len(plugins) != 1 || plugins[0].Plugin != plugin {
			t.Fatalf("unexpected ordered plugin snapshot: %#v", plugins)
		}
	}
}
