package heimdall

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/observability"
)

func pluginStopError(p HeimdallPlugin) error {
	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop %s: %w", p.Name(), err)
	}
	return nil
}

func (p *LoadedHeimdallPlugin) signalStop() error {
	return p.stopOnce.Do(func() error { return pluginStopError(p.Plugin) })
}

func (p *LoadedHeimdallPlugin) dispose() error {
	return p.disposeOnce.Do(func() error {
		if queue := globalEventDispatcher.detachPluginQueue(p); queue != nil {
			<-queue.done
		}
		if err := p.Plugin.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown %s: %w", p.Plugin.Name(), err)
		}
		return nil
	})
}

// The process dispatcher outlives a host. Detach only this loaded component's
// queue, retaining its completion even if process Stop already removed the map.
func (d *dbEventDispatcher) detachPluginQueue(p *LoadedHeimdallPlugin) *pluginEventQueue {
	d.mu.Lock()
	defer d.mu.Unlock()
	p.eventRetired = true
	queue := p.eventQueue
	if queue == nil {
		return nil
	}
	if d.pluginQueues[queue.name] == queue {
		delete(d.pluginQueues, queue.name)
	}
	queue.ch.Close()
	return queue
}

// No host caller can reach an unpublished instance. Its stop operation signals
// and joins private workers before Shutdown disposes it, just as normal phases do.
func cleanupUnpublishedPlugin(component *LoadedHeimdallPlugin, cfg Config) error {
	stopped := component.signalStop()
	disposed := component.dispose()
	return observability.RedactError(compose.Outcomes(stopped, disposed), observability.ClientCredentials(cfg.APIURL, cfg.APIKey)...)
}

// OnActionsRemoved registers a consumer which removes exported tools when their
// owning actions are unregistered. Hooks run outside the manager lock and are
// retired by ShutdownAll, before plugins can register their replacements.
func (m *SubsystemManager) OnActionsRemoved(hook func([]string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.actionRemovalHooks = append(m.actionRemovalHooks, hook)
}

// UnregisterPlugin removes a plugin's registration and its owned actions.
// Callers own the component lifecycle after removing a live registration.
func (m *SubsystemManager) UnregisterPlugin(name string) {
	m.mu.Lock()
	plugin, ok := m.plugins[name]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.plugins, name)
	removed := make(map[string]bool, len(plugin.actionNames))
	for _, name := range plugin.actionNames {
		delete(m.actions, name)
		removed[name] = true
	}
	order := m.actionOrder[:0]
	for _, name := range m.actionOrder {
		if !removed[name] {
			order = append(order, name)
		}
	}
	m.actionOrder = order
	m.orderedPlugins = nil
	m.orderDirty = true
	m.orderGeneration++
	m.initialized = len(m.plugins) > 0
	hooks := append([]func([]string){}, m.actionRemovalHooks...)
	names := append([]string(nil), plugin.actionNames...)
	m.mu.Unlock()
	// Unregister is admission removal, not disposal; the caller owns Stop and
	// Shutdown. Closing its queue prevents same-name registration reusing it.
	globalEventDispatcher.detachPluginQueue(plugin)
	for _, hook := range hooks {
		hook(names)
	}
}
