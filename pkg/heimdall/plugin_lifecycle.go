package heimdall

// OnActionsRemoved registers a consumer which removes exported tools when their
// owning actions are unregistered. Hooks run outside the manager lock and are
// retired by ShutdownAll, before plugins can register their replacements.
func (m *SubsystemManager) OnActionsRemoved(hook func([]string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.actionRemovalHooks = append(m.actionRemovalHooks, hook)
}

// UnregisterPlugin removes a plugin's registration and its owned actions.
// Callers own Stop/Shutdown; failed-start callers use this before cleanup.
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
	for _, hook := range hooks {
		hook(names)
	}
}
