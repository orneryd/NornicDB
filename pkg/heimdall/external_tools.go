package heimdall

// ExternalActions returns only explicitly public actions in successful registration
// order. Action maps have no order, so names within each plugin are sorted.
// Internal inference continues to see all actions through ActionsAsMCPTools.
func (m *SubsystemManager) ExternalActions() []ActionFunc {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var actions []ActionFunc
	for _, name := range m.actionOrder {
		if action := m.actions[name]; action.External {
			actions = append(actions, action)
		}
	}
	return actions
}
