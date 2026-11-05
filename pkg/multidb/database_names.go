package multidb

// ListDatabaseNames returns a snapshot of canonical catalog names, including
// empty, composite and system databases. Aliases remain selections of their
// targets, rather than additional databases. Name-only consumers avoid copying
// configuration, limits, aliases and constituent metadata they do not use.
func (m *DatabaseManager) ListDatabaseNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.databases))
	for name := range m.databases {
		names = append(names, name)
	}
	return names
}
