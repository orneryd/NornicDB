package server

// DatabaseOptions returns a database's options (cypher.DatabaseOptionsManager, #937).
func (a *databaseManagerAdapter) DatabaseOptions(name string) (map[string]string, error) {
	return a.manager.DatabaseOptions(name)
}

// SetDatabaseOption sets a database option, or removes it when value is empty.
func (a *databaseManagerAdapter) SetDatabaseOption(name, key, value string) error {
	return a.manager.SetDatabaseOption(name, key, value)
}
