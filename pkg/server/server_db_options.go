package server

import "context"

// DatabaseOptions returns a database's options (cypher.DatabaseOptionsManager, #937).
func (a *databaseManagerAdapter) DatabaseOptions(name string) (map[string]string, error) {
	return a.Manager.DatabaseOptions(name)
}

// SetDatabaseOption sets a database option, or removes it when value is empty.
func (a *databaseManagerAdapter) SetDatabaseOption(ctx context.Context, name, key, value string) error {
	return a.Manager.SetDatabaseOption(ctx, name, key, value)
}
