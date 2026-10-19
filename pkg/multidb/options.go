package multidb

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// OptionTxLogEnrichment is Neo4j's change data capture option: OFF (the
// default), DIFF or FULL (#937).
const OptionTxLogEnrichment = "txLogEnrichment"

// DatabaseOptions returns a copy of a database's options.
func (m *DatabaseManager) DatabaseOptions(databaseName string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, exists := m.databases[databaseName]
	if !exists {
		return nil, ErrDatabaseNotFound
	}
	options := make(map[string]string, len(info.Options))
	for key, value := range info.Options {
		options[key] = value
	}
	return options, nil
}

// SetDatabaseOption sets a database option, or removes it when value is
// empty, and persists the database metadata. The caller validates the
// option.
func (m *DatabaseManager) SetDatabaseOption(databaseName, key, value string) error {
	finish, err := m.admitDatabaseLifecycle(context.Background(), databaseName)
	if err != nil {
		return err
	}
	defer finish()
	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.databases[databaseName]
	if !exists {
		return ErrDatabaseNotFound
	}
	previous := info.Options
	options := make(map[string]string, len(previous)+1)
	for existing, existingValue := range previous {
		options[existing] = existingValue
	}
	if value == "" {
		delete(options, key)
	} else {
		options[key] = value
	}
	if len(options) == 0 {
		options = nil
	}
	info.Options = options
	info.UpdatedAt = time.Now()
	if err := m.persistMetadata(); err != nil {
		return err
	}
	m.applyChangeCapture(databaseName, options)
	return nil
}

// applyChangeCapture tells the storage engine a database's change capture
// mode, its txLogEnrichment option (#937): at load, when the option is set,
// and off when the database is dropped.
func (m *DatabaseManager) applyChangeCapture(databaseName string, options map[string]string) {
	if capture, ok := storage.ChangeCaptureOf(m.inner); ok {
		capture.SetChangeCapture(databaseName, storage.ParseChangeCaptureMode(options[OptionTxLogEnrichment]))
	}
}
