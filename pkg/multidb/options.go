package multidb

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/localization"
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
func (m *DatabaseManager) SetDatabaseOption(ctx context.Context, databaseName, key, value string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {
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
		return nil

	})
}

// applyChangeCapture tells the storage engine a database's change capture
// mode, its txLogEnrichment option (#937): at load, when the option is set,
// and off when the database is dropped.
func (m *DatabaseManager) applyChangeCapture(databaseName string, options map[string]string) {
	if capture, ok := storage.ChangeCaptureOf(m.inner); ok {
		capture.SetChangeCapture(databaseName, storage.ParseChangeCaptureMode(options[OptionTxLogEnrichment]))
	}
}

// LoadChangeCaptureModes sets every persisted database's change capture mode
// (its txLogEnrichment option) on engine before journal recovery. A journaled
// commit that start-up applies publishes the change events its database
// captures, and it is applied before the database manager exists: the manager,
// created by the server after nornicdb.Open returns, loads this same catalog
// record and settles the same modes again. A store that holds no catalog
// metadata yet has no persisted option and sets none.
//
// It reads only the catalog record; it neither writes nor validates metadata,
// so a store whose manager will be created later is unchanged.
func LoadChangeCaptureModes(ctx context.Context, engine storage.Engine, systemDatabase string) error {
	if _, ok := storage.ChangeCaptureOf(engine); !ok {
		return nil
	}
	node, err := readCatalogMetadataNode(ctx, engine, systemDatabase)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	data, ok := node.Properties["data"].(string)
	if !ok {
		return nil
	}
	var databases map[string]*DatabaseInfo
	if err := jsontext.Unmarshal([]byte(data), &databases); err != nil {
		return localizedError(localization.MultidbMetadataParseFailed(err), err)
	}
	capture, ok := storage.ChangeCaptureOf(engine)
	if !ok {
		return nil
	}
	for name, info := range databases {
		if err := ctx.Err(); err != nil {
			return err
		}
		if info != nil {
			capture.SetChangeCapture(name, storage.ParseChangeCaptureMode(info.Options[OptionTxLogEnrichment]))
		}
	}
	return nil
}
