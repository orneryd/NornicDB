package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// SetDefaultEmbedConfig binds the default provider configuration for per-database
// embedder selection and failure diagnostics. It may be set before or after the
// provider is published.
func (db *DB) SetDefaultEmbedConfig(defaultConfig *embed.Config) {
	if defaultConfig == nil {
		return
	}
	db.mu.Lock()
	eq := db.embedQueue
	closed := db.closed
	db.mu.Unlock()
	if closed {
		return
	}
	key := embedConfigKey(defaultConfig)
	configCopy := *defaultConfig
	db.embedderRegistryMu.Lock()
	db.defaultEmbedConfig = &configCopy
	if eq != nil {
		if defaultEmbedder := eq.currentEmbedder(); defaultEmbedder != nil {
			if db.embedderRegistry == nil {
				db.embedderRegistry = make(map[string]embed.Embedder)
			}
			db.embedderRegistry[key] = defaultEmbedder
			db.defaultEmbedKey = key
		}
	}
	db.embedderRegistryMu.Unlock()
	db.refreshEmbedFailureRedactor()
}

func (db *DB) registerDefaultEmbedder(embedder embed.Embedder) {
	if embedder == nil {
		return
	}
	db.embedderRegistryMu.Lock()
	if db.defaultEmbedConfig != nil {
		if db.embedderRegistry == nil {
			db.embedderRegistry = make(map[string]embed.Embedder)
		}
		key := embedConfigKey(db.defaultEmbedConfig)
		db.embedderRegistry[key] = embedder
		db.defaultEmbedKey = key
	}
	db.embedderRegistryMu.Unlock()
}

// SetEmbedConfigForDB sets the optional resolver that returns resolved embed config for a database.
// When set, EmbedQueryForDB uses the embedder registry (get or create by config key) instead of the global embedder.
func (db *DB) SetEmbedConfigForDB(fn func(dbName string) (*embed.Config, error)) {
	db.mu.Lock()
	db.embedConfigForDB = fn
	queue := db.embedQueue
	db.mu.Unlock()
	if queue == nil {
		return
	}
	if fn == nil {
		queue.SetEmbedderResolver(nil)
		db.refreshEmbedFailureRedactor()
		return
	}
	queue.SetEmbedderResolver(func(nodeID storage.NodeID) (embed.Embedder, error) {
		dbName, _, ok := splitQualifiedID(string(nodeID))
		if !ok {
			return nil, nil
		}
		return db.getOrCreateEmbedderForDB(dbName)
	})
	db.refreshEmbedFailureRedactor()
}

// refreshEmbedFailureRedactor binds the durable error owner to the same
// default or per-database config that selects the provider. This runs when
// configuration changes and on the worker's failure/read paths, not per query.
func (db *DB) refreshEmbedFailureRedactor() {
	db.mu.RLock()
	queue := db.embedQueue
	configForDB := db.embedConfigForDB
	db.mu.RUnlock()
	if queue == nil {
		return
	}
	db.embedderRegistryMu.RLock()
	var defaultConfig *embed.Config
	if db.defaultEmbedConfig != nil {
		copy := *db.defaultEmbedConfig
		defaultConfig = &copy
	}
	db.embedderRegistryMu.RUnlock()
	queue.SetFailureErrorRedactor(func(nodeID storage.NodeID, failure error) error {
		config := defaultConfig
		if configForDB != nil {
			if dbName, _, qualified := splitQualifiedID(string(nodeID)); qualified {
				if resolved, err := configForDB(dbName); err == nil && resolved != nil {
					config = resolved
				}
			}
		}
		if config == nil {
			return observability.RedactError(failure)
		}
		return observability.RedactClientError(failure, config.APIURL, config.APIKey)
	})
}
