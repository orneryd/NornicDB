package cypher

import (
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// DatabaseStorageResolver resolves the storage view selected by a protocol.
type DatabaseStorageResolver interface {
	GetStorage(name string) (storage.Engine, error)
}

type authenticatedDatabaseStorageResolver interface {
	GetStorageWithAuth(name, authToken string) (storage.Engine, error)
}

// DatabaseScopedExecutorOptions identifies the common executor lifetime and
// database selection shared by HTTP and Bolt. Protocol-specific configuration
// remains with the caller and runs before the executor is published.
type DatabaseScopedExecutorOptions struct {
	Name                    string
	Storage                 DatabaseStorageResolver
	AuthToken               string
	UseAuthenticatedStorage bool
	QueryCachePolicy        bool
	QueryCacheMaxEntries    int
	QueryCacheTTL           time.Duration
	StatementCacheManager   *multidb.DatabaseManager
	FallbackStatementCaches *StatementCaches
	LocalizationRenderer    ProcedureMetadataRenderer
	BaseExecutor            *StorageExecutor
	Configure               func(*StorageExecutor, storage.Engine)
}

// NewDatabaseScopedExecutor builds the one database-scoped execution view used
// by protocol adapters. It binds canonical statement caches before publication.
func NewDatabaseScopedExecutor(options DatabaseScopedExecutorOptions) (*StorageExecutor, error) {
	var (
		engine storage.Engine
		err    error
	)
	if options.UseAuthenticatedStorage {
		if resolver, ok := options.Storage.(authenticatedDatabaseStorageResolver); ok {
			engine, err = resolver.GetStorageWithAuth(options.Name, options.AuthToken)
		} else {
			return nil, fmt.Errorf("database storage resolver does not support authenticated access for %q", options.Name)
		}
	} else {
		engine, err = options.Storage.GetStorage(options.Name)
	}
	if err != nil {
		return nil, err
	}

	var executor *StorageExecutor
	if options.QueryCachePolicy {
		executor = NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: options.QueryCacheMaxEntries, TTL: options.QueryCacheTTL})
	} else {
		executor = NewStorageExecutor(engine)
	}
	if options.StatementCacheManager != nil {
		if err := executor.UseDatabaseStatementCaches(options.StatementCacheManager, options.Name); err != nil {
			return nil, err
		}
	} else if options.FallbackStatementCaches != nil {
		executor.UseStatementCaches(options.FallbackStatementCaches)
	}
	if options.LocalizationRenderer != nil {
		executor.SetLocalizationRenderer(options.LocalizationRenderer)
	}
	if options.BaseExecutor != nil {
		executor.ShareEmbedderFrom(options.BaseExecutor)
		executor.ShareInferenceManagerFrom(options.BaseExecutor)
	}
	if options.Configure != nil {
		options.Configure(executor, engine)
	}
	return executor, nil
}
