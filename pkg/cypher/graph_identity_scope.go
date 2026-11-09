package cypher

import (
	"context"
	"sync"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphIdentityScope is a fragment's existing execution selection, shared by
// identity readers and graph functions. Catalog access does not open storage;
// actual graph functions resolve the selected engine once, when needed.
type graphIdentityScope struct {
	database  string
	manager   DatabaseManagerInterface
	authToken string
	once      sync.Once
	engine    storage.Engine
	err       error
}

func (e *StorageExecutor) FunctionStorage(database string) (storage.Engine, error) {
	return e.graphFunctionStorage(database, e.storage)
}

func (e *StorageExecutor) graphFunctionStorage(database string, fallback storage.Engine) (storage.Engine, error) {
	if database == "" && e.graphIdentityScope != nil {
		database = e.graphIdentityScope.database
	}
	if database == "" || database == storageDatabaseName(fallback) || e.graphIdentityScope == nil || e.graphIdentityScope.manager == nil {
		return fallback, nil
	}
	scope := e.graphIdentityScope
	scope.once.Do(func() {
		selected, err := scope.manager.GetStorageForUse(scope.database, scope.authToken)
		if err != nil {
			scope.err = err
			return
		}
		var ok bool
		scope.engine, ok = selected.(storage.Engine)
		if !ok {
			scope.err = localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(scope.database), nil)
		}
	})
	if scope.err == nil {
		if statement := membershipOfStorage(e.storage).statement; statement != nil {
			selected, _, err := statement.on(context.Background(), scope.database, e, scope.engine)
			if err != nil {
				return nil, err
			}
			return selected.storage, nil
		}
	}
	if scope.err == nil {
		if member := e.membership(context.Background()); member.storage != nil {
			if err := TransactionDatabaseError(context.Background(), e.dbManager, storageDatabaseName(fallback), scope.database, "RETURN 1"); err != nil {
				return nil, err
			}
			return member.storage, nil
		}
	}
	return scope.engine, scope.err
}

func graphFunctionDatabase(ctx cypherfn.Context) string {
	if ctx.Database != "" {
		return ctx.Database
	}
	if executor, ok := ctx.Graphs.(*StorageExecutor); ok && executor.graphIdentityScope != nil {
		return executor.graphIdentityScope.database
	}
	return storageDatabaseName(ctx.Storage)
}

// EntityElementIDDatabase preserves source identity on composite values while
// retaining an ordinary statement's explicit execution-database presentation.
func (e *StorageExecutor) EntityElementIDDatabase(native, selected string) string {
	if selected != "" && e.graphIdentityScope == nil && storage.CompositeDatabaseOf(e.storage) == "" {
		return selected
	}
	return native
}
