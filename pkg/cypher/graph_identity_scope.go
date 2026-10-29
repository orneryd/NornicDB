package cypher

import (
	"strings"
	"sync"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphIdentityScope is a fragment's existing execution selection, shared by
// identity readers and graph functions. Catalog access does not open storage;
// actual graph functions resolve the selected engine once, when needed.
// Its source set is bounded by constituent count, never by graph size.
type graphIdentityScope struct {
	sources   map[string]bool
	database  string
	manager   DatabaseManagerInterface
	authToken string
	once      sync.Once
	engine    storage.Engine
	err       error
}

func newGraphIdentityScope(catalog *fabric.Catalog, database string, manager DatabaseManagerInterface, token string) *graphIdentityScope {
	scope := &graphIdentityScope{database: database, manager: manager, authToken: token}
	if catalog != nil {
		for _, name := range catalog.ListGraphs() {
			if !strings.Contains(name, ".") {
				continue
			}
			if location, err := catalog.Resolve(name); err == nil {
				if scope.sources == nil {
					scope.sources = make(map[string]bool)
				}
				scope.sources[location.DatabaseName()] = true
			}
		}
	}
	return scope
}

type graphIdentityDomain struct {
	engine storage.Engine
	scope  *graphIdentityScope
}

func (e *StorageExecutor) graphIdentityDomain() graphIdentityDomain {
	return graphIdentityDomain{engine: e.storage, scope: e.graphIdentityScope}
}

// A qualifier was historically a disposable prefix on plain stores. Only a
// real source selection in a composite scope may make it foreign. Native
// EntityIdentity values remain exact; this compatibility applies to text IDs.
func (domain graphIdentityDomain) foreignQualifier(database string) bool {
	if storage.CompositeDatabaseOf(domain.engine) != "" {
		return database != storageDatabaseName(domain.engine)
	}
	return domain.scope != nil && domain.scope.sources[database] && database != storageDatabaseName(domain.engine)
}

func (e *StorageExecutor) FunctionStorage(database string) (storage.Engine, error) {
	issued := e.storage
	if e.storageMembership().Status == storage.MembershipUnowned && e.txContext != nil && e.txContext.active {
		switch tx := e.txContext.tx.(type) {
		case *storage.BadgerTransaction:
			issued = e.transactionStorageFor(tx)
		case *fabric.FabricTransaction:
			if composite, ok := e.storage.(*storage.CompositeEngine); ok {
				issued = composite.WithMembership(e.txContext)
			}
		}
	}
	return e.graphFunctionStorage(database, issued)
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
func (domain graphIdentityDomain) preserveQualifiers(left, right interface{}) bool {
	leftText, leftOK := left.(string)
	rightText, rightOK := right.(string)
	if !leftOK || !rightOK {
		return true
	}
	leftKind, leftDatabase, _, leftQualified := storage.ParseElementID(leftText)
	rightKind, rightDatabase, _, rightQualified := storage.ParseElementID(rightText)
	if !leftQualified || !rightQualified || leftKind != rightKind {
		return true
	}
	return domain.foreignQualifier(leftDatabase) || domain.foreignQualifier(rightDatabase)
}
func graphFunctionIdentityDomain(ctx cypherfn.Context, engine storage.Engine) graphIdentityDomain {
	domain := graphIdentityDomain{engine: engine}
	if executor, ok := ctx.Graphs.(*StorageExecutor); ok {
		domain.scope = executor.graphIdentityScope
	}
	return domain
}

// EntityElementIDDatabase preserves source identity on composite values while
// retaining an ordinary statement's explicit execution-database presentation.
func (e *StorageExecutor) EntityElementIDDatabase(native, selected string) string {
	if selected != "" && e.graphIdentityScope == nil && storage.CompositeDatabaseOf(e.storage) == "" {
		return selected
	}
	return native
}
