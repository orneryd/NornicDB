package resultstream

import "context"

type databaseEpoch struct{ generation byte }

// BindScope captures the database lifetime before a producer prepares its first
// page. DiscardDatabase invalidates this binding and serializes publication.
func (r *Registry) BindScope(scope Scope) Scope {
	if scope.databaseEpoch != nil || scope.Database == "" || len(scope.Database) > maxTokenDatabaseBytes {
		return scope
	}
	r.databaseMu.Lock()
	if r.databases == nil {
		r.databases = make(map[string]*databaseEpoch)
	}
	epoch := r.databases[scope.Database]
	if epoch == nil {
		epoch = &databaseEpoch{}
		r.databases[scope.Database] = epoch
	}
	scope.databaseEpoch = epoch
	r.databaseMu.Unlock()
	return scope
}
func (r *Registry) validDatabaseScope(scope Scope) bool {
	r.databaseMu.RLock()
	valid := scope.databaseEpoch != nil && r.databases[scope.Database] == scope.databaseEpoch
	r.databaseMu.RUnlock()
	return valid
}

// DatabaseRegistry binds a database-scoped service to its current lifetime.
// It delegates all accounting and stream termination to the one Registry.
type DatabaseRegistry struct {
	*Registry
	scope Scope
}

// ForDatabase captures the service's database lifetime, including before its
// first search. A reset/drop therefore also retires unpublished old-service work.
func (r *Registry) ForDatabase(database string) *DatabaseRegistry {
	return &DatabaseRegistry{Registry: r, scope: r.BindScope(Scope{Database: database})}
}
func (r *DatabaseRegistry) BindScope(scope Scope) Scope {
	if scope.Database == r.scope.Database {
		scope.databaseEpoch = r.scope.databaseEpoch
		return scope
	}
	return r.Registry.BindScope(scope)
}
func (r *DatabaseRegistry) Start(ctx context.Context, scope Scope, stream Stream, n int) (*Page, error) {
	return r.Registry.Start(ctx, r.BindScope(scope), stream, n)
}
