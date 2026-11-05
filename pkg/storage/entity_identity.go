package storage

import "strings"

// QualifiedEntityID maps an ID exposed by a database-scoped engine to the ID
// used by the underlying store's access metadata and knowledge policies. It
// follows EngineUnwrapper layers to find the database namespace; an unscoped
// engine leaves the supplied ID unchanged. An already qualified ID is not
// prefixed again, and a colon inside a local ID does not change its database.
//
// For example, for tenant := NewNamespacedEngine(base, "tenant"):
//
//	QualifiedEntityID(tenant, "a:b")        // "tenant:a:b"
//	QualifiedEntityID(tenant, "tenant:a:b") // "tenant:a:b"
func QualifiedEntityID(engine Engine, id string) string {
	for engine != nil {
		if scoped, ok := engine.(interface{ Namespace() string }); ok {
			if namespace := scoped.Namespace(); namespace != "" {
				return qualifyEntityID(namespace+":", id)
			}
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return id
		}
		inner := wrapper.GetInnerEngine()
		if inner == engine {
			return id
		}
		engine = inner
	}
	return id
}

// qualifyEntityID applies the physical scope supplied by its actual owner.
// Discovery may skip an empty namespace, whereas a NamespacedEngine's explicit
// relative-ID API still owns the literal colon scope in that case.
func qualifyEntityID(scope, id string) string {
	if strings.HasPrefix(id, scope) {
		return id
	}
	return scope + id
}
