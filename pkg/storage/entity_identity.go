package storage

import "strings"

// QualifiedEntityID maps an ID exposed by a database-scoped engine to the ID
// used by the underlying store's access metadata and knowledge policies. The
// shared discovery cursor finds its first nonempty physical namespace, including
// through an empty outer scope. This identity lookup does not select graph reads.
// An already qualified ID is not prefixed again, and a colon inside a local ID
// does not change its database.
//
// For example, for tenant := NewNamespacedEngine(base, "tenant"):
//
//	QualifiedEntityID(tenant, "a:b")        // "tenant:a:b"
//	QualifiedEntityID(tenant, "tenant:a:b") // "tenant:a:b"
func QualifiedEntityID(engine Engine, id string) string {
	scoped, ok := engineCapability[interface{ Namespace() string }](engine, capabilityIdentityOwner, func(namespace interface{ Namespace() string }) bool { return namespace.Namespace() != "" })
	if ok {
		return qualifyEntityID(scoped.Namespace()+":", id)
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
