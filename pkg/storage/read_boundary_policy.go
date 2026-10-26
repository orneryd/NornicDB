package storage

import "reflect"

// Namespace data and metadata are admitted only when their implementation
// prefixes requests and checks scope before translating, counting or visiting.
// Engine-wide lifecycle/debt scans and unknown future operations are refused.
func (n *NamespacedEngine) ReadCapabilityScoped(request reflect.Type) bool {
	return namespacedEngineReadPolicy.Scoped(request)
}

// Composite reads own constituent selection, provenance and historical versions.
// Capability methods absent from this list remain behind the boundary.
func (c *CompositeEngine) ReadCapabilityScoped(request reflect.Type) bool {
	return compositeEngineReadPolicy.Scoped(request)
}
