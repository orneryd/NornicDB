package storage

import (
	"maps"
	"slices"
)

// CompositeConstituentResolver binds a catalog source to an existing
// transaction view. write is true only at the mutation boundary, before an
// effect; routing and source discovery use false. It never owns completion.
type CompositeConstituentResolver func(alias string, source Engine, write bool) (Engine, error)

// WithConstituentScope snapshots routing metadata and borrows source engines.
// The resolver belongs to one statement, while its transaction owner may reuse
// physical participants across statements. No entity cache is retained.
func (c *CompositeEngine) WithConstituentScope(resolve CompositeConstituentResolver) *CompositeEngine {
	if resolve == nil { return c }
	c.mu.RLock()
	defer c.mu.RUnlock()
	view := &CompositeEngine{
		name:c.name,
		constituents:maps.Clone(c.constituents),
		constituentNames:maps.Clone(c.constituentNames),
		accessModes:maps.Clone(c.accessModes),
		labelRouting:make(map[string][]string,len(c.labelRouting)),
		propertyRouting:make(map[string]map[interface{}]string,len(c.propertyRouting)),
		propertyDefaults:maps.Clone(c.propertyDefaults),
		constituentScope:resolve,
	}
	for label,aliases:=range c.labelRouting { view.labelRouting[label]=slices.Clone(aliases) }
	for property,routes:=range c.propertyRouting {view.propertyRouting[property]=maps.Clone(routes)}
	return view
}
