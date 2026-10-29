package storage

import "maps"

// WithConstituentAccess creates a statement-owned composite view. Access chooses
// the physical read/write participant by constituent alias while the composite retains routing and
// provenance ownership. Only catalog/routing configuration is copied, never
// graph entities or a graph-sized identity set.
func (c *CompositeEngine) WithConstituentAccess(owner any, access func(string, Engine, bool) (Engine, error)) *CompositeEngine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	properties := make(map[string]map[interface{}]string, len(c.propertyRouting))
	for key, routes := range c.propertyRouting {
		properties[key] = maps.Clone(routes)
	}
	return &CompositeEngine{owner: owner, name: c.name, constituents: maps.Clone(c.constituents), constituentNames: maps.Clone(c.constituentNames), accessModes: maps.Clone(c.accessModes), labelRouting: maps.Clone(c.labelRouting), propertyRouting: properties, propertyDefaults: maps.Clone(c.propertyDefaults), nodeToConstituent: make(map[NodeID]string), accessConstituent: access}
}

// StatementOwner identifies the statement to which this routing view belongs.
func (c *CompositeEngine) StatementOwner() any { return c.owner }
