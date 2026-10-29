package storage

import "maps"

// WithConstituentAccess creates a statement-owned composite view. Access chooses
// the physical read/write participant while the composite retains routing and
// provenance ownership. Only catalog/routing configuration is copied, never
// graph entities or a graph-sized identity set.
func (c *CompositeEngine) WithConstituentAccess(access func(string, Engine, bool) (Engine, error)) *CompositeEngine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	properties := make(map[string]map[interface{}]string, len(c.propertyRouting))
	for key, routes := range c.propertyRouting {
		properties[key] = maps.Clone(routes)
	}
	return &CompositeEngine{name: c.name, membershipProvider: c.membershipProvider, constituents: maps.Clone(c.constituents), constituentNames: maps.Clone(c.constituentNames), accessModes: maps.Clone(c.accessModes), labelRouting: maps.Clone(c.labelRouting), propertyRouting: properties, propertyDefaults: maps.Clone(c.propertyDefaults), nodeToConstituent: make(map[NodeID]string), accessConstituent: access}
}

// WithMembership issues a distinct control-bound view without opening a data
// participant. The owning coordinator supplies the opaque live descriptor.
func (c *CompositeEngine) WithMembership(provider MembershipProvider) *CompositeEngine {
	view := c.WithConstituentAccess(c.accessConstituent)
	view.membershipProvider = provider
	return view
}
func (c *CompositeEngine) StorageMembership() MembershipBinding {
	if c.membershipProvider == nil {
		return MembershipBinding{Status: MembershipUnowned, Boundary: c}
	}
	binding := c.membershipProvider.StorageMembership()
	if binding.Status == MembershipUnowned {
		binding.Status = MembershipUnbound
	}
	binding.Boundary = c
	return binding
}
