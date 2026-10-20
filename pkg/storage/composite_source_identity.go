package storage

// Source descriptors are bounded by the configured aliases. Rebuild only when
// the catalog name changes; scans never retain an entity-sized identity set.
func (c *CompositeEngine) rebuildSourceIdentitiesLocked() {
	c.sources = make(map[string]entitySource, len(c.constituents))
	physicalByDatabase := make(map[string]map[string]bool)
	canonicalAlias := make(map[string]string)
	for alias, engine := range c.constituents {
		database := DatabaseOf(engine)
		if database == "" {
			database = c.constituentNames[alias]
		}
		if database == "" {
			database = alias
		}
		source := remoteSourceKey(engine)
		physical := source
		if physical == "" {
			physical = PhysicalLocationKey("", database, false)
		}
		if physicalByDatabase[database] == nil {
			physicalByDatabase[database] = map[string]bool{}
		}
		physicalByDatabase[database][physical] = true
		if previous, exists := canonicalAlias[physical]; !exists || alias < previous {
			canonicalAlias[physical] = alias
		}
		c.sources[alias] = entitySource{source, database}
	}
	for alias, source := range c.sources {
		if source.key != "" && len(physicalByDatabase[source.qualifier]) > 1 {
			source.qualifier = canonicalAlias[source.key]
			if c.name != "" {
				source.qualifier = c.name + "." + source.qualifier
			}
			c.sources[alias] = source
		}
	}
}

// CanonicalEntityIdentity separates physical selection from public names. Old
// remote database qualifiers remain valid only when they select one location.
// Ambiguous names never pick the first map entry or open a participant.
func (c *CompositeEngine) CanonicalEntityIdentity(identity EntityIdentity) (EntityIdentity, bool) {
	if identity.Source == "" && identity.Database == "" {
		return identity, true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var selected entitySource
	found := false
	for alias, source := range c.sources {
		matches := identity.Source != "" && identity.Source == source.key
		if identity.Source == "" {
			qualifiedAlias := alias
			if c.name != "" {
				qualifiedAlias = c.name + "." + alias
			}
			matches = identity.Database == source.qualifier || identity.Database == qualifiedAlias || identity.Database == c.constituentNames[alias]
		}
		if !matches {
			continue
		}
		if found && (selected.key != source.key || selected.qualifier != source.qualifier) {
			return EntityIdentity{}, false
		}
		selected, found = source, true
	}
	if !found {
		return EntityIdentity{}, false
	}
	identity.Source, identity.Database = selected.key, selected.qualifier
	return identity, true
}

func (c *CompositeEngine) sourceIdentity(alias string, identity EntityIdentity) EntityIdentity {
	return c.sourceDescriptor(alias).identity(identity)
}

func (c *CompositeEngine) sourceDescriptor(alias string) entitySource {
	c.mu.RLock()
	source := c.sources[alias]
	c.mu.RUnlock()
	return source
}

func (source entitySource) identity(identity EntityIdentity) EntityIdentity {
	if source.key != "" || identity.Database == "" {
		identity.Database, identity.Source = source.qualifier, source.key
	}
	return identity
}

func (source entitySource) node(node *Node) *Node {
	if node == nil {
		return nil
	}
	identity := source.identity(node.Identity())
	if identity == node.Identity() {
		return node
	}
	return node.WithIdentity(identity)
}

func (source entitySource) edge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	identity := source.identity(edge.Identity())
	if identity == edge.Identity() {
		return edge
	}
	return edge.WithIdentity(identity)
}

func (c *CompositeEngine) referenceEdge(edge *Edge, database string) *Edge {
	if edge == nil {
		return nil
	}
	identity, ok := c.CanonicalEntityIdentity(EntityIdentity{Database: database, LocalID: string(edge.ID)})
	if !ok {
		return nil
	}
	return edge.WithIdentity(identity)
}

func (c *CompositeEngine) referenceEdges(edges []*Edge, database string) []*Edge {
	result := make([]*Edge, len(edges))
	for i, edge := range edges {
		result[i] = c.referenceEdge(edge, database)
	}
	return result
}

func (c *CompositeEngine) sourceNodes(alias string, engine Engine, nodes []*Node) []*Node {
	source := c.sourceDescriptor(alias)
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		result[i] = source.node(node)
	}
	return result
}

// Both relationship creation and point reads select endpoints through this
// owner. Alias text may differ while the physical location is the same.
func (c *CompositeEngine) qualifiedNodePair(start, end NodeID) (EntityIdentity, EntityIdentity, bool, bool) {
	leftKind, leftDatabase, leftLocal, leftScoped := ParseElementID(string(start))
	rightKind, rightDatabase, rightLocal, rightScoped := ParseElementID(string(end))
	if !leftScoped && !rightScoped {
		return EntityIdentity{LocalID: string(start)}, EntityIdentity{LocalID: string(end)}, false, true
	}
	if leftScoped && leftKind != "4" || rightScoped && rightKind != "4" {
		return EntityIdentity{}, EntityIdentity{}, true, false
	}
	left, right := EntityIdentity{LocalID: string(start)}, EntityIdentity{LocalID: string(end)}
	if leftScoped {
		var ok bool
		left, ok = c.CanonicalEntityIdentity(EntityIdentity{Database: leftDatabase, LocalID: leftLocal})
		if !ok {
			return EntityIdentity{}, EntityIdentity{}, true, false
		}
	}
	if rightScoped {
		var ok bool
		right, ok = c.CanonicalEntityIdentity(EntityIdentity{Database: rightDatabase, LocalID: rightLocal})
		if !ok {
			return EntityIdentity{}, EntityIdentity{}, true, false
		}
	}
	if !leftScoped {
		left.Database, left.Source = right.Database, right.Source
	}
	if !rightScoped {
		right.Database, right.Source = left.Database, left.Source
	}
	return left, right, true, left.physicalSource() == right.physicalSource()
}
