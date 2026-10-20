package storage

// constituentForElementID resolves a qualified identity before a local read.
// A qualified ID never falls back to another database with the same local ID.
// qualified is true even when its database is unavailable or the kind differs.
func (c *CompositeEngine) constituentForElementID(kind, value string) (engine Engine, local string, qualified bool) {
	aliases, local, qualified := c.constituentsForEntity(kind, value)
	if !qualified || len(aliases) == 0 {
		return nil, local, qualified
	}
	engine, _ = c.getConstituent(aliases[0])
	return engine, local, true
}

// constituentsForEntity is the shared source selector for ordinary and MVCC
// identity reads. A qualified reference cannot fall through to another source.
func (c *CompositeEngine) constituentsForEntity(kind, value string) ([]string, string, bool) {
	actual, database, local, qualified := ParseElementID(value)
	if !qualified {
		return c.constituentsForIdentity(EntityIdentity{LocalID: value}, c.getConstituentsForRead()), value, false
	}
	if actual != kind {
		return nil, local, true
	}
	return c.constituentsForIdentity(EntityIdentity{Database: database, LocalID: local}, c.getConstituentsForRead()), local, true
}

func (c *CompositeEngine) constituentDatabase(alias string, engine Engine) string {
	c.mu.RLock()
	database := c.sources[alias].qualifier
	c.mu.RUnlock()
	if database == "" {
		database = alias
	}
	return database
}

func nodeFromDatabase(node *Node, database string) *Node {
	if node == nil || node.Database != "" {
		return node
	}
	copy := *node
	copy.Database = database
	return &copy
}

func edgeFromDatabase(edge *Edge, database string) *Edge {
	if edge == nil || edge.Database != "" {
		return edge
	}
	copy := *edge
	copy.Database = database
	return &copy
}

func (c *CompositeEngine) sourceNode(alias string, engine Engine, node *Node) *Node {
	return c.sourceDescriptor(alias).node(node)
}
func (c *CompositeEngine) sourceEdge(alias string, engine Engine, edge *Edge) *Edge {
	return c.sourceDescriptor(alias).edge(edge)
}

func edgesFromDatabase(edges []*Edge, database string) []*Edge {
	for _, edge := range edges {
		if edge != nil && edge.Database == "" {
			copy := append([]*Edge(nil), edges...)
			for i, value := range copy {
				copy[i] = edgeFromDatabase(value, database)
			}
			return copy
		}
	}
	return edges
}

func nodesFromDatabase(nodes []*Node, database string) []*Node {
	for _, node := range nodes {
		if node != nil && node.Database == "" {
			copy := append([]*Node(nil), nodes...)
			for i, value := range copy {
				copy[i] = nodeFromDatabase(value, database)
			}
			return copy
		}
	}
	return nodes
}

func nodePairDatabase(start, end NodeID) string {
	_, database, _, qualified := ParseElementID(string(start))
	if !qualified {
		_, database, _, _ = ParseElementID(string(end))
	}
	return database
}

// constituentForNodePair scopes both endpoints to the same selected source.
// Unqualified endpoint IDs stay local within that source; two different
// qualified sources can never name a stored relationship.
func (c *CompositeEngine) constituentForNodePair(start, end NodeID) (Engine, NodeID, NodeID, bool) {
	left, right, qualified, valid := c.qualifiedNodePair(start, end)
	if !qualified {
		return nil, start, end, false
	}
	if !valid {
		return nil, "", "", true
	}
	aliases := c.constituentsForIdentity(left, c.getConstituentsForRead())
	if len(aliases) == 0 {
		return nil, "", "", true
	}
	engine, _ := c.getConstituent(aliases[0])
	return engine, NodeID(left.LocalID), NodeID(right.LocalID), true
}

func (c *CompositeEngine) constituentNodeElementID(alias string, engine Engine, id NodeID) NodeID {
	return NodeID(SourceNodeElementID(c.constituentDatabase(alias, engine), id))
}
