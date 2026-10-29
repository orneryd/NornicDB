package storage

import "context"

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
	aliases := c.getConstituentsForRead()
	if !qualified {
		return aliases, value, false
	}
	if actual != kind {
		return nil, local, true
	}
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if c.constituentDatabase(alias, engine) == database {
			return []string{alias}, local, true
		}
	}
	return nil, local, true
}

func (c *CompositeEngine) constituentDatabase(alias string, engine Engine) string {
	if database := DatabaseOf(engine); database != "" {
		return database
	}
	c.mu.RLock()
	database := c.constituentNames[alias]
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
	if node == nil || node.Database != "" {
		return node
	}
	return nodeFromDatabase(node, c.constituentDatabase(alias, engine))
}
func (c *CompositeEngine) sourceEdge(alias string, engine Engine, edge *Edge) *Edge {
	if edge == nil || edge.Database != "" {
		return edge
	}
	return edgeFromDatabase(edge, c.constituentDatabase(alias, engine))
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

func nodesFromDatabaseWithContext(ctx context.Context, nodes []*Node, database string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if node != nil && node.Database == "" {
			projected := make([]*Node, 0, len(nodes))
			err := materializedValuesWithContext(ctx, nodes, func(value *Node) { projected = append(projected, nodeFromDatabase(value, database)) })
			if err != nil {
				return nil, err
			}
			return projected, ctx.Err()
		}
	}
	return nodes, ctx.Err()
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
	startKind, startDB, startLocal, startQualified := ParseElementID(string(start))
	endKind, endDB, endLocal, endQualified := ParseElementID(string(end))
	if !startQualified && !endQualified {
		return nil, start, end, false
	}
	if startQualified && startKind != "4" || endQualified && endKind != "4" || startQualified && endQualified && startDB != endDB {
		return nil, "", "", true
	}
	selected := string(start)
	if !startQualified {
		selected = string(end)
		startLocal = string(start)
	}
	if !endQualified {
		endLocal = string(end)
	}
	engine, _, _ := c.constituentForElementID("4", selected)
	return engine, NodeID(startLocal), NodeID(endLocal), true
}

func (c *CompositeEngine) constituentNodeElementID(alias string, engine Engine, id NodeID) NodeID {
	return NodeID(SourceNodeElementID(c.constituentDatabase(alias, engine), id))
}

// ConstituentStorage resolves a native entity's database through the same
// selector as qualified reads, retaining the statement's participant view.
func (c *CompositeEngine) ConstituentStorage(database string) (Engine, error) {
	aliases, _, _ := c.constituentsForEntity("4", SourceNodeElementID(database, ""))
	if len(aliases) == 0 {
		return nil, ErrNotFound
	}
	return c.getConstituent(aliases[0])
}

// edgeSourceAlias is the source decision shared by single and bulk creation.
func (c *CompositeEngine) edgeSourceAlias(edge *Edge) (string, bool, error) {
	if edge == nil {
		return "", true, ErrNotFound
	}
	if edge.Database == "" {
		return "", false, nil
	}
	aliases, _, _ := c.constituentsForEntity("4", SourceNodeElementID(edge.Database, edge.StartNode))
	if len(aliases) == 0 {
		return "", true, ErrNotFound
	}
	return aliases[0], true, nil
}
