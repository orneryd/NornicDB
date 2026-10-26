package storage

import (
	"context"
	"reflect"
	"sort"
)

// resolveNodeReads owns source resolution for both point and batch reads.
// It binds the catalog and live readable aliases once, asks only required
// children, and resolves each original ID at its first successful source.
// Callers adapt point/batch transport; they never choose source precedence.
func (c *CompositeEngine) resolveNodeReads(ctx context.Context, ids []NodeID, read func(string, Engine, []NodeID, func(NodeID, *Node) error) error, emit func(NodeID, *Node)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.StorageMembership().ReadError(); err != nil {
		return err
	}
	aliases, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	catalog := c.constituentReadCatalog()
	type route struct {
		original, local NodeID
		alias           string
	}
	var single [1]route
	routes := single[:0]
	if len(ids) > 1 {
		routes = make([]route, 0, len(ids))
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		selected, local, qualified, err := catalog.selectEntity("4", string(id), aliases)
		if err == ErrNotFound || err == ErrInvalidID {
			continue
		}
		if err != nil {
			return err
		}
		r := route{original: id, local: NodeID(local)}
		if qualified {
			r.alias = selected[0]
		}
		routes = append(routes, r)
	}
	var resolved map[NodeID]bool
	if len(routes) > 1 {
		resolved = make(map[NodeID]bool, len(routes))
	}
	resolvedSingle := false
	isResolved := func(id NodeID) bool {
		if len(routes) == 1 {
			return resolvedSingle
		}
		return resolved[id]
	}
	for _, alias := range aliases {
		if err := ctx.Err(); err != nil {
			return err
		}
		var point [1]NodeID
		requests := point[:0]
		var originals map[NodeID][]NodeID
		if len(routes) > 1 {
			originals = make(map[NodeID][]NodeID)
		}
		for _, r := range routes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if isResolved(r.original) || r.alias != "" && r.alias != alias {
				continue
			}
			if originals == nil {
				requests = append(requests, r.local)
				continue
			}
			if _, requested := originals[r.local]; !requested {
				requests = append(requests, r.local)
			}
			originals[r.local] = append(originals[r.local], r.original)
		}
		if len(requests) == 0 {
			continue
		}
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if err := read(alias, engine, requests, func(local NodeID, node *Node) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			var pointOriginal [1]NodeID
			matching := pointOriginal[:0]
			if originals == nil {
				if local == routes[0].local {
					matching = append(matching, routes[0].original)
				}
			} else {
				matching = originals[local]
			}
			for _, original := range matching {
				if isResolved(original) {
					continue
				}
				if resolved == nil {
					resolvedSingle = true
				} else {
					resolved[original] = true
				}
				emit(original, c.sourceNode(alias, engine, node))
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// constituentForElementID resolves a qualified identity before a local read.
// A qualified ID never falls back to another database with the same local ID.
// qualified is true even when its database is unavailable or the kind differs.
func (c *CompositeEngine) constituentForElementID(kind, value string) (engine Engine, local string, qualified bool, err error) {
	aliases, local, qualified := c.constituentsForEntity(kind, value)
	if !qualified || len(aliases) == 0 {
		return nil, local, qualified, nil
	}
	engine, err = c.getConstituent(aliases[0])
	return engine, local, true, err
}

// constituentsForEntity is the shared source selector for ordinary and MVCC
// identity reads. A qualified reference cannot fall through to another source.
func (c *CompositeEngine) constituentsForEntity(kind, value string) ([]string, string, bool) {
	aliases, local, qualified, _ := c.constituentReadCatalog().selectEntity(kind, value, c.getConstituentsForRead())
	return aliases, local, qualified
}

func (c *CompositeEngine) constituentDatabase(alias string, engine Engine) string {
	if database, known := c.constituentReadCatalog().database[alias]; known {
		return database
	}
	// Preserve the helper's unknown-alias diagnostic contract. Actual operation
	// aliases always consume the issued catalog above.
	if database := DatabaseOf(engine); database != "" {
		return database
	}
	c.mu.RLock()
	database := c.constituentNames[alias]
	c.mu.RUnlock()
	if database != "" {
		return database
	}
	return alias
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
func (c *CompositeEngine) constituentForNodePair(start, end NodeID) (Engine, NodeID, NodeID, bool, error) {
	startKind, startDB, startLocal, startQualified := ParseElementID(string(start))
	endKind, endDB, endLocal, endQualified := ParseElementID(string(end))
	if !startQualified && !endQualified {
		return nil, start, end, false, nil
	}
	if startQualified && startKind != "4" || endQualified && endKind != "4" || startQualified && endQualified && startDB != endDB {
		return nil, "", "", true, nil
	}
	selected := string(start)
	if !startQualified {
		selected = string(end)
		startLocal = string(start)
	}
	if !endQualified {
		endLocal = string(end)
	}
	engine, _, _, err := c.constituentForElementID("4", selected)
	return engine, NodeID(startLocal), NodeID(endLocal), true, err
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

// compositeReadCatalog is this CompositeEngine's immutable view topology.
// WithConstituentAccess/WithMembership issue fresh engines and fresh catalogs;
// live participant admission remains getConstituent's operation, never metadata
// discovery. No snapshot, publication frontier or entity payload is cached here.
type compositeReadSourceKey struct {
	engine                  Engine
	database, namespacePath string
}
type compositeReadCatalog struct {
	aliases    []string
	database   map[string]string
	keys       map[string]compositeReadSourceKey
	comparable map[string]bool
}

func (c *CompositeEngine) constituentReadCatalog() *compositeReadCatalog {
	c.readCatalogOnce.Do(func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		catalog := &compositeReadCatalog{database: map[string]string{}, keys: map[string]compositeReadSourceKey{}, comparable: map[string]bool{}}
		aliases := make([]string, 0, len(c.constituents))
		for alias := range c.constituents {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
		for _, alias := range aliases {
			engine := c.constituents[alias]
			database := DatabaseOf(engine)
			physicalDatabase := database
			if database == "" {
				database = c.constituentNames[alias]
			}
			if database == "" {
				database = alias
			}
			catalog.database[alias] = database
			if engine == nil {
				continue
			}
			source, path := readSourceIdentity(engine)
			catalog.keys[alias] = compositeReadSourceKey{source, physicalDatabase, path}
			catalog.comparable[alias] = source != nil && reflect.TypeOf(source).Comparable()
			catalog.aliases = append(catalog.aliases, alias)
		}
		c.readCatalog = catalog
	})
	return c.readCatalog
}

// selectEntity is the shared ordinary/MVCC/stored identity selector. aliases is
// the operation's live access-mode selection, not cached permission.
func (catalog *compositeReadCatalog) selectEntity(kind, value string, aliases []string) ([]string, string, bool, error) {
	actual, database, local, qualified := ParseElementID(value)
	if !qualified {
		return aliases, value, false, nil
	}
	if actual != kind {
		return nil, local, true, ErrInvalidID
	}
	for i, alias := range aliases {
		if catalog.database[alias] == database {
			return aliases[i : i+1], local, true, nil
		}
	}
	return nil, local, true, ErrNotFound
}
