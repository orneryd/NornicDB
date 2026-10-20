package storage

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// constituentsForIdentity is the sole source selector for native entities and
// external lookup references. Native local IDs stay opaque; only the external
// reference adapter parses an element ID. A selected source never falls back.
func (c *CompositeEngine) constituentsForIdentity(identity EntityIdentity, candidates []string) []string {
	if identity.Database == "" && identity.Source == "" {
		return candidates
	}
	identity, ok := c.CanonicalEntityIdentity(identity)
	if !ok {
		return nil
	}
	for _, alias := range candidates {
		// Select from immutable catalog metadata before opening a physical
		// participant. Looking for one native source must not open all others.
		c.mu.RLock()
		source := c.sources[alias]
		c.mu.RUnlock()
		if source.key == identity.Source && source.qualifier == identity.Database {
			return []string{alias}
		}
	}
	return nil
}

// EntitySource exposes the selected physical source for metadata readers.
// Data operations still use the composite/transaction owner, never this escape.
func (c *CompositeEngine) EntitySource(identity EntityIdentity) (Engine, error) {
	if identity.Database == "" && identity.Source == "" {
		return nil, ErrNotFound
	}
	aliases := c.constituentsForIdentity(identity, c.getConstituentsForRead())
	if len(aliases) == 0 {
		return nil, ErrNotFound
	}
	return c.getConstituent(aliases[0])
}

// withExistingIdentity keeps point mutations on the same native source as
// reads. The operation receives the storage-local ID only after selection.
func (c *CompositeEngine) withExistingIdentity(kind string, identity EntityIdentity, apply func(Engine, string) error) error {
	for _, alias := range c.constituentsForIdentity(identity, c.getConstituentsForRead()) {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if kind == "4" {
			_, err = engine.GetNode(NodeID(identity.LocalID))
		} else {
			_, err = engine.GetEdge(EdgeID(identity.LocalID))
		}
		if err == nil {
			engine, err = c.constituentForOperation(alias, true)
			if err != nil {
				return err
			}
			return apply(engine, identity.LocalID)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	return ErrNotFound
}

func (c *CompositeEngine) nodeCreateSource(node *Node) (string, Engine, error) {
	if node == nil {
		return "", nil, ErrInvalidData
	}
	candidates := c.getConstituentsForWrite()
	if len(candidates) == 0 {
		return "", nil, localizedError(localization.StorageCompositeNoWritableConstituents(), nil)
	}
	alias := ""
	if node.Database != "" {
		if selected := c.constituentsForIdentity(node.Identity(), candidates); len(selected) > 0 {
			alias = selected[0]
		}
	} else {
		alias = c.routeWrite("create_node", node.Labels, node.Properties, candidates)
	}
	if alias == "" {
		return "", nil, localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil)
	}
	engine, err := c.constituentForOperation(alias, true)
	return alias, engine, err
}

// BindRelationshipEndpoints retains both native endpoint identities before a
// CREATE/MERGE plan is stored. One stored relationship has exactly one source;
// a cross-source pair cannot be reinterpreted as a local pair or self-loop.
func BindRelationshipEndpoints(edge *Edge, start, end *Node) error {
	if edge == nil || start == nil || end == nil {
		return ErrInvalidData
	}
	left, right := start.Identity(), end.Identity()
	if left.physicalSource() != "" && right.physicalSource() != "" && left.physicalSource() != right.physicalSource() {
		return localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil)
	}
	database := left.Database
	if database == "" {
		database = right.Database
	}
	edge.StartNode, edge.EndNode = NodeID(left.LocalID), NodeID(right.LocalID)
	edge.Database = database
	edge.source = start.source
	if edge.source.key == "" {
		edge.source = end.source
	}
	return nil
}

// edgeCreateSource is shared by single and bulk creation. Scoped native values
// never parse their opaque IDs. Unscoped legacy API values may use qualified
// endpoints, resolved through the same source selector before any write.
func (c *CompositeEngine) edgeCreateSource(edge *Edge) (string, Engine, *Edge, error) {
	if edge == nil {
		return "", nil, nil, localizedError(localization.StorageCompositeEdgeRequired(), nil)
	}
	writable := c.getConstituentsForWrite()
	if len(writable) == 0 {
		return "", nil, nil, localizedError(localization.StorageCompositeNoWritableConstituents(), nil)
	}
	prepared := *edge
	if prepared.Database == "" && prepared.source.key == "" {
		left, right, qualified, valid := c.qualifiedNodePair(prepared.StartNode, prepared.EndNode)
		if !valid {
			return "", nil, nil, localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil)
		}
		if qualified {
			prepared.Database, prepared.StartNode, prepared.EndNode = left.Database, NodeID(left.LocalID), NodeID(right.LocalID)
			prepared.source = entitySource{left.Source, left.Database}
		}
	}
	candidates := c.constituentsForIdentity(prepared.Identity(), writable)
	for _, alias := range candidates {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if prepared.Database == "" {
			if _, err := engine.GetNode(prepared.StartNode); errors.Is(err, ErrNotFound) {
				continue
			} else if err != nil {
				return "", nil, nil, err
			}
			if _, err := engine.GetNode(prepared.EndNode); errors.Is(err, ErrNotFound) {
				continue
			} else if err != nil {
				return "", nil, nil, err
			}
		}
		prepared.Database = c.constituentDatabase(alias, engine)
		identity := c.sourceIdentity(alias, prepared.Identity())
		prepared = *prepared.WithIdentity(identity)
		engine, err = c.constituentForOperation(alias, true)
		if err != nil {
			return "", nil, nil, err
		}
		return alias, engine, &prepared, nil
	}
	// Preserve the existing read-only diagnostic, without writing through it.
	for _, alias := range c.constituentsForIdentity(prepared.Identity(), c.getConstituentsForRead()) {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if _, err := engine.GetNode(prepared.StartNode); err == nil {
			return "", nil, nil, localizedError(localization.StorageCompositeReadOnlyStartNode(alias), nil)
		}
	}
	return "", nil, nil, localizedError(localization.StorageCompositeStartNodeNotFound(), nil)
}
