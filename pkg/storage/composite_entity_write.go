package storage

import (
    "errors"

    "github.com/orneryd/nornicdb/pkg/localization"
)

// constituentsForIdentity is the sole source selector for native entities and
// external lookup references. Native local IDs stay opaque; only the external
// reference adapter parses an element ID. A selected source never falls back.
func (c *CompositeEngine) constituentsForIdentity(identity EntityIdentity, candidates []string) []string {
    if identity.Database == "" { return candidates }
    for _, alias := range candidates {
        engine, err := c.getConstituent(alias)
        if err == nil && c.constituentDatabase(alias, engine) == identity.Database { return []string{alias} }
    }
    return nil
}

// EntitySource exposes the selected physical source for metadata readers.
// Data operations still use the composite/transaction owner, never this escape.
func (c *CompositeEngine) EntitySource(identity EntityIdentity) (Engine, error) {
    if identity.Database == "" { return nil, ErrNotFound }
    aliases := c.constituentsForIdentity(identity, c.getConstituentsForRead())
    if len(aliases) == 0 { return nil, ErrNotFound }
    return c.getConstituent(aliases[0])
}

// withExistingIdentity keeps point mutations on the same native source as
// reads. The operation receives the storage-local ID only after selection.
func (c *CompositeEngine) withExistingIdentity(kind string, identity EntityIdentity, apply func(Engine, string) error) error {
    for _, alias := range c.constituentsForIdentity(identity, c.getConstituentsForRead()) {
        engine, err := c.getConstituent(alias)
        if err != nil { continue }
        if kind == "4" { _, err = engine.GetNode(NodeID(identity.LocalID)) } else { _, err = engine.GetEdge(EdgeID(identity.LocalID)) }
        if err == nil { return apply(engine, identity.LocalID) }
        if !errors.Is(err, ErrNotFound) { return err }
    }
    return ErrNotFound
}

func (c *CompositeEngine) nodeCreateSource(node *Node) (string, Engine, error) {
    if node == nil { return "", nil, ErrInvalidData }
    candidates := c.getConstituentsForWrite()
    if len(candidates) == 0 { return "", nil, localizedError(localization.StorageCompositeNoWritableConstituents(), nil) }
    alias := ""
    if node.Database != "" {
        if selected := c.constituentsForIdentity(node.Identity(), candidates); len(selected) > 0 { alias = selected[0] }
    } else { alias = c.routeWrite("create_node", node.Labels, node.Properties, candidates) }
    if alias == "" { return "", nil, localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil) }
    engine, err := c.getConstituent(alias)
    return alias, engine, err
}

// BindRelationshipEndpoints retains both native endpoint identities before a
// CREATE/MERGE plan is stored. One stored relationship has exactly one source;
// a cross-source pair cannot be reinterpreted as a local pair or self-loop.
func BindRelationshipEndpoints(edge *Edge, start, end *Node) error {
    if edge == nil || start == nil || end == nil { return ErrInvalidData }
    left, right := start.Identity(), end.Identity()
    if left.Database != "" && right.Database != "" && left.Database != right.Database {
        return localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil)
    }
    database := left.Database
    if database == "" { database = right.Database }
    edge.StartNode, edge.EndNode = NodeID(left.LocalID), NodeID(right.LocalID)
    edge.Database = database
    return nil
}

// edgeCreateSource is shared by single and bulk creation. Scoped native values
// never parse their opaque IDs. Unscoped legacy API values may use qualified
// endpoints, resolved through the same source selector before any write.
func (c *CompositeEngine) edgeCreateSource(edge *Edge) (string, Engine, *Edge, error) {
    if edge == nil { return "", nil, nil, localizedError(localization.StorageCompositeEdgeRequired(), nil) }
    writable := c.getConstituentsForWrite()
    if len(writable) == 0 { return "", nil, nil, localizedError(localization.StorageCompositeNoWritableConstituents(), nil) }
    prepared := *edge
    if prepared.Database == "" {
        leftKind, leftDB, left, leftScoped := ParseElementID(string(prepared.StartNode))
        rightKind, rightDB, right, rightScoped := ParseElementID(string(prepared.EndNode))
        if leftScoped && leftKind != "4" || rightScoped && rightKind != "4" || leftScoped && rightScoped && leftDB != rightDB {
            return "", nil, nil, localizedError(localization.StorageCompositeWriteTargetAmbiguous(), nil)
        }
        if leftScoped { prepared.Database, prepared.StartNode = leftDB, NodeID(left) }
        if rightScoped { prepared.Database, prepared.EndNode = rightDB, NodeID(right) }
    }
    candidates := c.constituentsForIdentity(prepared.Identity(), writable)
    for _, alias := range candidates {
        engine, err := c.getConstituent(alias)
        if err != nil { continue }
        if prepared.Database == "" {
            if _, err := engine.GetNode(prepared.StartNode); errors.Is(err, ErrNotFound) { continue } else if err != nil { return "", nil, nil, err }
            if _, err := engine.GetNode(prepared.EndNode); errors.Is(err, ErrNotFound) { continue } else if err != nil { return "", nil, nil, err }
        }
        prepared.Database = c.constituentDatabase(alias, engine)
        return alias, engine, &prepared, nil
    }
    // Preserve the existing read-only diagnostic, without writing through it.
    for _, alias := range c.constituentsForIdentity(prepared.Identity(), c.getConstituentsForRead()) {
        engine, err := c.getConstituent(alias)
        if err != nil { continue }
        if _, err := engine.GetNode(prepared.StartNode); err == nil { return "", nil, nil, localizedError(localization.StorageCompositeReadOnlyStartNode(alias), nil) }
    }
    return "", nil, nil, localizedError(localization.StorageCompositeStartNodeNotFound(), nil)
}