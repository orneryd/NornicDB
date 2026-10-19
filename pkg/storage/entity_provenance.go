package storage

import "strconv"

// EntityDatabase keeps the database carried by a value across USE and Fabric
// boundaries. The executing database is only a fallback for unscoped values.
func EntityDatabase(origin, fallback string) string {
	if origin != "" {
		return origin
	}
	return fallback
}

// EntityIdentityKey is an unambiguous key for a database-local stored ID.
// Keep unscoped IDs unchanged for engines without a database namespace.
func EntityIdentityKey(database, id string) string {
	if database == "" {
		return id
	}
	return "\x00" + strconv.Itoa(len(database)) + ":" + database + id
}

func (n *Node) IdentityKey() string { return EntityIdentityKey(n.Database, string(n.ID)) }
func (e *Edge) IdentityKey() string { return EntityIdentityKey(e.Database, string(e.ID)) }

func (n *Node) ElementID(fallback string) string {
	return NodeElementID(EntityDatabase(n.Database, fallback), n.ID)
}

func (e *Edge) ElementID(fallback string) string {
	return RelationshipElementID(EntityDatabase(e.Database, fallback), e.ID)
}

func (e *Edge) StartElementID(fallback string) string {
	return NodeElementID(EntityDatabase(e.Database, fallback), e.StartNode)
}

func (e *Edge) EndElementID(fallback string) string {
	return NodeElementID(EntityDatabase(e.Database, fallback), e.EndNode)
}

// DatabaseOf follows the same wrapper boundary as QualifiedEntityID. It does
// not infer provenance from a stored ID, which may itself contain colons.
func DatabaseOf(engine Engine) string {
	for engine != nil {
		if scoped, ok := engine.(interface{ Namespace() string }); ok {
			return scoped.Namespace()
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok { return "" }
		inner := wrapper.GetInnerEngine()
		if inner == engine { return "" }
		engine = inner
	}
	return ""
}
