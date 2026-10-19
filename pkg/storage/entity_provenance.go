package storage

import (
	"strconv"
	"strings"
)

// EntityIdentityKey is an unambiguous key for a database-local stored ID.
// Keep unscoped IDs unchanged for engines without a database namespace.
func EntityIdentityKey(database, id string) string {
	if database == "" {
		if strings.HasPrefix(id, "\x00") {
			return "\x00U" + id
		}
		return id
	}
	return "\x00D" + strconv.Itoa(len(database)) + ":" + database + id
}

// IdentityKey identifies a native node independently of the executing database.
func (n *Node) IdentityKey() string { return EntityIdentityKey(n.Database, string(n.ID)) }

// IdentityKey identifies a native relationship independently of its snapshots.
func (e *Edge) IdentityKey() string { return EntityIdentityKey(e.Database, string(e.ID)) }

// ElementID uses source provenance, falling back only for unscoped values.
func (n *Node) ElementID(fallback string) string {
	if n.Database != "" {
		return SourceNodeElementID(n.Database, n.ID)
	}
	return NodeElementID(fallback, n.ID)
}

// ElementID uses source provenance, falling back only for unscoped values.
func (e *Edge) ElementID(fallback string) string {
	if e.Database != "" {
		return SourceRelationshipElementID(e.Database, e.ID)
	}
	return RelationshipElementID(fallback, e.ID)
}

// StartElementID identifies the source endpoint in the relationship's database.
func (e *Edge) StartElementID(fallback string) string {
	if e.Database != "" {
		return SourceNodeElementID(e.Database, e.StartNode)
	}
	return NodeElementID(fallback, e.StartNode)
}

// EndElementID identifies the target endpoint in the relationship's database.
func (e *Edge) EndElementID(fallback string) string {
	if e.Database != "" {
		return SourceNodeElementID(e.Database, e.EndNode)
	}
	return NodeElementID(fallback, e.EndNode)
}

// DatabaseOf follows the same wrapper boundary as QualifiedEntityID. It does
// not infer provenance from a stored ID, which may itself contain colons.
func DatabaseOf(engine Engine) string {
	for engine != nil {
		if scoped, ok := engine.(interface{ Namespace() string }); ok {
			return scoped.Namespace()
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return ""
		}
		inner := wrapper.GetInnerEngine()
		if inner == engine {
			return ""
		}
		engine = inner
	}
	return ""
}
