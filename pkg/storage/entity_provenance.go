package storage

import (
	"fmt"
	"strconv"
	"strings"
)

// EntityIdentity is the source-qualified identity of a stored native value.
// A stored local ID is opaque: only an explicit external element ID is parsed.
type EntityIdentity struct {
	Database string
	LocalID  string
}

func (id EntityIdentity) Key() string { return EntityIdentityKey(id.Database, id.LocalID) }

// InDatabase encodes the identity in an existing projection's ID domain. Local
// IDs remain local inside that database's catalog; foreign values are qualified.
func (id EntityIdentity) InDatabase(database string) string {
	if id.Database != "" && id.Database != database {
		return SourceNodeElementID(id.Database, NodeID(id.LocalID))
	}
	return id.LocalID
}

func (n *Node) Identity() EntityIdentity {
	return EntityIdentity{n.RuntimeMetadata().Database, string(n.ID)}
}
func (e *Edge) Identity() EntityIdentity      { return EntityIdentity{e.Database, string(e.ID)} }
func (e *Edge) StartIdentity() EntityIdentity { return EntityIdentity{e.Database, string(e.StartNode)} }
func (e *Edge) EndIdentity() EntityIdentity   { return EntityIdentity{e.Database, string(e.EndNode)} }

// EntityIdentityFromKey decodes only the internal key produced by Key. External
// string arguments belong to their API's ID domain and must not enter here.
func EntityIdentityFromKey(key string) EntityIdentity {
	if strings.HasPrefix(key, "\x00U") {
		return EntityIdentity{LocalID: key[2:]}
	}
	if strings.HasPrefix(key, "\x00D") {
		length, body, found := strings.Cut(key[2:], ":")
		if size, err := strconv.Atoi(length); found && err == nil && size >= 0 && size <= len(body) {
			return EntityIdentity{Database: body[:size], LocalID: body[size:]}
		}
	}
	return EntityIdentity{LocalID: key}
}

// NodeLookupID preserves source identity at the engine boundary. A qualified
// foreign value cannot be reinterpreted as a local ID in a namespaced engine.
func (id EntityIdentity) NodeLookupID(engine Engine) (NodeID, bool) {
	value, ok := id.LookupID(engine, "4")
	return NodeID(value), ok
}

// LookupID translates an identity for a storage read without dropping a source
// selection when the engine is composite. kind is the element ID entity kind.
func (id EntityIdentity) LookupID(engine Engine, kind string) (string, bool) {
	if CompositeDatabaseOf(engine) != "" {
		if id.Database != "" {
			return kind + ":" + id.Database + ":" + id.LocalID, true
		}
		return id.LocalID, true
	}
	if source := DatabaseOf(engine); id.Database != "" && source != "" && source != id.Database {
		return "", false
	}
	return id.LocalID, true
}

// CompositeDatabaseOf identifies a composite through read/admission wrappers;
// it does not unwrap the actual operation or bypass those owners.
func CompositeDatabaseOf(engine Engine) string {
	for depth := 0; engine != nil && depth < 16; depth++ {
		if composite, ok := engine.(interface{ CompositeName() string }); ok {
			return composite.CompositeName()
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	return ""
}

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
func (n *Node) IdentityKey() string { return n.Identity().Key() }

// IdentityKey identifies a native relationship independently of its snapshots.
func (e *Edge) IdentityKey() string { return e.Identity().Key() }

// ElementID uses source provenance, falling back only for unscoped values.
func (n *Node) ElementID(fallback string) string {
	if n.RuntimeMetadata().Database != "" {
		return SourceNodeElementID(n.RuntimeMetadata().Database, n.ID)
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

// SetEndpoints binds an edge to native endpoint identities before storage can
// discard their source. A stored relationship cannot span physical databases.
func (edge *Edge) SetEndpoints(start, end *Node) error {
	if start.RuntimeMetadata().Database != "" && end.RuntimeMetadata().Database != "" && start.RuntimeMetadata().Database != end.RuntimeMetadata().Database {
		return fmt.Errorf("cannot create a relationship between databases %q and %q", start.RuntimeMetadata().Database, end.RuntimeMetadata().Database)
	}
	edge.StartNode, edge.EndNode = start.ID, end.ID
	edge.Database = start.RuntimeMetadata().Database
	if edge.Database == "" {
		edge.Database = end.RuntimeMetadata().Database
	}
	return nil
}
