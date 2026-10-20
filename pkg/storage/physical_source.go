package storage

import "strings"

// PhysicalLocationKey is the shared location identity for transaction admission
// and native graph provenance. Preserve the shipped Fabric key strings exactly.
// These keys are internal: element IDs and errors use catalog qualifiers.
func PhysicalLocationKey(uri, database string, remote bool) string {
	if !remote {
		return "local:" + strings.TrimSpace(database)
	}
	return "remote:" + strings.TrimSpace(uri) + "|" + strings.TrimSpace(database)
}

// entitySource is query-value metadata, never a property or a persisted field.
// The public qualifier and the physical location have independent lifetimes:
// two catalogs can name one source, and two remotes can name one database.
type entitySource struct{ key, qualifier string }

func remoteSourceKey(engine Engine) string {
	for depth := 0; engine != nil && depth < 16; depth++ {
		if remote, ok := engine.(interface{ PhysicalSourceKey() string }); ok {
			return remote.PhysicalSourceKey()
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok || wrapper.GetInnerEngine() == engine {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	return ""
}

func (r *RemoteEngine) PhysicalSourceKey() string { return r.sourceKey }
func (r *RemoteEngine) Namespace() string         { return r.database }

// WithIdentity copies a native value without losing its physical source when
// the caller changes its row view or restores an internal graph key.
func (n *Node) WithIdentity(identity EntityIdentity) *Node {
	if n == nil {
		return nil
	}
	copy := *n
	if identity.Database == "" && identity.Source != "" && identity.Source == n.source.key {
		identity.Database = n.source.qualifier
	}
	copy.ID, copy.Database = NodeID(identity.LocalID), identity.Database
	copy.source = entitySource{identity.Source, identity.Database}
	return &copy
}

func (e *Edge) WithIdentity(identity EntityIdentity) *Edge {
	if e == nil {
		return nil
	}
	copy := *e
	if identity.Database == "" && identity.Source != "" && identity.Source == e.source.key {
		identity.Database = e.source.qualifier
	}
	copy.ID, copy.Database = EdgeID(identity.LocalID), identity.Database
	copy.source = entitySource{identity.Source, identity.Database}
	return &copy
}

// Resolve consults bounded source metadata without reading entities or opening
// transaction participants. It is also the adapter from a decoded graph key.
func (id EntityIdentity) Resolve(engine Engine) (EntityIdentity, bool) {
	for depth := 0; engine != nil && depth < 16; depth++ {
		if catalog, ok := engine.(interface {
			CanonicalEntityIdentity(EntityIdentity) (EntityIdentity, bool)
		}); ok {
			return catalog.CanonicalEntityIdentity(id)
		}
		if source := remoteSourceKey(engine); source != "" {
			if id.Source != "" && id.Source != source {
				return EntityIdentity{}, false
			}
			if id.Database != "" && id.Database != DatabaseOf(engine) && id.Source == "" {
				return EntityIdentity{}, false
			}
			id.Source, id.Database = source, DatabaseOf(engine)
			return id, true
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok || wrapper.GetInnerEngine() == engine {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	if id.Source != "" {
		return EntityIdentity{}, false
	}
	return id, true
}

// SourceEngine resolves metadata/property ownership without reading data or
// opening transaction participants. Actual data operations keep their outer
// transaction/admission engine and use LookupID instead.
func (id EntityIdentity) SourceEngine(engine Engine) (Engine, bool) {
	selected := engine
	if engine != nil && id.Source == "" && (id.Database == "" || id.Database == DatabaseOf(engine)) {
		return engine, true
	}
	for depth := 0; engine != nil && depth < 16; depth++ {
		if sources, ok := engine.(interface {
			EntitySource(EntityIdentity) (Engine, error)
		}); ok {
			source, err := sources.EntitySource(id)
			return source, err == nil && source != nil
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok || wrapper.GetInnerEngine() == engine {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	if selected == nil {
		return nil, false
	}
	if id.Database != "" && id.Database != DatabaseOf(selected) && id.Source == "" {
		return nil, false
	}
	if _, ok := id.Resolve(selected); !ok {
		return nil, false
	}
	return selected, true
}
