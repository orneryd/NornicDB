package storage

import (
	"context"
	"errors"
)

// constituentsForEntity is the shared source selector for ordinary and MVCC
// identity reads. A qualified reference cannot fall through to another source,
// so the source that owns it is opened here once and returned beside the
// selector result; the fan-out callers leave that source nil. qualified is true
// even when the referenced database is unavailable or the kind differs.
func (c *CompositeEngine) constituentsForEntity(kind, value string) ([]string, Engine, string, bool, error) {
	actual, database, local, qualified := ParseElementID(value)
	aliases := c.getConstituentsForRead()
	if !qualified {
		return aliases, nil, value, false, nil
	}
	if actual != kind {
		return nil, nil, local, true, nil
	}
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			// A readable source configured for the referenced database could
			// hold the entity, so its participant-open failure is the answer;
			// only an unrelated source's failure may be skipped.
			if c.configuredConstituentDatabase(alias) == database {
				return nil, nil, local, true, err
			}
			continue
		}
		if c.constituentDatabase(alias, engine) == database {
			return []string{alias}, engine, local, true, nil
		}
	}
	return nil, nil, local, true, nil
}

// configuredConstituentDatabase names the database a readable alias is
// configured for without opening the participant. A qualified reference must
// report the failure of the source that owns it instead of not found; the
// database-name decision stays with constituentDatabase.
func (c *CompositeEngine) configuredConstituentDatabase(alias string) string {
	c.mu.RLock()
	engine := c.constituents[alias]
	c.mu.RUnlock()
	return c.constituentDatabase(alias, engine)
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
	if node == nil || node.RuntimeMetadata().Database != "" {
		return node
	}
	copy := *node
	{
		metadata := copy.RuntimeMetadata()
		metadata.Database = database
		copy.SetRuntimeMetadata(metadata)
	}
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
	if node == nil || node.RuntimeMetadata().Database != "" {
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

// entitiesFromDatabase stamps a source identity without mutating source-owned
// entities or slices. Already-bound collections retain their backing array.
// One bounded traversal owns both projection and cancellation for both kinds.
func entitiesFromDatabase[T *Node | *Edge](ctx context.Context, entities []T, database string) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := entities
	copied := false
	for start := 0; start < len(entities); start += bulkReadCancellationInterval {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+bulkReadCancellationInterval, len(entities))
		// Select the entity representation once per bounded block. The inner
		// loops inspect concrete metadata without an indirect per-record call.
		switch values := any(entities[start:end]).(type) {
		case []*Node:
			for i, value := range values {
				if value != nil && value.RuntimeMetadata().Database == "" {
					if !copied {
						result = append([]T(nil), entities...)
						copied = true
					}
					result[start+i] = any(nodeFromDatabase(value, database)).(T)
				}
			}
		case []*Edge:
			for i, value := range values {
				if value != nil && value.Database == "" {
					if !copied {
						result = append([]T(nil), entities...)
						copied = true
					}
					result[start+i] = any(edgeFromDatabase(value, database)).(T)
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
	_, source, _, _, err := c.constituentsForEntity("4", selected)
	return source, NodeID(startLocal), NodeID(endLocal), true, err
}

func (c *CompositeEngine) constituentNodeElementID(alias string, engine Engine, id NodeID) NodeID {
	return NodeID(SourceNodeElementID(c.constituentDatabase(alias, engine), id))
}

// ConstituentStorage resolves a native entity's database through the same
// selector as qualified reads, retaining the statement's participant view.
func (c *CompositeEngine) ConstituentStorage(database string) (Engine, error) {
	aliases, _, _, _, err := c.constituentsForEntity("4", SourceNodeElementID(database, ""))
	if err != nil {
		return nil, err
	}
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
	aliases, _, _, _, err := c.constituentsForEntity("4", SourceNodeElementID(edge.Database, edge.StartNode))
	if err != nil {
		return "", true, err
	}
	if len(aliases) == 0 {
		return "", true, ErrNotFound
	}
	return aliases[0], true, nil
}

// constituentNotFoundMode identifies how a routed constituent read recognizes
// an identity the source does not hold. The mode is part of each public
// contract: ordinary node/edge reads and the update/delete routes compare the
// sentinel directly, while MVCC provider reads accept a wrapped sentinel. The
// modes are not unified, because that would change what each API publishes for
// a wrapped not-found.
type constituentNotFoundMode int

const (
	// constituentNotFoundDirect keeps the sentinel comparison of the ordinary
	// node/edge reads and the update/delete routes.
	constituentNotFoundDirect constituentNotFoundMode = iota
	// constituentNotFoundWrapped accepts a wrapped sentinel from MVCC
	// providers and the projected read tail.
	constituentNotFoundWrapped
)

func (mode constituentNotFoundMode) matches(err error) bool {
	if mode == constituentNotFoundWrapped {
		return errors.Is(err, ErrNotFound)
	}
	return err == ErrNotFound
}

// constituentWriteAdmission identifies which participant a routed write opens
// once its presence read has resolved the source. The two admissions are
// separate public contracts: an entity mutation is admitted by the source's
// configured write role, while a capability maintenance write of derived state
// is admitted by the same readable participant that resolved the identity,
// exactly as the capability file's other maintenance dispatchers are.
type constituentWriteAdmission int

const (
	// constituentWriteEntityMutation opens the resolved source for writing, so
	// a read-only constituent refuses the node or edge mutation.
	constituentWriteEntityMutation constituentWriteAdmission = iota
	// constituentWriteMaintenance keeps the resolved readable participant and
	// writes maintenance of derived state through it.
	constituentWriteMaintenance
)

// readFirstConstituent is the single owner of the composite's first-match
// identity reads. It opens each selected source once through the access owner
// and returns the first value that source resolves. A source's not-found result
// continues the walk in the caller's declared mode; any other failure is
// published unchanged. retainFailedValue keeps the failing source's value for
// the head reads, whose API returns it beside the error; the value reads discard
// it. project is nil when the value carries no source identity.
func readFirstConstituent[V any](
	composite *CompositeEngine,
	aliases []string,
	notFound constituentNotFoundMode,
	retainFailedValue bool,
	read func(Engine) (V, error),
	project func(V, string, Engine) V,
) (V, error) {
	for _, alias := range aliases {
		engine, err := composite.getConstituent(alias)
		if err != nil {
			var zero V
			return zero, err
		}
		value, err := read(engine)
		if err == nil {
			if project != nil {
				value = project(value, alias, engine)
			}
			return value, nil
		}
		if !notFound.matches(err) {
			if !retainFailedValue {
				var zero V
				value = zero
			}
			return value, err
		}
	}
	var zero V
	return zero, ErrNotFound
}

// writeFirstConstituent is the single owner of the composite's first-match
// write routes. It finds the source that holds the identity with the caller's
// presence read, opens that source under the caller's write admission, and runs
// the caller's write. present must not project or mutate the entity. The
// qualified-observation fast path stays with its adapter because that path must
// not reread the entity. missing is the adapter's documented result when no
// readable source holds the identity (ErrNotFound, or the update conflict
// decision).
func writeFirstConstituent(
	composite *CompositeEngine,
	aliases []string,
	notFound constituentNotFoundMode,
	admission constituentWriteAdmission,
	present func(Engine) error,
	write func(Engine) error,
	missing error,
) error {
	for _, alias := range aliases {
		engine, err := composite.getConstituent(alias)
		if err != nil {
			return err
		}
		err = present(engine)
		if err == nil {
			writer := engine
			if admission == constituentWriteEntityMutation {
				writer, err = composite.getConstituentForWrite(alias)
				if err != nil {
					return err
				}
			}
			return write(writer)
		}
		if !notFound.matches(err) {
			return err
		}
	}
	return missing
}
