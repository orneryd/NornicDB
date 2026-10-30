package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// setWrites counts the properties a SET writes to one node or relationship,
// by Neo4j's properties_set rule (#678). Neo4j counts writes, not changes:
//   - x.p = v counts 1 when v is not null, or when it removes a key the
//     entity has. Writing the current value counts. A run of consecutive
//     x.p assignments to the same entity is one write per distinct key
//     (SET n.x = 1, n.y = 2, n.x = 3 counts 2); any other item (a label,
//     a map, another entity) or a new SET clause ends the run.
//   - x += map counts every entry with a value, and every null entry for a
//     key the entity has.
//   - x = map counts the same entries, plus every key the entity has that
//     the map drops.
//   - A null for a key the entity doesn't have counts in a map, and in a run
//     of more than one assignment, when the key name is known in the
//     database (Neo4j's property-key token: some write has stored a value
//     under it, even one rolled back; storage.PropertyKeyLookup). Neo4j
//     writes such a run as one map. A lone x.p = null for a key the entity
//     lacks never counts.
//
// The zero value is ready to use and knows no key names (known nil).
// Callers record each assignment before applying it, since the rule depends
// on the entity's keys at that point.
type setWrites struct {
	count int
	// known looks up the database's key names; nil knows none.
	known storage.PropertyKeyLookup
	// run holds the keys of the current run; runKeys spills past four.
	run     [4]string
	runLen  int
	runKeys []string
}

// endRun ends the current run of x.p assignments.
func (w *setWrites) endRun() {
	w.runLen = 0
	w.runKeys = w.runKeys[:0]
}

// property records x.key = value on an entity that had the key (existed) or
// not, as part of the current run; merged is true when the run has more than
// one assignment.
func (w *setWrites) property(existed bool, key string, value interface{}, merged bool) {
	if value == nil && !existed && !(merged && w.knownKey(key)) {
		return
	}
	for i := 0; i < w.runLen; i++ {
		if w.run[i] == key {
			return
		}
	}
	for _, runKey := range w.runKeys {
		if runKey == key {
			return
		}
	}
	if w.runLen < len(w.run) {
		w.run[w.runLen] = key
		w.runLen++
	} else {
		w.runKeys = append(w.runKeys, key)
	}
	w.count++
}

// propertyKeyLookup is store's key-name lookup for SET's counter
// (setWrites.known), or nil when the store keeps none.
func propertyKeyLookup(store storage.Engine) storage.PropertyKeyLookup {
	lookup, _ := store.(storage.PropertyKeyLookup)
	return lookup
}

// knownKey reports whether key is a known key name in the database.
func (w *setWrites) knownKey(key string) bool {
	return w.known != nil && w.known.PropertyKeyKnown(key)
}

// simplePropertyWrites is setWrites for a SET that is one x.key = value on an
// entity with properties before.
func simplePropertyWrites(before map[string]interface{}, key string, value interface{}) int {
	if value != nil {
		return 1
	}
	if _, existed := before[key]; existed {
		return 1
	}
	return 0
}

// mapWrites is setWrites for a SET that is one x += props (replace false) or
// x = props (replace true) on an entity with properties before.
func mapWrites(before, props map[string]interface{}, replace bool) int {
	var writes setWrites
	writes.mapEntries(before, props, replace)
	return writes.count
}

// mapEntries records x += props (replace false) or x = props (replace true)
// on an entity whose properties before the assignment are before. props
// holds the map's entries, nulls included.
func (w *setWrites) mapEntries(before, props map[string]interface{}, replace bool) {
	w.endRun()
	for key, value := range props {
		if value != nil {
			w.count++
		} else if _, existed := before[key]; existed || w.knownKey(key) {
			w.count++
		}
	}
	if !replace {
		return
	}
	for key := range before {
		if _, kept := props[key]; !kept {
			w.count++
		}
	}
}

// setPropertyRun holds the values of a run of x.p = <expr> assignments to
// one entity (setWrites' run: consecutive, same entity, same SET clause).
// Neo4j evaluates every right-hand side of a run before it writes any of
// them, so SET n.a = 1, n.b = n.a + 1 reads n.a as it was before the SET
// (#907). Callers add each evaluated value and apply the run when it ends.
type setPropertyRun struct {
	// The first assignments of a run sit in the arrays (no allocation);
	// the rest spill to overflow.
	length   int
	keys     [8]string
	values   [8]interface{}
	overflow []setPropertyAssignment
}

type setPropertyAssignment struct {
	key   string
	value interface{}
}

func (r *setPropertyRun) add(key string, value interface{}) {
	if r.length < len(r.keys) {
		r.keys[r.length], r.values[r.length] = key, value
		r.length++
		return
	}
	r.overflow = append(r.overflow, setPropertyAssignment{key: key, value: value})
}

// each calls write with the run's assignments in order and empties the run.
func (r *setPropertyRun) each(write func(key string, value interface{})) {
	for i := 0; i < r.length; i++ {
		write(r.keys[i], r.values[i])
		r.values[i] = nil
	}
	for _, assignment := range r.overflow {
		write(assignment.key, assignment.value)
	}
	r.length, r.overflow = 0, r.overflow[:0]
}

// size is the number of assignments in the run.
func (r *setPropertyRun) size() int {
	return r.length + len(r.overflow)
}

// applyToNode writes the run's values to node in order, recording each on
// writes against the node's properties as they are then, and empties the
// run.
func (r *setPropertyRun) applyToNode(node *storage.Node, writes *setWrites) {
	merged := r.size() > 1
	r.each(func(key string, value interface{}) {
		_, existed := node.Properties[key]
		writes.property(existed, key, value, merged)
		setNodeProperty(node, key, value)
	})
}

// applyToRelationship is applyToNode for a relationship.
func (r *setPropertyRun) applyToRelationship(edge *storage.Edge, writes *setWrites) {
	merged := r.size() > 1
	r.each(func(key string, value interface{}) {
		_, existed := edge.Properties[key]
		writes.property(existed, key, value, merged)
		setRelationshipProperty(edge, key, value)
	})
}
