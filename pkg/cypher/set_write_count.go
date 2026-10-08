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
//
// A null for a key the entity doesn't have never counts here. Neo4j also
// counts it in a map or a run when the key name has been used anywhere in the
// database before (a property-key token exists); NornicDB keeps no such
// registry, so it counts as Neo4j does for key names never used before.
//
// The zero value is ready to use. Callers record each assignment before
// applying it, since the rule depends on the entity's keys at that point.
type setWrites struct {
	count int
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
// not, as part of the current run.
func (w *setWrites) property(existed bool, key string, value interface{}) {
	if value == nil && !existed {
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
		} else if _, existed := before[key]; existed {
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

// applyToNode writes the run's values to node in order, recording each on
// writes against the node's properties as they are then, and empties the
// run.
func (r *setPropertyRun) applyToNode(node *storage.Node, writes *setWrites) {
	r.each(func(key string, value interface{}) {
		_, existed := node.Properties[key]
		writes.property(existed, key, value)
		setNodeProperty(node, key, value)
	})
}

// applyToRelationship is applyToNode for a relationship.
func (r *setPropertyRun) applyToRelationship(edge *storage.Edge, writes *setWrites) {
	r.each(func(key string, value interface{}) {
		_, existed := edge.Properties[key]
		writes.property(existed, key, value)
		setRelationshipProperty(edge, key, value)
	})
}
