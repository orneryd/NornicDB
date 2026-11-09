package storage

// temporalCreationGroups assigns transient interval groups during TEMPORAL DDL.
// Scalar keys retain their legacy display partition. List hashes only select
// candidates: fallback tokens and collisions must still pass the shared equality.
type temporalCreationGroups struct {
	spec    temporalKeySpec
	next    int
	scalars map[string]int
	lists   map[string][]temporalCreationGroup
}

type temporalCreationGroup struct {
	keyValue interface{}
	id       int
}

func (g *temporalCreationGroups) groupFor(keyValue interface{}) int {
	if !g.spec.listBearing(keyValue) {
		key := g.spec.groupString(keyValue)
		if id, ok := g.scalars[key]; ok {
			return id
		}
		if g.scalars == nil {
			g.scalars = make(map[string]int)
		}
		id := g.nextGroup()
		g.scalars[key] = id
		return id
	}

	key := g.spec.keyHash(keyValue)
	for _, candidate := range g.lists[key] {
		if g.spec.keyEqual(candidate.keyValue, keyValue) {
			return candidate.id
		}
	}
	if g.lists == nil {
		g.lists = make(map[string][]temporalCreationGroup)
	}
	id := g.nextGroup()
	g.lists[key] = append(g.lists[key], temporalCreationGroup{keyValue: keyValue, id: id})
	return id
}

func (g *temporalCreationGroups) nextGroup() int {
	id := g.next
	g.next++
	return id
}
