package storage

import (
	"sort"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Token lookup indexes, as Neo4j 5 lists them (#530).
//
// A new Neo4j database has one node label lookup index and one relationship
// type lookup index. They are schema objects: SHOW INDEXES lists them, DROP
// INDEX removes them, and CREATE LOOKUP INDEX recreates them, one per entity
// type. NornicDB's label and type lookups are always built in, so dropping
// the index only removes the schema object; queries answer the same, as
// Neo4j's do without the index.

// Neo4j's names for a new database's lookup indexes (the same in every
// database, derived from the index descriptor).
const (
	DefaultNodeLookupIndexName         = "index_343aff4e"
	DefaultRelationshipLookupIndexName = "index_f7700477"
)

// SchemaLookupIndexDef is a persisted token lookup index.
type SchemaLookupIndexDef struct {
	Name       string               `json:"name"`
	EntityType ConstraintEntityType `json:"entity_type"`
}

// defaultLookupIndexes are the lookup indexes a database starts with, keyed
// by entity type.
func defaultLookupIndexes() map[ConstraintEntityType]string {
	return map[ConstraintEntityType]string{
		ConstraintEntityNode:         DefaultNodeLookupIndexName,
		ConstraintEntityRelationship: DefaultRelationshipLookupIndexName,
	}
}

// LookupIndexName is the name of the lookup index for entityType, if the
// schema has one.
func (sm *SchemaManager) LookupIndexName(entityType ConstraintEntityType) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	name, ok := sm.lookupIndexes[entityType]
	return name, ok
}

// AddLookupIndex adds the lookup index for entityType named name. There is
// at most one per entity type; the caller reports an existing one (Neo4j's
// IndexAlreadyExists). An empty name is the default name.
func (sm *SchemaManager) AddLookupIndex(name string, entityType ConstraintEntityType) error {
	if name == "" {
		name = defaultLookupIndexes()[entityType]
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.lookupIndexes[entityType]; ok {
		pattern := "(:<any-labels>)"
		if entityType == ConstraintEntityRelationship {
			pattern = "()-[:<any-types>]-()"
		}
		return localizedError(localization.StorageSchemaLookupIndexAlreadyExists(pattern), nil)
	}
	if sm.indexNameTakenLocked(name) {
		return localizedError(localization.StorageSchemaIndexNameAlreadyExists(name), nil)
	}
	sm.lookupIndexes[entityType] = name
	if sm.persist != nil {
		if err := sm.persist(sm.exportDefinitionLocked()); err != nil {
			delete(sm.lookupIndexes, entityType)
			return err
		}
	}
	return nil
}

// dropLookupIndexLocked removes the lookup index named name, reporting
// whether there was one. The caller holds sm.mu and persists.
func (sm *SchemaManager) dropLookupIndexLocked(name string) (ConstraintEntityType, bool) {
	for entityType, existing := range sm.lookupIndexes {
		if existing == name {
			delete(sm.lookupIndexes, entityType)
			return entityType, true
		}
	}
	return "", false
}

// indexNameTakenLocked reports whether an index of any kind is named name.
func (sm *SchemaManager) indexNameTakenLocked(name string) bool {
	if _, ok := sm.compositeIndexes[name]; ok {
		return true
	}
	if _, ok := sm.fulltextIndexes[name]; ok {
		return true
	}
	if _, ok := sm.vectorIndexes[name]; ok {
		return true
	}
	if _, ok := sm.rangeIndexes[name]; ok {
		return true
	}
	for _, idx := range sm.propertyIndexes {
		if idx.Name == name {
			return true
		}
	}
	for _, existing := range sm.lookupIndexes {
		if existing == name {
			return true
		}
	}
	return false
}

// exportLookupIndexesLocked is the persisted form of the lookup indexes. It
// is never nil, so a schema with none dropped reads back with none.
func (sm *SchemaManager) exportLookupIndexesLocked() *[]SchemaLookupIndexDef {
	defs := make([]SchemaLookupIndexDef, 0, len(sm.lookupIndexes))
	for entityType, name := range sm.lookupIndexes {
		defs = append(defs, SchemaLookupIndexDef{Name: name, EntityType: entityType})
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].EntityType < defs[j].EntityType })
	return &defs
}

// importLookupIndexes reads persisted lookup indexes. A definition written
// before lookup indexes were persisted (nil) has the default ones.
func importLookupIndexes(defs *[]SchemaLookupIndexDef) map[ConstraintEntityType]string {
	if defs == nil {
		return defaultLookupIndexes()
	}
	out := make(map[ConstraintEntityType]string, len(*defs))
	for _, def := range *defs {
		out[defaultConstraintEntityType(def.EntityType)] = def.Name
	}
	return out
}
