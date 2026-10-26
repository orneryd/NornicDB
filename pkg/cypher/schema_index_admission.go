package cypher

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) admitIndexCreation(statement braceView, name, kind, label string, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
	return e.admitIndexCreationForTargets(statement, name, kind, []string{label}, properties, entityType)
}

func (e *StorageExecutor) admitIndexCreationForTargets(statement braceView, name, kind string, targets, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
	guarded := keywordIndexFrom(statement.text(), "IF NOT EXISTS", 0, defaultKeywordScanOpts(statement)) >= 0
	return admitSchemaIndexCreation(e.storage.GetSchema(), guarded, name, kind, targets, properties, entityType)
}

// admitSchemaIndexCreation decides a new index against the schema. guarded is
// the statement's IF NOT EXISTS; a procedure-created index has none.
func admitSchemaIndexCreation(schema *storage.SchemaManager, guarded bool, name, kind string, targets, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
	for _, constraint := range schema.GetAllConstraints() {
		if constraint.Name != name {
			continue
		}
		if guarded {
			return false, nil
		}
		message := localizedError(localization.StorageSchemaConstraintAlreadyExists(name), nil)
		return false, newSemanticError("Neo.ClientError.Schema.ConstraintWithNameAlreadyExists", "ConstraintWithNameAlreadyExists", message.Error())
	}
	for _, item := range schema.GetIndexes() {
		index, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		existingName, _ := index["name"].(string)
		existingKind, _ := index["type"].(string)
		if existingKind == "PROPERTY" || existingKind == "COMPOSITE" {
			existingKind = "RANGE"
		}
		existingEntity, _ := index["entityType"].(string)
		existingTargets, _ := index["labels"].([]string)
		if relationshipTypes, ok := index["relationshipTypes"].([]string); ok && len(relationshipTypes) > 0 {
			existingTargets = relationshipTypes
			existingEntity = string(storage.ConstraintEntityRelationship)
		}
		if existingEntity == "" {
			existingEntity = string(storage.ConstraintEntityNode)
		}
		if label, ok := index["label"].(string); ok {
			existingTargets = []string{label}
		}
		existingProperties, _ := index["properties"].([]string)
		if property, ok := index["property"].(string); ok {
			existingProperties = []string{property}
		}
		equivalent := existingKind == kind && existingEntity == string(entityType) && slices.Equal(existingTargets, targets) && slices.Equal(existingProperties, properties)
		if existingName != name && !equivalent {
			continue
		}
		if guarded {
			return false, nil
		}
		code := "IndexAlreadyExists"
		if existingName == name {
			code = "IndexWithNameAlreadyExists"
			if equivalent {
				code = "EquivalentSchemaRuleAlreadyExists"
			}
		}
		diagnostic := localization.StorageSchemaIndexNameAlreadyExists(existingName)
		if owner, ok := index["owningConstraint"].(string); ok && owner != "" {
			code = "ConstraintAlreadyExists"
			diagnostic = localization.StorageSchemaConstraintAlreadyExists(owner)
			if uniqueness, ok := uniquenessConstraintNamed(schema, owner); ok && equivalent && len(uniqueness.Properties) == 1 {
				// Neo4j's wording for the index a uniqueness constraint owns (#884).
				diagnostic = localization.StorageSchemaUniquenessConstraintIndexExists(uniqueness.Label, uniqueness.Properties[0])
			}
		}
		message := localizedError(diagnostic, nil)
		return false, newSemanticError("Neo.ClientError.Schema."+code, code, message.Error())
	}
	return true, nil
}

// uniquenessConstraintNamed returns the uniqueness constraint called name.
func uniquenessConstraintNamed(schema *storage.SchemaManager, name string) (storage.Constraint, bool) {
	for _, constraint := range schema.GetAllConstraints() {
		if constraint.Name == name && constraint.Type == storage.ConstraintUnique {
			return constraint, true
		}
	}
	return storage.Constraint{}, false
}
