package cypher

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) admitIndexCreation(query, name, kind, label string, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
	return e.admitIndexCreationForTargets(query, name, kind, []string{label}, properties, entityType)
}

func (e *StorageExecutor) admitIndexCreationForTargets(query, name, kind string, targets, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
	guarded := keywordIndexFrom(query, "IF NOT EXISTS", 0, defaultKeywordScanOpts()) >= 0
	for _, item := range e.storage.GetSchema().GetIndexes() {
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
		message := localizedError(localization.StorageSchemaIndexNameAlreadyExists(existingName), nil)
		return false, newSemanticError("Neo.ClientError.Schema."+code, code, message.Error())
	}
	return true, nil
}
