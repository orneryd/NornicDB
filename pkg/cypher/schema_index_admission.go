package cypher

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) admitIndexCreation(query, name, kind, label string, properties []string, entityType storage.ConstraintEntityType) (bool, error) {
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
		if existingEntity == "" {
			existingEntity = string(storage.ConstraintEntityNode)
		}
		existingLabel, _ := index["label"].(string)
		existingProperties, _ := index["properties"].([]string)
		if property, ok := index["property"].(string); ok {
			existingProperties = []string{property}
		}
		equivalent := existingKind == kind && existingLabel == label && existingEntity == string(entityType) && slices.Equal(existingProperties, properties)
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
