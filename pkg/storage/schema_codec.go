package storage

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// EncodeSchemaDefinition writes the current persisted schema format. All schema
// writers, including transaction snapshots and exported sidecars, use this owner.
// A decoded legacy definition is rewritten from its FLOAT values as version 2.
func EncodeSchemaDefinition(def *SchemaDefinition) ([]byte, error) {
	if def == nil {
		return nil, fmt.Errorf("schema: definition is required")
	}
	current := *def
	current.Version = schemaDefinitionVersion
	return convert.PropertyJSON.Marshal(&current)
}

// DecodeSchemaDefinition reads a complete persisted schema document. Versions
// 0/1 retain the former float64 meaning; version 2 preserves exact number kinds.
// Decoding bytes from EncodeSchemaDefinition also provides an isolated deep copy
// for transaction publication without introducing another schema copy format.
func DecodeSchemaDefinition(data []byte, def *SchemaDefinition) error {
	var envelope struct {
		Version int `json:"version"`
	}
	if err := convert.PropertyJSON.Unmarshal(data, &envelope); err != nil {
		return err
	}
	switch envelope.Version {
	case 0, 1:
		// The old writer emitted 1152921504606847000 for float64(1<<60).
		// Only the former float64 reader restores its original value.
		return convert.PropertyJSON.UnmarshalLegacyState(data, def)
	case schemaDefinitionVersion:
		return convert.PropertyJSON.Unmarshal(data, def)
	default:
		return fmt.Errorf("schema: unsupported version %d", envelope.Version)
	}
}
