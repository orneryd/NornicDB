package storage

import (
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validatePropertiesForStorage ensures property values are gob-encodable.
// This prevents runtime serialization failures during async flush.
func validatePropertiesForStorage(properties map[string]interface{}) error {
	if len(properties) == 0 {
		return nil
	}
	for key, value := range properties {
		if err := validatePropertyValueForStorage(value); err != nil {
			return localizedError(localization.StoragePropertyInvalidValue(key, err), err)
		}
	}
	return nil
}

func validatePropertyValueForStorage(value interface{}) error {
	switch typedValue := value.(type) {
	case nil,
		string,
		bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32,
		float64,
		time.Time:
		return nil
	case TypedPropertyValue:
		return nil
	case []interface{}:
		for i, item := range typedValue {
			if err := validatePropertyValueForStorage(item); err != nil {
				return localizedError(localization.StoragePropertyInvalidIndex(i, err), err)
			}
		}
		return nil
	case []byte, []string, []int, []int32, []int64, []uint64, []float32, []float64, []bool:
		return nil
	case map[string]interface{}:
		for key, item := range typedValue {
			if err := validatePropertyValueForStorage(item); err != nil {
				return localizedError(localization.StoragePropertyInvalidMapKey(key, err), err)
			}
		}
		return nil
	default:
		return localizedError(localization.StoragePropertyTypeUnsupported(fmt.Sprintf("%T", value)), nil)
	}
}

// TypedPropertyValue marks a typed Cypher scalar (temporal values, points)
// that supplies its own durable serialization while remaining opaque to the
// storage layer. PropertyValueKind names the type for property type
// constraints: date, local-time, time, local-date-time, zoned-date-time,
// duration, point.
type TypedPropertyValue interface {
	PropertyValueKind() string
}
