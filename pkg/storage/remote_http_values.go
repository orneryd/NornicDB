package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

func decodeRemoteHTTPValue(raw json.RawMessage, metadata []interface{}, index *int, entities map[string]interface{}) (interface{}, error) {
	var plain interface{}
	if err := json.Unmarshal(raw, &plain); err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return plain, nil
	}
	if *index < len(metadata) {
		if pathMetadata, ok := metadata[*index].([]interface{}); ok {
			*index++
			nestedIndex := 0
			return decodeRemoteHTTPValue(raw, pathMetadata, &nestedIndex, entities)
		}
		if entityMetadata, ok := metadata[*index].(map[string]interface{}); ok {
			if entity, found := entities[fmt.Sprint(entityMetadata["elementId"])].(map[string]interface{}); found && reflect.DeepEqual(plain, entity["properties"]) {
				*index++
				return entity, nil
			}
		}
	}
	switch plain.(type) {
	case map[string]interface{}:
		decoder := json.NewDecoder(bytes.NewReader(raw))
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			var nested json.RawMessage
			if err := decoder.Decode(&nested); err != nil {
				return nil, err
			}
			value, err := decodeRemoteHTTPValue(nested, metadata, index, entities)
			if err != nil {
				return nil, err
			}
			result[key.(string)] = value
		}
		return result, nil
	case []interface{}:
		var nested []json.RawMessage
		if err := json.Unmarshal(raw, &nested); err != nil {
			return nil, err
		}
		result := make([]interface{}, len(nested))
		for position, item := range nested {
			value, err := decodeRemoteHTTPValue(item, metadata, index, entities)
			if err != nil {
				return nil, err
			}
			result[position] = value
		}
		return result, nil
	default:
		if *index < len(metadata) {
			*index++
		}
		return plain, nil
	}
}
