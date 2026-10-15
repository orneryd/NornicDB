package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

func decodeRemoteHTTPValue(raw json.RawMessage, metadata []interface{}, index *int, entities map[string]interface{}) (interface{}, error) {
	return decodeRemoteHTTPValueWithREST(raw, nil, metadata, index, entities)
}

// REST values preserve the positions lost by Neo4j's flattened metadata for
// empty maps/lists. A graph marker requires the serializer's distinct envelope,
// not merely equality between an ordinary map and an entity's property map.
func decodeRemoteHTTPValueWithREST(raw, restRaw json.RawMessage, metadata []interface{}, index *int, entities map[string]interface{}) (interface{}, error) {
	var plain interface{}
	if err := json.Unmarshal(raw, &plain); err != nil {
		return nil, err
	}
	if len(restRaw) > 0 && !json.Valid(restRaw) {
		return nil, fmt.Errorf("invalid remote HTTP REST JSON")
	}
	if len(entities) == 0 {
		return plain, nil
	}
	if *index < len(metadata) {
		if pathMetadata, ok := metadata[*index].([]interface{}); ok && (len(restRaw) == 0 || remoteHTTPRESTPath(restRaw, plain)) {
			*index++
			nestedIndex := 0
			value, err := decodeRemoteHTTPValue(raw, pathMetadata, &nestedIndex, entities)
			if err != nil {
				return nil, err
			}
			items, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("remote path has no entity sequence")
			}
			path := make(RemotePathValue, len(items))
			for i, item := range items {
				entity, ok := item.(RemoteEntityValue)
				if !ok {
					return nil, fmt.Errorf("remote path contains a non-entity")
				}
				path[i] = entity
			}
			if _, _, valid := path.Parts(); !valid {
				return nil, fmt.Errorf("remote HTTP path has inconsistent entities")
			}
			return path, nil
		}
		if entityMetadata, ok := metadata[*index].(map[string]interface{}); ok {
			if entity, found := entities[fmt.Sprint(entityMetadata["elementId"])].(map[string]interface{}); found && reflect.DeepEqual(plain, entity["properties"]) && (len(restRaw) == 0 || remoteHTTPRESTEntity(restRaw, plain, entityMetadata)) {
				*index++
				return RemoteEntityValue(entity), nil
			}
		}
	}
	switch plain.(type) {
	case map[string]interface{}:
		decoder := json.NewDecoder(bytes.NewReader(raw))
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		var restFields map[string]json.RawMessage
		if len(restRaw) > 0 {
			if err := json.Unmarshal(restRaw, &restFields); err != nil {
				return nil, err
			}
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
			nestedREST, present := restFields[key.(string)]
			if len(restRaw) > 0 && !present {
				return nil, fmt.Errorf("remote HTTP row/REST map keys differ")
			}
			value, err := decodeRemoteHTTPValueWithREST(nested, nestedREST, metadata, index, entities)
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
		var restItems []json.RawMessage
		if len(restRaw) > 0 {
			if err := json.Unmarshal(restRaw, &restItems); err != nil {
				return nil, err
			}
			if len(restItems) != len(nested) {
				return nil, fmt.Errorf("remote HTTP row/REST list lengths differ")
			}
		}
		result := make([]interface{}, len(nested))
		for position, item := range nested {
			var itemREST json.RawMessage
			if len(restRaw) > 0 {
				itemREST = restItems[position]
			}
			value, err := decodeRemoteHTTPValueWithREST(item, itemREST, metadata, index, entities)
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

// Decode only identity and data: REST navigation URLs can dwarf an empty
// entity, so retaining their decoded strings would add avoidable heap cost.
func remoteHTTPRESTEntity(raw json.RawMessage, plain interface{}, metadata map[string]interface{}) bool {
	identity, valid := metadata["elementId"].(string)
	if !valid || identity == "" {
		return false
	}
	var envelope struct {
		Metadata struct {
			ElementID string `json:"elementId"`
		} `json:"metadata"`
		Self string          `json:"self"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.Metadata.ElementID != identity || envelope.Self == "" || len(envelope.Data) == 0 {
		return false
	}
	var properties interface{}
	if json.Unmarshal(envelope.Data, &properties) != nil {
		return false
	}
	// An ordinary REST map equals its row representation and cannot also equal
	// its own finite nested data field. No map-key shape grants provenance.
	return reflect.DeepEqual(plain, properties)
}

func remoteHTTPRESTPath(raw json.RawMessage, plain interface{}) bool {
	if _, list := plain.([]interface{}); !list {
		return false
	}
	var path struct {
		Nodes         []string `json:"nodes"`
		Relationships []string `json:"relationships"`
		Length        *int     `json:"length"`
	}
	if json.Unmarshal(raw, &path) != nil || path.Nodes == nil || path.Relationships == nil || path.Length == nil || *path.Length < 0 {
		return false
	}
	items := plain.([]interface{})
	return len(path.Relationships) == *path.Length && len(path.Nodes) == *path.Length+1 && len(items) == 2*(*path.Length)+1
}

func remoteHTTPNumericID(value interface{}) int64 {
	id, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	if err != nil {
		return -1
	}
	return id
}

func remoteHTTPEndpointID(edge map[string]interface{}, key string, entities map[string]interface{}) int64 {
	if id := remoteHTTPNumericID(edge[key]); id != -1 {
		return id
	}
	if node, ok := entities[fmt.Sprint(edge[key+"ElementId"])].(map[string]interface{}); ok {
		if identity, ok := node[remoteIdentityKey].(remoteNumericIdentity); ok {
			return identity.id
		}
	}
	return -1
}
