package storage

import (
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"reflect"
	"strconv"
)

// REST values preserve the positions lost by Neo4j's flattened metadata for
// empty maps/lists. A graph marker requires the serializer's distinct envelope,
// not merely equality between an ordinary map and an entity's property map.
// The metadata is one flat list per row (a list or map adds its elements'
// entries, a plain value null); it is walked when the row has graph entities
// or typed temporal or point values (RemoteTypedValue).

// remoteHTTPMetaHasTypes reports whether a row's metadata types a temporal or
// point value.
func remoteHTTPMetaHasTypes(metadata []interface{}) bool {
	for _, entry := range metadata {
		if typed, ok := entry.(map[string]interface{}); ok {
			if kind, _ := typed["type"].(string); kind != "" && kind != "node" && kind != "relationship" {
				return true
			}
		}
	}
	return false
}

type remoteHTTPTraversal uint8

const (
	remoteHTTPAuto remoteHTTPTraversal = iota
	remoteHTTPPlain
	remoteHTTPGraph
)

func decodeRemoteHTTPValue(raw, restRaw []byte, metadata []interface{}, index *int, entities map[string]interface{}, traversal remoteHTTPTraversal) (interface{}, error) {
	if traversal == remoteHTTPAuto {
		traversal = remoteHTTPPlain
		if len(entities) > 0 || remoteHTTPMetaHasTypes(metadata) {
			traversal = remoteHTTPGraph
		}
	}
	var plain interface{}
	if err := unmarshalRemoteHTTPJSON(raw, &plain); err != nil {
		return nil, err
	}
	plain = remoteHTTPNumbers(plain)
	if len(restRaw) > 0 && !json.Valid(restRaw) {
		return nil, localizedError(localization.StorageRemoteRESTInvalidJSON(), nil)
	}
	if traversal == remoteHTTPPlain {
		return plain, nil
	}
	if *index < len(metadata) {
		if pathMetadata, ok := metadata[*index].([]interface{}); ok && (len(restRaw) == 0 || remoteHTTPRESTPath(restRaw, plain)) {
			*index++
			nestedIndex := 0
			value, err := decodeRemoteHTTPValue(raw, nil, pathMetadata, &nestedIndex, entities, remoteHTTPAuto)
			if err != nil {
				return nil, err
			}
			items, ok := value.([]interface{})
			if !ok {
				return nil, localizedError(localization.StorageRemotePathEntitySequenceMissing(), nil)
			}
			path := make(RemotePathValue, len(items))
			for i, item := range items {
				entity, ok := item.(RemoteEntityValue)
				if !ok {
					return nil, localizedError(localization.StorageRemotePathNonEntity(), nil)
				}
				path[i] = entity
			}
			if _, _, valid := path.Parts(); !valid {
				return nil, localizedError(localization.StorageRemotePathEntitiesInconsistent(), nil)
			}
			return path, nil
		}
		if entityMetadata, ok := metadata[*index].(map[string]interface{}); ok {
			if entity, found := entities[fmt.Sprint(entityMetadata["elementId"])].(map[string]interface{}); found && reflect.DeepEqual(plain, entity["properties"]) && (len(restRaw) == 0 || remoteHTTPRESTEntity(restRaw, plain, entityMetadata)) {
				*index++
				return RemoteEntityValue(entity), nil
			}
			if kind, _ := entityMetadata["type"].(string); kind != "node" && kind != "relationship" {
				if typed, ok := remoteTypedFromHTTP(kind, plain); ok {
					*index++
					return typed, nil
				}
			}
		}
	}
	switch plain.(type) {
	case map[string]interface{}:
		decoder := jsontext.NewTokenReader(raw)
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		var restFields map[string]jsontext.RawMessage
		if len(restRaw) > 0 {
			if err := jsontext.Unmarshal(restRaw, &restFields); err != nil {
				return nil, err
			}
		}
		result := make(map[string]interface{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			var nested jsontext.RawMessage
			if err := decoder.Decode(&nested); err != nil {
				return nil, err
			}
			nestedREST, present := restFields[key.(string)]
			if len(restRaw) > 0 && !present {
				return nil, localizedError(localization.StorageRemoteRESTMapKeysMismatch(), nil)
			}
			value, err := decodeRemoteHTTPValue(nested, nestedREST, metadata, index, entities, traversal)
			if err != nil {
				return nil, err
			}
			result[key.(string)] = value
		}
		return result, nil
	case []interface{}:
		var nested []jsontext.RawMessage
		if err := jsontext.Unmarshal(raw, &nested); err != nil {
			return nil, err
		}
		var restItems []jsontext.RawMessage
		if len(restRaw) > 0 {
			if err := jsontext.Unmarshal(restRaw, &restItems); err != nil {
				return nil, err
			}
			if len(restItems) != len(nested) {
				return nil, localizedError(localization.StorageRemoteRESTListLengthMismatch(), nil)
			}
		}
		result := make([]interface{}, len(nested))
		for position, item := range nested {
			var itemREST jsontext.RawMessage
			if len(restRaw) > 0 {
				itemREST = restItems[position]
			}
			value, err := decodeRemoteHTTPValue(item, itemREST, metadata, index, entities, traversal)
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
func remoteHTTPRESTEntity(raw []byte, plain interface{}, metadata map[string]interface{}) bool {
	identity, valid := metadata["elementId"].(string)
	if !valid || identity == "" {
		return false
	}
	var envelope struct {
		Metadata struct {
			ElementID string `json:"elementId"`
		} `json:"metadata"`
		Self string              `json:"self"`
		Data jsontext.RawMessage `json:"data"`
	}
	if jsontext.Unmarshal(raw, &envelope) != nil || envelope.Metadata.ElementID != identity || envelope.Self == "" || len(envelope.Data) == 0 {
		return false
	}
	var properties interface{}
	if unmarshalRemoteHTTPJSON(envelope.Data, &properties) != nil {
		return false
	}
	properties = remoteHTTPNumbers(properties)
	// An ordinary REST map equals its row representation and cannot also equal
	// its own finite nested data field. No map-key shape grants provenance.
	return reflect.DeepEqual(plain, properties)
}

func remoteHTTPRESTPath(raw []byte, plain interface{}) bool {
	if _, list := plain.([]interface{}); !list {
		return false
	}
	var path struct {
		Nodes         []string `json:"nodes"`
		Relationships []string `json:"relationships"`
		Length        *int     `json:"length"`
	}
	if jsontext.Unmarshal(raw, &path) != nil || path.Nodes == nil || path.Relationships == nil || path.Length == nil || *path.Length < 0 {
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
