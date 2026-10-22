package cypher

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// fabricRemoteValueCodec is the Cypher executor's view of the values remote
// participants exchange with it, for Bolt and HTTP alike: a value imported
// into a remote CALL body is checked by checkFabricImport and sent as a
// parameter, which keeps its type (storage.RemoteParameterValue), and a value
// a remote returns is read by decodeFabricRemoteValue. A value of every kind
// keeps its type both ways, at any nesting depth, whichever participant it
// came from.
var fabricRemoteValueCodec = fabric.RemoteValueCodec{Import: checkFabricImport, Decode: decodeFabricRemoteValue}

// decodeFabricRemoteValue is the Cypher value a remote participant's result
// value denotes. Both transports deliver temporal and point values as
// storage.RemoteTypedValue; lists and maps are read element by element (and
// copied only when one holds such a value), and graph entities keep their
// remote identity.
func decodeFabricRemoteValue(value interface{}) (interface{}, error) {
	decoded, _, err := decodeFabricRemote(value)
	return decoded, err
}

func decodeFabricRemote(value interface{}) (interface{}, bool, error) {
	switch v := value.(type) {
	case storage.RemoteTypedValue:
		decoded, err := decodeFabricTypedValue(v)
		return decoded, true, err
	case []interface{}:
		var out []interface{}
		for i, item := range v {
			decoded, changed, err := decodeFabricRemote(item)
			if err != nil {
				return nil, false, err
			}
			if !changed {
				continue
			}
			if out == nil {
				out = append([]interface{}(nil), v...)
			}
			out[i] = decoded
		}
		if out == nil {
			return value, false, nil
		}
		return out, true, nil
	case map[string]interface{}:
		var out map[string]interface{}
		for key, item := range v {
			decoded, changed, err := decodeFabricRemote(item)
			if err != nil {
				return nil, false, err
			}
			if !changed {
				continue
			}
			if out == nil {
				out = make(map[string]interface{}, len(v))
				for k, original := range v {
					out[k] = original
				}
			}
			out[key] = decoded
		}
		if out == nil {
			return value, false, nil
		}
		return out, true, nil
	}
	return value, false, nil
}

// decodeFabricTypedValue is the Cypher temporal or point value a remote
// typed value denotes, built by the same readers as date('…') or
// point({…}); text no reader accepts is an error, never a string.
func decodeFabricTypedValue(v storage.RemoteTypedValue) (interface{}, error) {
	if v.Type == "point" {
		fields := map[string]interface{}{"srid": int64(v.SRID)}
		for i, axis := range []string{"x", "y", "z"} {
			if i < len(v.Coordinates) {
				fields[axis] = v.Coordinates[i]
			}
		}
		point, ok, err := newPointFromMap(fields)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentValue",
				localization.CypherCoreTemporalMapInvalid("Point", fmt.Sprint(fields)))
		}
		return point, nil
	}
	value, ok, err := parseTemporalTextWithError(v.Type, v.Text)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, temporalConstructorError(v.Type, v.Text)
	}
	return value, nil
}

// checkFabricImport refuses a row value no remote participant may import: a
// node, relationship or path, at any depth (Neo4j's 22N16). Every other value
// is imported as a parameter, which the remote transport sends as its Cypher
// type (storage.RemoteParameterValue).
func checkFabricImport(value interface{}) error {
	switch kind := cypherValueKindOf(value); kind {
	case valueKindNode, valueKindRelationship, valueKindPath:
		entity := map[cypherValueKind]string{valueKindNode: "node", valueKindRelationship: "relationship", valueKindPath: "path"}[kind]
		return localizedStatusError("Neo.ClientError.Statement.TypeError", "22N16", localization.CypherCommandRoutingFabricImportEntity(entity))
	case valueKindList:
		for _, item := range toAnySlice(value) {
			if err := checkFabricImport(item); err != nil {
				return err
			}
		}
	case valueKindMap:
		if fields, ok := value.(map[string]interface{}); ok {
			for _, item := range fields {
				if err := checkFabricImport(item); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
