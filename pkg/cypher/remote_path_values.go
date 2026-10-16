package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"strings"
)

func nilRemotePath(value interface{}) bool {
	path, ok := value.(storage.RemotePathValue)
	return ok && path == nil
}

// Row and fallback function readers share the transport's validated path parts,
// retaining marked remote entities rather than inventing local node identities.
func remotePathFunctionValue(function string, path storage.RemotePathValue) (interface{}, error) {
	if path == nil {
		return nil, nil
	}
	nodes, relationships, valid := path.Parts()
	if !valid {
		return nil, invalidFunctionArgument(function, path)
	}
	if strings.EqualFold(function, "length") {
		return int64(len(relationships)), nil
	}
	parts := nodes
	if strings.EqualFold(function, "relationships") {
		parts = relationships
	}
	values := make([]interface{}, len(parts))
	for i, value := range parts {
		values[i] = value
	}
	return values, nil
}
