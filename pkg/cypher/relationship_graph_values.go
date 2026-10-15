package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// relationshipEndpointArgument is the common startNode/endNode argument
// contract. Remote endpoints belong to the remote database, so preserve the
// established null result rather than resolving their IDs in local storage.
func relationshipEndpointArgument(value interface{}, end bool) (*storage.Edge, error) {
	if value == nil {
		return nil, nil
	}
	if edge, ok := value.(*storage.Edge); ok {
		return edge, nil
	}
	if remote, ok := value.(storage.RemoteEntityValue); ok {
		if remote == nil {
			return nil, nil
		}
		if _, relationship := remote["type"]; relationship {
			return nil, nil
		}
	}
	function := "startNode"
	if end {
		function = "endNode"
	}
	return nil, invalidFunctionArgument(function, value)
}
