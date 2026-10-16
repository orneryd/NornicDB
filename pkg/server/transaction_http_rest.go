package server

import (
	"fmt"
	"net/url"
)

func transactionHTTPWantsREST(states []*transactionHTTPValueState) bool {
	return len(states) > 0 && states[0].rest
}

// REST's entity envelope preserves position independently of flattened meta.
// Its legacy self URI identifies the entity; it does not add legacy CRUD routes.
func transactionHTTPRESTEntity(kind, database string, identity, properties, fields map[string]interface{}) map[string]interface{} {
	metadata := make(map[string]interface{}, len(identity)+1)
	for key, value := range identity {
		if key != "type" {
			metadata[key] = value
		}
	}
	if kind == "node" {
		metadata["labels"] = fields["labels"]
	} else {
		metadata["type"] = fields["type"]
	}
	self := "/db/" + url.PathEscape(database) + "/data/" + kind + "/" + fmt.Sprint(identity["id"])
	value := map[string]interface{}{"self": self, "metadata": metadata, "data": properties}
	if kind == "relationship" {
		value["type"] = fields["type"]
		value["start"] = fields["startNodeElementId"]
		value["end"] = fields["endNodeElementId"]
	}
	return value
}

func (s *Server) transactionHTTPRESTPath(entities []interface{}, database string, state *transactionHTTPValueState) interface{} {
	nodes, relationships := make([]string, 0, (len(entities)+1)/2), make([]string, 0, len(entities)/2)
	for index, entity := range entities {
		value, _ := s.transactionHTTPValue(entity, database, state)
		envelope, ok := value.(map[string]interface{})
		if !ok {
			return nil
		}
		self, _ := envelope["self"].(string)
		if index%2 == 0 {
			nodes = append(nodes, self)
		} else {
			relationships = append(relationships, self)
		}
	}
	result := map[string]interface{}{"nodes": nodes, "relationships": relationships, "length": len(relationships)}
	if len(nodes) > 0 {
		result["start"] = nodes[0]
		result["end"] = nodes[len(nodes)-1]
	}
	return result
}
