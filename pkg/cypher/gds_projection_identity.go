package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// nodeIdentity keeps existing local projection IDs, qualifying only entities
// from another database. The same owner keys nodes, properties and endpoints.
func (p *GraphProjection) nodeIdentity(database string, id storage.NodeID) string {
	if database != "" && database != p.Database {
		return storage.SourceNodeElementID(database, id)
	}
	return string(id)
}
