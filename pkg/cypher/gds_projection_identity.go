package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// nodeIdentity keeps existing local projection IDs, qualifying only entities
// from another database. The same owner keys nodes, properties and endpoints.
func (p *GraphProjection) nodeIdentity(database string, id storage.NodeID) string {
	return (storage.EntityIdentity{Database: database, LocalID: string(id)}).InDatabase(p.Database)
}

func (p *GraphProjection) nodeReference(value interface{}) (string,bool) {
	identity,ok := graphEntityReference(value)
	if !ok { return "",false }
	// Text IDs already use the projection's public domain. Native values carry
	// their source explicitly and must pass through the same owner as producers.
	if _,native := value.(*storage.Node); !native {
		if _,present := p.nodeIndex[identity.LocalID]; present { return identity.LocalID,true }
		if kind,database,local,qualified := storage.ParseElementID(identity.LocalID); qualified {
			if kind != "4" { return "",false }
			identity = storage.EntityIdentity{Database: database,LocalID: local}
			return identity.InDatabase(p.Database),true
		}
		return identity.LocalID,true
	}
	return identity.InDatabase(p.Database),true
}
