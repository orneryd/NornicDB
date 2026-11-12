package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/queryprofile"
)

// Graph token reads. db.labels, db.relationshipTypes, db.propertyKeys and
// nornicdb.stats ask the same question of the same populations - which distinct
// labels, relationship types or property keys does this graph hold - so they ask
// it through this one reader instead of each enumerating a population itself.

// graphTokenKind names the token set readGraphTokenKeys collects.
type graphTokenKind uint8

// graphTokenLabels is the distinct label of every node.
const graphTokenLabels graphTokenKind = 0

// graphTokenRelationshipTypes is the distinct type of every relationship.
const graphTokenRelationshipTypes graphTokenKind = 1

// graphTokenPropertyKeys is the distinct property key of every node and every
// relationship.
const graphTokenPropertyKeys graphTokenKind = 2

// readGraphTokenKeys adds the distinct tokens of kind to keys. The caller
// supplies the map that receives them, so one listing keeps one map: the read
// neither builds a second one nor returns one that escapes.
//
// The read owns the population acquisitions it performs - each AllNodes/AllEdges
// request and the population PROFILE reports for it - so every caller reports
// the same reads for the same tokens. A population that cannot be read fails the
// caller's statement: the tokens of the population that did return are not a
// partial listing.
func (e *StorageExecutor) readGraphTokenKeys(ctx context.Context, kind graphTokenKind, keys map[string]bool) error {
	switch kind {
	case graphTokenLabels, graphTokenRelationshipTypes, graphTokenPropertyKeys:
	default:
		panic("invalid graph token kind")
	}
	if kind != graphTokenRelationshipTypes {
		nodes, err := e.storage.AllNodes(ctx)
		if err != nil {
			return err
		}
		queryprofile.EntityPopulation(ctx, profileKindAllNodesScan, "", nodes)
		for _, node := range nodes {
			if kind == graphTokenLabels {
				for _, label := range node.Labels {
					keys[label] = true
				}
			} else {
				for property := range node.Properties {
					keys[property] = true
				}
			}
		}
	}
	if kind != graphTokenLabels {
		edges, err := e.storage.AllEdges(ctx)
		if err != nil {
			return err
		}
		queryprofile.EntityPopulation(ctx, profileKindAllRelationshipsScan, "", edges)
		for _, edge := range edges {
			if kind == graphTokenRelationshipTypes {
				keys[edge.Type] = true
			} else {
				for property := range edge.Properties {
					keys[property] = true
				}
			}
		}
	}
	return nil
}
