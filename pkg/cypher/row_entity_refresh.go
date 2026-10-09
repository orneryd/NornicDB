package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// refreshRowEntities makes every row bind the current version of an entity
// that rows bind through different copies (each row of a MATCH or MERGE
// read its own). Before a SET, REMOVE or FOREACH it makes the rows write one
// shared entity, so a row reads what the rows before it wrote and doesn't
// store a stale copy over them; after a write clause it shows later clauses
// what every row wrote. In Neo4j an entity is one value: UNWIND [1, 2] AS k
// MERGE (m {k: 1}) ON MATCH SET m.m = k RETURN m.m answers 2 on both rows
// (#907). The current version is read once per entity from the statement's
// storage view; rows that share one copy, or bind distinct entities, cost
// one pass and no read. An entity the clause deleted keeps its copies.
func (e *StorageExecutor) refreshRowEntities(ctx context.Context, rows []pipelineRow) {
	if len(rows) < 2 {
		return
	}
	var nodeCopies map[storage.NodeID]*storage.Node
	var edgeCopies map[storage.EdgeID]*storage.Edge
	var staleNodes map[storage.NodeID]*storage.Node
	var staleEdges map[storage.EdgeID]*storage.Edge
	for _, row := range rows {
		for _, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				if entity == nil {
					continue
				}
				if nodeCopies == nil {
					nodeCopies = make(map[storage.NodeID]*storage.Node)
				}
				if first, seen := nodeCopies[entity.ID]; !seen {
					nodeCopies[entity.ID] = entity
				} else if first != entity {
					if staleNodes == nil {
						staleNodes = make(map[storage.NodeID]*storage.Node)
					}
					staleNodes[entity.ID] = nil
				}
			case *storage.Edge:
				if entity == nil {
					continue
				}
				if edgeCopies == nil {
					edgeCopies = make(map[storage.EdgeID]*storage.Edge)
				}
				if first, seen := edgeCopies[entity.ID]; !seen {
					edgeCopies[entity.ID] = entity
				} else if first != entity {
					if staleEdges == nil {
						staleEdges = make(map[storage.EdgeID]*storage.Edge)
					}
					staleEdges[entity.ID] = nil
				}
			}
		}
	}
	if len(staleNodes) == 0 && len(staleEdges) == 0 {
		return
	}
	store := e.getStorage(ctx)
	for id := range staleNodes {
		if current, err := store.GetNode(id); err == nil && current != nil {
			staleNodes[id] = current
		} else {
			delete(staleNodes, id)
		}
	}
	for id := range staleEdges {
		if current, err := store.GetEdge(id); err == nil && current != nil {
			staleEdges[id] = current
		} else {
			delete(staleEdges, id)
		}
	}
	for _, row := range rows {
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				if entity != nil {
					if current := staleNodes[entity.ID]; current != nil {
						row[name] = current
					}
				}
			case *storage.Edge:
				if entity != nil {
					if current := staleEdges[entity.ID]; current != nil {
						row[name] = current
					}
				}
			}
		}
	}
}
