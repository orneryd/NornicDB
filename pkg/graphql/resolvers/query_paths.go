package resolvers

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// queryPaths shares bounded traversal, relationship uniqueness, and cancellation
// with Cypher instead of maintaining a second GraphQL path enumerator.
func (r *queryResolver) queryPaths(ctx context.Context, startID, endID string, depth, limit int, relationshipTypes []string, shortest bool) ([][]models.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth <= 0 || limit <= 0 {
		return [][]models.Node{}, nil
	}
	types := make([]string, 0, len(relationshipTypes))
	for _, typ := range relationshipTypes {
		if typ != "" {
			types = append(types, "`"+strings.ReplaceAll(typ, "`", "``")+"`")
		}
	}
	if len(relationshipTypes) > 0 && len(types) == 0 {
		return [][]models.Node{}, nil
	}
	typePattern := ""
	if len(types) > 0 {
		typePattern = ":" + strings.Join(types, "|")
	}
	pattern := fmt.Sprintf("(a)-[%s*0..%d]-(b)", typePattern, depth)
	if shortest {
		pattern = "shortestPath(" + pattern + ")"
	}
	query := "MATCH (a) WHERE (id(a) = $start OR a.id = $start) MATCH (b) WHERE (id(b) = $end OR b.id = $end) MATCH p = " + pattern + " RETURN nodes(p) AS nodes LIMIT $limit"
	result, err := r.executeCypher(ctx, query, map[string]interface{}{"start": startID, "end": endID, "limit": limit}, "")
	if err != nil {
		return nil, err
	}
	paths := make([][]models.Node, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		var values []interface{}
		switch nodes := row[0].(type) {
		case []interface{}:
			values = nodes
		case []*storage.Node:
			values = make([]interface{}, len(nodes))
			for i, node := range nodes {
				values[i] = node
			}
		default:
			return nil, fmt.Errorf("unexpected Cypher path node list: %T", row[0])
		}
		path := make([]models.Node, 0, len(values))
		for _, value := range values {
			node, err := extractNodeFromResult(value)
			if err != nil {
				return nil, err
			}
			path = append(path, *dbNodeToModel(node))
		}
		paths = append(paths, path)
	}
	return paths, nil
}
