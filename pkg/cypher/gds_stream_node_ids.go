package cypher

import "context"

// gdsStreamNodeIDColumns are the columns of every GDS algorithm stream that
// hold projected node ids; "shared" is node similarity's list of {nodeId,
// weight}. Each stream's rows carry the projection's stored ids, and
// gdsStreamHandler, applied to all of them where procedures are
// registered, is the one place they become the ids GDS returns: the INTEGER
// an aggregation row supplied, else the stored string id (#1165). Every
// registered gds.*.stream must be listed here (TestGDSStreamsRenderNodeIDs).
var gdsStreamNodeIDColumns = map[string][]string{
	"gds.fastRP.stream":                                {"nodeId"},
	"gds.louvain.stream":                               {"nodeId"},
	"gds.nodeSimilarity.stream":                        {"node1", "node2", "shared"},
	"gds.nodeSimilarity.filtered.stream":               {"node1", "node2", "shared"},
	"gds.linkPrediction.adamicAdar.stream":             {"node1", "node2"},
	"gds.linkPrediction.commonNeighbors.stream":        {"node1", "node2"},
	"gds.linkPrediction.resourceAllocation.stream":     {"node1", "node2"},
	"gds.linkPrediction.preferentialAttachment.stream": {"node1", "node2"},
	"gds.linkPrediction.jaccard.stream":                {"node1", "node2"},
	"gds.linkPrediction.predict.stream":                {"node1", "node2"},
}

// gdsStreamHandler wraps a GDS stream's handler so its node ids leave
// through the projection's renderer; other procedures are unchanged.
func gdsStreamHandler(procedure string, handler admittedProcedureHandler) admittedProcedureHandler {
	columns, ok := gdsStreamNodeIDColumns[procedure]
	if !ok {
		return handler
	}
	return func(statement admittedRelationshipStatement, ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		result, err := handler(statement, ctx, e, cypher, args)
		if err != nil || result == nil {
			return result, err
		}
		// The projection the stream computed from, not a catalog lookup now:
		// the name may have been dropped or replaced meanwhile. Nil when the
		// stream read the stored graph.
		if projection := result.gdsProjection; projection != nil {
			result.gdsProjection = nil
			projection.renderNodeIDs(result, columns)
		}
		return result, nil
	}
}

// renderNodeIDs renders the result's node-id columns, and the nodeId of the
// maps a list column holds, through nodeIDValue.
func (p *GraphProjection) renderNodeIDs(result *ExecuteResult, columns []string) {
	var indexes []int
	for i, column := range result.Columns {
		for _, name := range columns {
			if column == name {
				indexes = append(indexes, i)
			}
		}
	}
	render := func(value interface{}) interface{} {
		if id, ok := value.(string); ok {
			return p.nodeIDValue(id)
		}
		return value
	}
	for _, row := range result.Rows {
		for _, i := range indexes {
			if i >= len(row) {
				continue
			}
			if list, ok := row[i].([]interface{}); ok {
				for _, item := range list {
					if entry, ok := item.(map[string]interface{}); ok {
						entry["nodeId"] = render(entry["nodeId"])
					}
				}
				continue
			}
			row[i] = render(row[i])
		}
	}
}
