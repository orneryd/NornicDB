package bolt

import "github.com/orneryd/nornicdb/pkg/cypher"

func (stream *resultStream) resolvedQueryType() string {
	if stream.result != nil {
		if mode, ok := cypher.ResolvedStatementQueryType(stream.result.Metadata); ok {
			return mode
		}
	}
	var stats *cypher.QueryStats
	if stream.result != nil {
		stats = stream.result.Stats
	}
	return boltSummaryQueryType(stream.isWrite, stats)
}

func (stream *resultStream) resolvedWrite() bool {
	switch stream.resolvedQueryType() {
	case "w", "rw", "s":
		return true
	}
	return false
}
