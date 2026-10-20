package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// Reading a planned clause's properties must not parse that clause again as a
// separate statement. This fails on the previous physical-read adapter.
func TestPhysicalReadsRetainClauseDescriptor(t *testing.T) {
	query := "MATCH (admission101_person:Person) RETURN admission101_person.name LIMIT 10"
	clauses, ok := pipelineClausesFor(query)
	require.True(t, ok)
	tail := pipelineReadOnlyTail(clauses[1:])
	properties, complete := pipelineLabelScanProjection(context.Background(), nodePatternInfo{variable: "admission101_person", labels: []string{"Person"}}, "", tail)
	require.True(t, complete)
	require.Equal(t, []string{"name"}, properties)
	pipelineClauseSplits.RLock()
	_, reparsed := pipelineClauseSplits.splits["RETURN admission101_person.name LIMIT 10"]
	pipelineClauseSplits.RUnlock()
	require.False(t, reparsed, "a retained clause was reparsed as a standalone statement")
}
