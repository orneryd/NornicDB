package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRowExpressionProjectsLabelPredicate(t *testing.T) {
	executor := &StorageExecutor{}
	value, ok := rowValue(t, executor, "(node:Expected)", pipelineRow{
		"node": &storage.Node{Labels: []string{"Expected"}},
	})
	require.True(t, ok)
	require.Equal(t, true, value)
}
