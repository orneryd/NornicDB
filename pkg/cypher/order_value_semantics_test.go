package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOrderValueTypePrecedencePlacesPathsBetweenListsAndStrings(t *testing.T) {
	executor := &StorageExecutor{}
	path := executor.pathToMap(PathResult{
		Nodes: []*storage.Node{{ID: "start"}, {ID: "end"}},
	})

	require.Less(t, compareValuesForSort([]interface{}{"list"}, path), 0)
	require.Less(t, compareValuesForSort(path, "text"), 0)
	require.Equal(t, compareValuesForSort(path, "text"), executor.compareOrderValues(path, "text"))
}
