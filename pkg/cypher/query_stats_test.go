package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddQueryStats(t *testing.T) {
	total := &QueryStats{
		NodesCreated:         1,
		NodesDeleted:         2,
		RelationshipsCreated: 3,
		RelationshipsDeleted: 4,
		PropertiesSet:        5,
		LabelsAdded:          6,
	}
	addQueryStats(total, &QueryStats{
		NodesCreated:         6,
		NodesDeleted:         5,
		RelationshipsCreated: 4,
		RelationshipsDeleted: 3,
		PropertiesSet:        2,
		LabelsAdded:          1,
	})

	require.Equal(t, &QueryStats{
		NodesCreated:         7,
		NodesDeleted:         7,
		RelationshipsCreated: 7,
		RelationshipsDeleted: 7,
		PropertiesSet:        7,
		LabelsAdded:          7,
	}, total)

	addQueryStats(nil, total)
	addQueryStats(total, nil)
}
