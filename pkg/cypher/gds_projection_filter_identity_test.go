package cypher

import (
    "testing"
    "github.com/orneryd/nornicdb/pkg/storage"
    "github.com/stretchr/testify/require"
)

func TestGDSProjectionFilterNativeIdentity(t *testing.T) {
    projection := &GraphProjection{Database: "comp", nodeIndex: map[string]int{"4:one:same": 0, "4:two:same": 1}}
    for _, database := range []string{"one", "two"} {
        id, ok := gdsNodeFilterID(projection, &storage.Node{ID: "same", Database: database})
        require.True(t, ok)
        require.Equal(t, "4:"+database+":same", id)
    }
    id, ok := gdsNodeFilterID(projection, "4:one:same")
    require.True(t, ok)
    require.Equal(t, "4:one:same", id)
}
