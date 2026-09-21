package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreatePropertyMapOmitsNullValues(t *testing.T) {
	exec := &StorageExecutor{}

	properties := exec.parseProperties(context.Background(), "{id: 12, name: null}")

	require.Equal(t, map[string]interface{}{"id": int64(12)}, properties)
}
