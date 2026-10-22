package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreatePropertyMapOmitsNullValues(t *testing.T) {
	exec := &StorageExecutor{}

	fixtureAdmission1551Context, fixtureAdmission1551 := expressionTestContext(context.Background(), "{id: 12, name: null}")
	_ = fixtureAdmission1551Context
	properties := exec.parseProperties(fixtureAdmission1551, fixtureAdmission1551Context, "{id: 12, name: null}")

	require.Equal(t, map[string]interface{}{"id": int64(12)}, properties)
}
