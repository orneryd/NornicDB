package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreatePropertyMapOmitsNullValues(t *testing.T) {
	exec := &StorageExecutor{}

	fixtureAdmission1565Context, fixtureAdmission1565 := expressionTestContext(context.Background(), "{id: 12, name: null}")
	_ = fixtureAdmission1565Context
	properties := parsedTestValue(exec.parseProperties(fixtureAdmission1565, fixtureAdmission1565Context, "{id: 12, name: null}"))

	require.Equal(t, map[string]interface{}{"id": int64(12)}, properties)
}
