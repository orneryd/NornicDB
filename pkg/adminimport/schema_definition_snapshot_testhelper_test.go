package adminimport

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// schemaDefinitionForTest returns the accepted definition snapshot under a
// test-owned lifetime; a refused snapshot fails the test.
func schemaDefinitionForTest(t testing.TB, schema *storage.SchemaManager) *storage.SchemaDefinition {
	t.Helper()
	definition, err := schema.ExportDefinition(context.Background())
	require.NoError(t, err)
	return definition
}

// constraintContractsForTest returns the detached constraint-contract snapshot
// under a test-owned lifetime.
func constraintContractsForTest(t testing.TB, schema *storage.SchemaManager) []storage.ConstraintContract {
	t.Helper()
	contracts, err := schema.GetAllConstraintContracts(context.Background())
	require.NoError(t, err)
	return contracts
}
