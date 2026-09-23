package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractLabelsFromQuery_ColonScannerEdgeCases(t *testing.T) {
	require.Empty(t, extractLabelsFromQuery("RETURN :"))
	require.Empty(t, extractLabelsFromQuery("MATCH (n:person) RETURN n"))
	require.Equal(t, []string{"TypeCast"}, extractLabelsFromQuery("MATCH (n::TypeCast) RETURN n"))
	require.Equal(t, []string{"Person"}, extractLabelsFromQuery("MATCH (n:Person) RETURN n"))
}

func TestResolveContextPathRef_EmptyAndNoParams(t *testing.T) {
	v, ok := resolveContextPathRef(context.Background(), "")
	require.False(t, ok)
	require.Nil(t, v)

	v, ok = resolveContextPathRef(context.Background(), "row.id")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestExtractCreateVariableRefs_IgnoresEmptyPatternSegments(t *testing.T) {
	vars := extractCreateVariableRefs("CREATE (a)-[:R]->(b), ")
	require.ElementsMatch(t, []string{"a", "b"}, vars)
}

func TestDatetimeFunction_PassthroughTypedDateTime(t *testing.T) {
	exec := &StorageExecutor{}
	out := exec.evaluateExpressionWithContext(context.Background(), "datetime(datetime())", nil, nil)
	_, ok := out.(CypherDateTime)
	require.True(t, ok)
}

func TestParseIndexHints_InvalidFormsRemainInQuery(t *testing.T) {
	tests := []string{
		"MATCH (n) USING INDEX n:Person() RETURN n",
		"MATCH (n) USING JOIN a RETURN n",
		"MATCH (n) USING SCAN n RETURN n",
	}

	for _, q := range tests {
		hints, clean := ParseIndexHints(q)
		require.Empty(t, hints)
		require.Contains(t, clean, "USING")
	}
}
