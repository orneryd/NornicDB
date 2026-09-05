package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryPermissionRequirementsRecognizesCypherKeywords(t *testing.T) {
	for name, testCase := range map[string]struct {
		query  string
		write  bool
		schema bool
		admin  bool
	}{
		"read":                  {query: "MATCH (n) RETURN n"},
		"set after match":       {query: "MATCH (n) SET n.value = 1", write: true},
		"set before newline":    {query: "MATCH (n) SET\nn.value = 1", write: true},
		"remove before tab":     {query: "MATCH (n) REMOVE\tn.value", write: true},
		"create after unwind":   {query: "UNWIND [1] AS value CREATE ({value: value})", write: true},
		"schema":                {query: "CREATE INDEX example FOR (n:Example) ON (n.value)", write: true, schema: true},
		"write procedure":       {query: "CALL db.create.setNodeVectorProperty('id', 'embedding', [1.0])", write: true},
		"keyword in string":     {query: "RETURN 'SET value' AS text"},
		"keyword in identifier": {query: "MATCH (n:`CREATE`) RETURN n"},
		"keyword in comment":    {query: "// DELETE n\nMATCH (n) RETURN n"},
	} {
		t.Run(name, func(t *testing.T) {
			requirements := QueryPermissionRequirements(testCase.query)
			require.True(t, requirements.Read)
			require.Equal(t, testCase.write, requirements.Write)
			require.Equal(t, testCase.schema, requirements.Schema)
			require.Equal(t, testCase.admin, requirements.Admin)
		})
	}
}
