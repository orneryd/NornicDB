package tck

import (
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestBoltStatementTypeErrorsAreRuntimeFailures(t *testing.T) {
	err := classifyBoltError(&neo4j.Neo4jError{
		Code:      "Neo.ClientError.Statement.TypeError",
		GqlStatus: "InvalidPropertyType",
	})

	var queryError *QueryError
	require.True(t, errors.As(err, &queryError))
	require.Equal(t, "TypeError", queryError.Type)
	require.Equal(t, "runtime", queryError.Phase)
	require.Equal(t, "InvalidPropertyType", queryError.Detail)
}

func TestBoltSyntaxErrorsAreCompileTimeFailures(t *testing.T) {
	err := classifyBoltError(&neo4j.Neo4jError{
		Code:      "Neo.ClientError.Statement.SyntaxError",
		GqlStatus: "UndefinedVariable",
	})

	var queryError *QueryError
	require.True(t, errors.As(err, &queryError))
	require.Equal(t, "SyntaxError", queryError.Type)
	require.Equal(t, "compile time", queryError.Phase)
	require.Equal(t, "UndefinedVariable", queryError.Detail)
}
