package bolt

import (
	"context"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestBoltNestedCypherLiteralExpressionWriteRejection(t *testing.T) {
	// Literal update expressions already fail validation. Dynamic procedures
	// retain their shipped acceptance; summary aggregation changes neither.
	for _, kind := range []string{"COLLECT", "COUNT"} {
		t.Run(kind, func(t *testing.T) {
			session := newStatementFramingSession(t)
			ctx := context.Background()
			result, err := session.Run(ctx, "RETURN "+kind+" { CREATE (:NestedStats {p:1}) RETURN 1 AS value } AS nested", nil)
			if err == nil {
				_, err = result.Consume(ctx)
			}
			var dbErr *neo4jdriver.Neo4jError
			require.ErrorAs(t, err, &dbErr)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", dbErr.Code)
		})
	}
}
