package tck

import (
	"context"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestBoltAutocommitAwaitIndexesAfterUnmanagedFulltextRead(t *testing.T) {
	driver, shutdown := startConformanceServer(t)
	defer shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	for _, query := range []string{
		"CREATE FULLTEXT INDEX yield_text FOR (n:SearchItem) ON EACH [n.text]",
		"CALL db.index.fulltext.queryNodes('yield_text', 'boiler') YIELD node RETURN node",
		"CREATE VECTOR INDEX yield_vector FOR (n:SearchItem) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
		"CALL db.awaitIndexes(5)",
		"CREATE (:SearchItem {text:'boiler', embedding:[1.0,0.0,0.0,0.0]})",
		"CALL db.awaitIndexes(5)",
	} {
		result, err := session.Run(ctx, query, nil)
		require.NoError(t, err, query)
		rows, err := result.Collect(ctx)
		require.NoError(t, err, query)
		if query == "CALL db.awaitIndexes(5)" {
			require.Empty(t, rows)
		}
	}
}
