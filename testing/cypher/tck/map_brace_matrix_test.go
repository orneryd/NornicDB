package tck

import (
	"context"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

func TestMapBraceSyntaxTriviaMatrixBolt(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI for the pinned Neo4j differential")
	}
	ctx := context.Background()
	reference, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Close(ctx)
	for _, route := range []struct {
		name string
		mode TransactionMode
	}{{"autocommit", AutocommitMode}, {"explicit", ExplicitTransactionMode}} {
		t.Run(route.name, func(t *testing.T) {
			local, shutdown := startConformanceServer(t)
			defer shutdown()
			n := newBoltDifferentialExecutor(t, local, "nornic", route.mode)
			r := newBoltDifferentialExecutor(t, reference, "neo4j", route.mode)
			querytextmatrix.MapBraceCases(func(item querytextmatrix.MapBraceCase) {
				t.Run(item.Name, func(t *testing.T) {
					expected := r.Execute(ctx, item.Query)
					actual := n.Execute(ctx, item.Query)
					if actual.Code != expected.Code || (!item.Explain && !differential.Same(item.Query, expected, actual)) {
						t.Fatalf("query=%s reference=%+v local=%+v", item.Query, expected, actual)
					}
				})
			})
			for _, executor := range []boltDifferentialExecutor{n, r} {
				if result := executor.Execute(ctx, "MATCH (n:MapBraceMatrix) DETACH DELETE n"); result.Failed() {
					t.Fatalf("fixture cleanup: %+v", result)
				}
			}
		})
	}
}
