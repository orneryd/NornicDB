package cypher

import (
	"context"
	"testing"

	cantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestInvalidCypherCannotSilentlyWriteOrReturnText(t *testing.T) {
	queries := []string{
		"WITH 1 +* 2 AS x RETURN x",
		"MATCH (p:P) SET p.x = 1 +* 2 RETURN p.x AS x",
		"MATCH (p:P) SET p += {x: 1 +* 2} RETURN p.x AS x",
		"CREATE (n:Q {v: 1 +* 2}) RETURN n.v AS v",
		"MERGE (n:Q {v: 1 +* 2}) RETURN n.v AS v",
		"UNWIND [1, 2 +* 3] AS x RETURN x",
		"MATCH (p:P) WHERE p.id = 1 +* 2 RETURN count(p) AS c",
		"MATCH (p:P) RETURN p.id AS id ORDER BY 1 +* 2",
		"MATCH (p:P) SET p.x = p.name.. RETURN p.x AS x",
		"MATCH (p:P) DELET p",
		"MATCH (p:P) DETACH DELET p",
		"MATCH (p:P) SETT p.x = 1",
		"MATCH (p:P) REMOVEE p.name",
		"MATCH (p:P) RETRUN p",
		"MATCH (p:P) FROBNICATE p RETURN p.id AS id",
		"MATCH (p:P) WHERE p.id = 1 GARBAGE RETURN p.id AS id",
		"MATCH (p:P) RETURN p.id AS id FOOBAR BAZ",
		"CREATE (n:Q {v: 1}) GARBAGE",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(engine)
			_, err := exec.Execute(context.Background(), "CREATE (:P {id: 1, name: 'a'})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			nodes, err := engine.AllNodes()
			require.NoError(t, err)
			require.Len(t, nodes, 1, "query: %s", query)
		})
	}
}

func TestStrictParserRecognizesReportedSyntax(t *testing.T) {
	for _, query := range []string{
		"MATCH (p:P) DELET p",
		"CREATE (n:Q {v: 1}) GARBAGE",
		"WITH 1 +* 2 AS x RETURN x",
		"MATCH (p:P) WHERE p.id = 1 GARBAGE RETURN p.id",
	} {
		require.Error(t, cantlr.Validate(query), query)
	}
	for _, query := range []string{
		"MATCH (p:P) WHERE p.id = 1 RETURN p.id AS id ORDER BY id",
		"CREATE (n:Q {v: 1}) RETURN n.v",
		"UNWIND range(1, 3) AS i CREATE (n:Q {v: i}) RETURN n",
	} {
		require.NoError(t, cantlr.Validate(query), query)
	}
}
