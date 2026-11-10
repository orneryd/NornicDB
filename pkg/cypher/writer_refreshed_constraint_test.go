package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNoOpRefreshedWriterPreservesCommittedUniqueClaims(t *testing.T) {
	for _, shape := range []struct{ name, constraint, seed, peer, noop, duplicate, read string }{
		{"node", "CREATE CONSTRAINT doc_token FOR (n:Doc) REQUIRE n.token IS UNIQUE", "CREATE (:Doc {token:'old', value:1})", "MATCH (n:Doc) SET n.token='new'", "MATCH (n:Doc) SET n.value=1", "CREATE (:Doc {token:'new'})", "MATCH (n:Doc) RETURN n.token"},
		{"relationship", "CREATE CONSTRAINT rel_token FOR ()-[r:HAS]-() REQUIRE r.token IS UNIQUE", "CREATE (:Doc)-[:HAS {token:'old', value:1}]->(:Part)", "MATCH ()-[r:HAS]->() SET r.token='new'", "MATCH ()-[r:HAS]->() SET r.value=1", "CREATE (:Doc)-[:HAS {token:'new'}]->(:Part)", "MATCH ()-[r:HAS]->() RETURN r.token"},
		{"composite_node", "CREATE CONSTRAINT doc_tuple FOR (n:Doc) REQUIRE (n.token, n.tenant) IS UNIQUE", "CREATE (:Doc {token:'old', tenant:'one', value:1})", "MATCH (n:Doc) SET n.token='new'", "MATCH (n:Doc) SET n.value=1", "CREATE (:Doc {token:'new', tenant:'one'})", "MATCH (n:Doc) RETURN n.token"},
		{"composite_relationship", "CREATE CONSTRAINT rel_tuple FOR ()-[r:HAS]-() REQUIRE (r.token, r.tenant) IS UNIQUE", "CREATE (:Doc)-[:HAS {token:'old', tenant:'one', value:1}]->(:Part)", "MATCH ()-[r:HAS]->() SET r.token='new'", "MATCH ()-[r:HAS]->() SET r.value=1", "CREATE (:Doc)-[:HAS {token:'new', tenant:'one'}]->(:Part)", "MATCH ()-[r:HAS]->() RETURN r.token"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			ctx := t.Context()
			store, peer := newMergeRaceStore(t)
			executeClauseQueries(t, peer, ctx, shape.constraint, shape.seed)
			writer := NewStorageExecutor(store)
			executeClauseQueries(t, writer, ctx, "BEGIN", shape.read)
			defer writer.Execute(ctx, "ROLLBACK", nil)
			executeClauseQueries(t, peer, ctx, shape.peer)
			executeClauseQueries(t, writer, ctx, shape.noop)
			require.Zero(t, writer.txContext.tx.(*storage.BadgerTransaction).OperationCount())
			_, err := writer.Execute(ctx, shape.duplicate, nil)
			if err == nil {
				_, err = writer.Execute(ctx, "COMMIT", nil)
			}
			require.Error(t, err, "a refreshed read image still owns its committed UNIQUE value")
			require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintValidationFailed")
			require.Equal(t, [][]interface{}{{"new"}}, refreshedReadRows(t, peer, ctx, shape.read))
		})
	}
}
