package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestNOR1112CollectionViewerVisibilityCurrentContract characterizes the
// reported #935/#936 example on the current source: two documents, two
// collections, and a caller-supplied collection predicate.
//
// Current contract, not a feature test. The engine has no collection access
// scope and no graph TRAVERSE/READ/MATCH privileges, so a "viewer" here is
// only the query text plus parameters the application would send. The
// filtered read returns the permitted document because the caller wrote the
// predicate; the unfiltered read returns both documents. The collection
// policies the report used to restrict the viewer are still refused as
// unsupported administration commands (also pinned by
// TestUnsupportedAdministrationCommands), so this is a missing capability,
// not a bypass of an installed collection ACL.
func TestNOR1112CollectionViewerVisibilityCurrentContract(t *testing.T) {
	const unsupported = "Neo.ClientError.Statement.UnsupportedAdministrationCommand"
	for _, parser := range []struct {
		name  string
		scope func() func()
	}{
		{name: "nornic", scope: config.WithNornicParser},
		{name: "antlr", scope: config.WithANTLRParser},
	} {
		t.Run(parser.name, func(t *testing.T) {
			cleanup := parser.scope()
			t.Cleanup(cleanup)

			exec, _ := newTestExecutor(t, "nornic")
			ctx := context.Background()

			_, err := exec.Execute(ctx, `CREATE (a:PDDocument {id:'allowed'}),(b:PDDocument {id:'secret'}),
(c:PDCollection {id:'A'}),(d:PDCollection {id:'B'}),
(a)-[:MEMBER_OF]->(c),(b)-[:MEMBER_OF]->(d)`, nil)
			require.NoError(t, err)

			filtered, err := exec.Execute(ctx, `MATCH (d:PDDocument) WHERE EXISTS { MATCH (d)-[:MEMBER_OF]->(project:PDCollection) WHERE project.id IN $collection_ids } RETURN d.id AS id ORDER BY id`,
				map[string]any{"collection_ids": []any{"A"}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"allowed"}}, filtered.Rows,
				"the caller-written collection predicate selects only the permitted document")

			unfiltered, err := exec.Execute(ctx, `MATCH (d:PDDocument) RETURN d.id AS id ORDER BY id`, map[string]any{})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"allowed"}, {"secret"}}, unfiltered.Rows,
				"without a caller predicate both documents are visible: the engine imposes no collection visibility")

			// The policy statements the report tried are still refused, so no
			// installed collection ACL is being bypassed above.
			for _, policy := range []string{
				"DENY TRAVERSE ON GRAPH nornic NODES PDDocument TO viewer",
				"GRANT TRAVERSE ON GRAPH nornic FOR (n:PDDocument) WHERE n.collection_id = 'A' TO viewer",
			} {
				_, err := exec.Execute(ctx, policy, nil)
				require.Error(t, err, policy)
				require.Contains(t, statusText(err), unsupported, policy)
			}
		})
	}
}
