package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// asParams runs a statement with params on lib as a login holding roles.
func (f *collectionAccessFixture) asParams(t *testing.T, roles []string, statement string, params map[string]interface{}) (*ExecuteResult, error) {
	t.Helper()
	access := PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
		return ResolveGraphVisibility(context.Background(), f.engine, roles, database)
	}}
	return f.admin.Execute(WithPrincipalAccess(context.Background(), "lib", access), statement, params)
}

// A restricted login's statement addresses every node it created: a node is
// created before the properties a DERIVE entry matches are set, so the scope
// would hide it from the rest of its own statement. It stays visible to the
// statement, and in an explicit transaction to the transaction's later
// statements, in every create form. Once the transaction ends, the scope
// decides alone: a node filed in the login's collection is visible, one
// filed elsewhere (W1: the write switch governs writes) is hidden from it.
// A created node is never a source of another node's visibility, so
// creating a document with a hidden document's key reveals nothing.
func TestCollectionAccessCreatedNodesStayAddressable(t *testing.T) {
	agent := []string{"agent_a"}
	forms := []struct {
		name       string
		statements []string // each RETURNs the node's id; later ones run in a transaction only
		explicit   bool     // the form needs an explicit transaction
	}{
		{name: "MERGE SET +=", statements: []string{"MERGE (n:GraphNode {id: $record.id}) SET n += $record RETURN n.id"}},
		{name: "MERGE ON CREATE SET", statements: []string{
			"MERGE (n:GraphNode {id: $record.id}) ON CREATE SET n.origin = $record.origin, n.collection_ids = $record.collection_ids RETURN n.id"}},
		{name: "CREATE map", statements: []string{"CREATE (n:GraphNode $record) RETURN n.id"}},
		{name: "CREATE SET +=", statements: []string{"CREATE (n:GraphNode {id: $record.id}) SET n += $record RETURN n.id"}},
		{name: "CREATE, then MATCH SET", explicit: true, statements: []string{
			"CREATE (n:GraphNode {id: $record.id}) RETURN n.id",
			"MATCH (n:GraphNode {id: $record.id}) SET n += $record RETURN n.id"}},
	}
	for _, form := range forms {
		for _, explicit := range []bool{false, true} {
			if form.explicit && !explicit {
				continue
			}
			for _, collection := range []string{"A", "B"} {
				name := form.name + "/" + map[bool]string{false: "auto-commit", true: "explicit"}[explicit] + "/collection " + collection
				t.Run(name, func(t *testing.T) {
					f := newCollectionAccessFixture(t)
					record := map[string]interface{}{"id": "new", "origin": "user", "collection_ids": []interface{}{collection}}
					params := map[string]interface{}{"record": record}
					ctx := context.Background()
					if explicit {
						_, err := f.admin.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					for _, statement := range form.statements {
						result, err := f.asParams(t, agent, statement, params)
						require.NoError(t, err, statement)
						require.Equal(t, [][]interface{}{{"new"}}, result.Rows, statement)
					}
					if explicit {
						result, err := f.asParams(t, agent, "MATCH (n:GraphNode {id: 'new'}) RETURN n.id", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{"new"}}, result.Rows, "visible to the transaction's later statements")
						_, err = f.admin.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					stored, err := f.admin.Execute(ctx, "MATCH (n:GraphNode {id: 'new'}) RETURN n.origin, n.collection_ids", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"user", []interface{}{collection}}}, stored.Rows)
					want := []string(nil)
					if collection == "A" {
						want = []string{"new"}
					}
					require.Equal(t, want, f.ids(t, agent, "MATCH (n:GraphNode {id: 'new'}) RETURN n.id"), "after the transaction the scope decides")
				})
			}
		}
	}

	t.Run("a created node reveals nothing", func(t *testing.T) {
		f := newCollectionAccessFixture(t)
		ctx := context.Background()
		_, err := f.admin.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		// d2 is hidden (collection B); its version v2 derives from the key d2.
		result, err := f.asParams(t, agent, "CREATE (d:Document {id: 'd2'}) RETURN d.id", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"d2"}}, result.Rows)
		for _, statement := range []string{
			"MATCH (v:Version) RETURN v.id",
			"MATCH (s:SearchSource) RETURN s.id",
			"MATCH (d:Document {id: 'd2'})-[:HAS_VERSION]->(v) RETURN v.id",
		} {
			ids := f.ids(t, agent, statement)
			require.NotContains(t, ids, "v2", statement)
			require.NotContains(t, ids, "s2", statement)
		}
		_, err = f.admin.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
	})
}
