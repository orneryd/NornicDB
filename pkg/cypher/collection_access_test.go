package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// collectionAccessFixture is a library shaped like a document application's:
// collections A, B; d1 in A, d2 in B, d3 in A and B, d4 in none, d5 an
// unfiled attachment of d1; versions and search sources by document_id; an
// original shared by d1's and d2's versions; mentions resolving to
// identities; a library definition; a summary owned by collection B.
type collectionAccessFixture struct {
	system *StorageExecutor // the system database: roles, privileges, scope
	admin  *StorageExecutor // lib, unrestricted
	engine storage.Engine   // the system store
	lib    storage.Engine   // the library database
}

const collectionScope = `CREATE ACCESS SCOPE collections ON GRAPH lib
  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
  DERIVE (x:Document) FROM (x)-[:ATTACHMENT_OF]->(d)
  DERIVE (x:Collection) FROM COLLECTION x.id
  DERIVE (x:Version) FROM DOCUMENT x.document_id
  DERIVE (x:SearchSource) FROM DOCUMENT x.document_id
  DERIVE (x:Original) FROM (x)<-[:HAS_ORIGINAL]-(:Version)<-[:HAS_VERSION]-(d)
  DERIVE (x:GraphNode) WHERE x.origin = 'automatic' FROM DOCUMENT x.document_id
  DERIVE (x:GraphNode) WHERE x.origin <> 'automatic' AND x.collection_ids IS NOT NULL FROM COLLECTION x.collection_ids
  DERIVE (x:Identity) FROM (x)<-[:RELATION {kind: 'resolves_to'}]-(t:GraphNode)
  DERIVE (x:Summary) WHERE x.owner_kind = 'collection' FROM COLLECTION x.owner_id
  DERIVE (x:Summary) WHERE x.owner_kind = 'document' FROM DOCUMENT x.owner_id`

func newCollectionAccessFixture(t *testing.T) *collectionAccessFixture {
	t.Helper()
	inner := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(inner, "system")
	f := &collectionAccessFixture{
		system: NewStorageExecutor(system),
		lib:    storage.NewNamespacedEngine(inner, "lib"),
		engine: system,
	}
	f.admin = NewStorageExecutor(f.lib)
	// The GDS catalog is the process's: each test starts and ends empty.
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	ctx := context.Background()
	roles := auth.NewRoleStore(system)
	for _, role := range []string{"agent_a", "agent_ab", "agent_b_only"} {
		require.NoError(t, roles.CreateRole(ctx, role))
	}
	for _, statement := range []string{
		collectionScope,
		"GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent_a, agent_ab",
		"GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'B'}) TO agent_ab, agent_b_only",
		"DENY MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'B'}) TO agent_ab",
		"GRANT MATCH {*} ON GRAPH lib FOR (n:Definition) WHERE n.level = 'library' TO PUBLIC",
	} {
		_, err := f.system.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	_, err := f.admin.Execute(ctx, `
		CREATE (a:Collection {id: 'A', name: 'Household'}), (b:Collection {id: 'B', name: 'Medical', archived: true})
		CREATE (d1:Document {id: 'd1'})-[:MEMBER_OF]->(a)
		CREATE (d2:Document {id: 'd2'})-[:MEMBER_OF]->(b)
		CREATE (d3:Document {id: 'd3'})-[:MEMBER_OF]->(a), (d3)-[:MEMBER_OF]->(b)
		CREATE (d4:Document {id: 'd4'})
		CREATE (d5:Document {id: 'd5'})-[:ATTACHMENT_OF]->(d1)
		CREATE (v1:Version {id: 'v1', document_id: 'd1'}), (d1)-[:HAS_VERSION]->(v1)
		CREATE (v2:Version {id: 'v2', document_id: 'd2'}), (d2)-[:HAS_VERSION]->(v2)
		CREATE (s1:SearchSource {id: 's1', document_id: 'd1'}), (s2:SearchSource {id: 's2', document_id: 'd2'})
		CREATE (o:Original {id: 'shared'}), (v1)-[:HAS_ORIGINAL]->(o), (v2)-[:HAS_ORIGINAL]->(o)
		CREATE (o2:Original {id: 'only-d2'}), (v2)-[:HAS_ORIGINAL]->(o2)
		CREATE (m1:GraphNode:Entity {id: 'm1', origin: 'automatic', document_id: 'd1'})-[:RELATION {kind: 'resolves_to'}]->(i1:Identity {id: 'i1'})
		CREATE (m2:GraphNode:Entity {id: 'm2', origin: 'automatic', document_id: 'd2'})-[:RELATION {kind: 'resolves_to'}]->(i2:Identity {id: 'i2'})
		CREATE (:GraphNode {id: 'u1', origin: 'user', collection_ids: ['B', 'A']})
		CREATE (:GraphNode {id: 'u2', origin: 'user', collection_ids: ['B']})
		CREATE (:Definition {id: 'lib', level: 'library'}), (:Definition {id: 'proj', level: 'project'})
		CREATE (:Summary {id: 'sumB', owner_kind: 'collection', owner_id: 'B'})
		CREATE (:Summary {id: 'sumD1', owner_kind: 'document', owner_id: 'd1'})
		CREATE (:Unscoped {id: 'other'})`, nil)
	require.NoError(t, err)
	return f
}

// as runs a statement on lib as a login holding roles.
func (f *collectionAccessFixture) as(t *testing.T, roles []string, statement string) (*ExecuteResult, error) {
	t.Helper()
	access := PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
		return ResolveGraphVisibility(context.Background(), f.engine, roles, database)
	}}
	return f.admin.Execute(WithPrincipalAccess(context.Background(), "lib", access), statement, nil)
}

func (f *collectionAccessFixture) ids(t *testing.T, roles []string, statement string) []string {
	t.Helper()
	result, err := f.as(t, roles, statement)
	require.NoError(t, err, statement)
	var ids []string
	for _, row := range result.Rows {
		if id, ok := row[0].(string); ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// The settled rules for a login restricted to collection A:
//  1. ANY: a document in several collections is visible through any granted
//     one; an unfiled document only to logins without a collection rule;
//  4. derived and shared nodes follow their sources, by ANY;
//  5. a collection-owned summary follows its collection;
//  6. an attachment is visible through its own membership or its email's;
//  7. only granted collection nodes are visible (archived changes nothing),
//     and PUBLIC's library definitions to everyone.
func TestCollectionAccessVisibility(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	require.Equal(t, []string{"d1", "d3", "d5"}, f.ids(t, agent, "MATCH (n:Document) RETURN n.id"))
	require.Equal(t, []string{"A"}, f.ids(t, agent, "MATCH (n:Collection) RETURN n.id"))
	require.Equal(t, []string{"v1"}, f.ids(t, agent, "MATCH (n:Version) RETURN n.id"))
	require.Equal(t, []string{"s1"}, f.ids(t, agent, "MATCH (n:SearchSource) RETURN n.id"))
	require.Equal(t, []string{"shared"}, f.ids(t, agent, "MATCH (n:Original) RETURN n.id"))
	require.Equal(t, []string{"m1", "u1"}, f.ids(t, agent, "MATCH (n:GraphNode) RETURN n.id"))
	require.Equal(t, []string{"i1"}, f.ids(t, agent, "MATCH (n:Identity) RETURN n.id"))
	require.Equal(t, []string{"lib"}, f.ids(t, agent, "MATCH (n:Definition) RETURN n.id"))
	require.Equal(t, []string{"sumD1"}, f.ids(t, agent, "MATCH (n:Summary) RETURN n.id"))
	require.Empty(t, f.ids(t, agent, "MATCH (n:Unscoped) RETURN n.id"))

	// A relationship is visible when both ends are: d3's membership in B is
	// hidden, so is the path to anything hidden.
	require.Equal(t, []string{"A"}, f.ids(t, agent, "MATCH (:Document {id: 'd3'})-[:MEMBER_OF]->(c) RETURN c.id"))
	require.Empty(t, f.ids(t, agent, "MATCH (n {id: 'd2'}) RETURN n.id"))
	require.Empty(t, f.ids(t, agent, "MATCH (:Original {id: 'shared'})<-[:HAS_ORIGINAL]-(v:Version {id: 'v2'}) RETURN v.id"))

	// Counts count what is visible, through every count path.
	result, err := f.as(t, agent, "MATCH (n:Document) RETURN count(n) AS c")
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Rows[0][0])
	result, err = f.as(t, agent, "MATCH (n) RETURN count(*) AS c")
	require.NoError(t, err)
	require.EqualValues(t, 12, result.Rows[0][0])
	result, err = f.as(t, agent, "MATCH ()-[r:MEMBER_OF]->() RETURN count(r) AS c")
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Rows[0][0])
}

// A login with no collection rule sees the whole library, unfiled documents
// included, as before graph privileges existed; so does a request with no
// principal.
func TestCollectionAccessUnlimitedLogin(t *testing.T) {
	f := newCollectionAccessFixture(t)
	require.Equal(t, []string{"d1", "d2", "d3", "d4", "d5"}, f.ids(t, []string{"editor"}, "MATCH (n:Document) RETURN n.id"))
	result, err := f.admin.Execute(context.Background(), "MATCH (n:Document) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 5, result.Rows[0][0])
}

// DENY wins over GRANT, across a login's roles: granted A and B but denied
// B, a document in both is hidden; a role that only holds B sees B's.
func TestCollectionAccessDenyWins(t *testing.T) {
	f := newCollectionAccessFixture(t)
	require.Equal(t, []string{"d1", "d5"}, f.ids(t, []string{"agent_ab"}, "MATCH (n:Document) RETURN n.id"))
	require.Equal(t, []string{"d2", "d3"}, f.ids(t, []string{"agent_b_only"}, "MATCH (n:Document) RETURN n.id"))
	// Granting a further role can't undo another role's DENY.
	require.Equal(t, []string{"d1", "d5"}, f.ids(t, []string{"agent_ab", "agent_b_only"}, "MATCH (n:Document) RETURN n.id"))
}

// W1: a restricted login writes what it can see; a hidden node can't be
// matched to be changed; deleting a node with a relationship it can't see
// is refused and changes nothing.
func TestCollectionAccessWrites(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	_, err := f.as(t, agent, "MATCH (d:Document {id: 'd1'}) SET d.seen = true")
	require.NoError(t, err)
	_, err = f.as(t, agent, "MATCH (d:Document {id: 'd2'}) SET d.seen = true")
	require.NoError(t, err)
	result, err := f.admin.Execute(context.Background(), "MATCH (d:Document) WHERE d.seen RETURN d.id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"d1"}}, result.Rows)

	_, err = f.as(t, agent, "MATCH (d:Document {id: 'd3'}) DETACH DELETE d")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Security.Forbidden")
	result, err = f.admin.Execute(context.Background(), "MATCH (d:Document {id: 'd3'})-[r]-() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Rows[0][0])
}

// The access scope and graph privileges are Cypher administration commands:
// SHOW PRIVILEGES lists the rules, SHOW ACCESS SCOPES the scope; the scope
// is refused when one exists, and malformed declarations are syntax errors.
func TestCollectionAccessAdministration(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	result, err := f.system.Execute(ctx, "SHOW PRIVILEGES YIELD access, segment, role WHERE role = 'agent_ab'", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	result, err = f.system.Execute(ctx, "SHOW ACCESS SCOPES", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "lib", result.Rows[0][0])

	_, err = f.system.Execute(ctx, collectionScope, nil)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	for _, invalid := range []string{
		"CREATE ACCESS SCOPE s ON GRAPH g MEMBERSHIP (d:D)-[:M]->(c:C) KEY d.id, c.id DERIVE (x:X) FROM (x)-[:R*]->(d)",
		"CREATE ACCESS SCOPE s ON GRAPH g MEMBERSHIP (d:D)-[:M]->(c:C) KEY d.id, c.id DERIVE (x:X) FROM DOCUMENT x.a DERIVE (x:X) FROM DOCUMENT x.b",
		"GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 7}) TO agent_a",
	} {
		_, err := f.system.Execute(ctx, invalid, nil)
		require.Error(t, err, invalid)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", invalid)
	}
	for _, unsupported := range []string{
		"GRANT TRAVERSE ON GRAPH lib NODES Document TO agent_a",
		"GRANT READ {name} ON GRAPH lib NODES Document TO agent_a",
		"GRANT MATCH {*} ON GRAPH lib RELATIONSHIPS MEMBER_OF TO agent_a",
	} {
		_, err := f.system.Execute(ctx, unsupported, nil)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.UnsupportedAdministrationCommand", unsupported)
	}

	// REVOKE removes a rule; a GRANT to a missing role changes nothing.
	_, err = f.system.Execute(ctx, "REVOKE GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'B'}) FROM agent_b_only", nil)
	require.NoError(t, err)
	_, err = f.system.Execute(ctx, "GRANT MATCH {*} ON GRAPH lib NODES Document TO nobody", nil)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	visibility, err := ResolveGraphVisibility(ctx, f.engine, []string{"agent_b_only"}, "lib")
	require.NoError(t, err)
	require.Nil(t, visibility, "a login whose last collection rule was revoked is unrestricted again")
}

// GDS for a restricted login: a Cypher aggregation projection is built from
// the rows its view read and kept in its own catalog partition, where a
// login with other privileges neither sees it nor collides with its name;
// the label-and-type projection is refused; gds.util.asNode of a node it
// can't see is null.
func TestCollectionAccessGDS(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	ctx := context.Background()
	// The agent signs in as a user, so its catalog is its own (the catalog
	// key is the executing principal).
	asAgent := func(statement string) (*ExecuteResult, error) {
		return f.admin.Execute(f.principalContext("agent", agent), statement, nil)
	}

	result, err := asAgent("MATCH (s:Document) RETURN gds.graph.project('topics', s) AS g")
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Rows[0][0].(map[string]interface{})["nodeCount"])

	result, err = f.admin.Execute(ctx, "MATCH (s:Document) RETURN gds.graph.project('topics', s) AS g", nil)
	require.NoError(t, err, "a same-named projection of another login doesn't collide")
	require.EqualValues(t, 5, result.Rows[0][0].(map[string]interface{})["nodeCount"])

	result, err = asAgent("CALL gds.graph.list() YIELD graphName, nodeCount RETURN graphName, nodeCount")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"topics", int64(3)}}, result.Rows)

	_, err = asAgent("CALL gds.graph.project('native', 'Document', '*')")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Security.Forbidden")
	require.Contains(t, err.Error(), "gds.graph.project", "refused by its contract (PrivilegeUnrestrictedView)")

	result, err = f.admin.Execute(ctx, "MATCH (n:Document {id: 'd2'}) RETURN id(n) AS id", nil)
	require.NoError(t, err)
	hidden := result.Rows[0][0]
	result, err = f.admin.Execute(WithPrincipalAccess(ctx, "lib", PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
		return ResolveGraphVisibility(ctx, f.engine, agent, database)
	}}), "RETURN gds.util.asNode($id) AS n", map[string]interface{}{"id": hidden})
	require.NoError(t, err)
	require.Nil(t, result.Rows[0][0])

	_, err = asAgent("CALL gds.graph.drop('topics') YIELD graphName RETURN graphName")
	require.NoError(t, err)
	result, err = f.admin.Execute(ctx, "CALL gds.graph.list() YIELD graphName, nodeCount RETURN graphName, nodeCount", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"topics", int64(5)}}, result.Rows, "the other login's projection is untouched")
}

// db.retrieve for a restricted login returns only nodes it can see, from a
// candidate pool its visibility filtered before ranking: documents of
// collections it isn't granted never appear, however well they match.
func TestCollectionAccessRetrieve(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	svc := search.NewServiceWithDimensions(f.lib, 3)
	t.Cleanup(func() { _ = svc.Close() })
	f.admin.SetSearchService(svc)
	_, err := f.admin.Execute(ctx, `MATCH (d:Document) SET d.content = CASE d.id
		WHEN 'd1' THEN 'zephyrine household budget'
		WHEN 'd2' THEN 'zephyrine zephyrine zephyrine medical'
		ELSE 'zephyrine' END`, nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))

	retrieved := func(roles []string) []string {
		result, err := f.as(t, roles, "CALL db.retrieve({query: 'zephyrine', limit: 10}) YIELD node RETURN node.id AS id")
		require.NoError(t, err)
		var ids []string
		for _, row := range result.Rows {
			if id, ok := row[0].(string); ok {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return ids
	}
	var all []string
	require.Eventually(t, func() bool {
		all = retrieved([]string{"editor"})
		return len(all) == 5
	}, 10*time.Second, 50*time.Millisecond, "the unrestricted login finds every document: %v", all)
	require.Equal(t, []string{"d1", "d3", "d5"}, retrieved([]string{"agent_a"}))
	require.Equal(t, []string{"d1", "d5"}, retrieved([]string{"agent_ab"}))
}

// principalContext is a statement context for a principal as the transports
// build it, impersonation included: the executing user (the impersonated
// one while a session impersonates) in the request identity, and the one
// resolver built from that user's roles (cypher.PrincipalAccess).
func (f *collectionAccessFixture) principalContext(user string, roles []string) context.Context {
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: user, Roles: roles}})
	return WithPrincipalAccess(ctx, "lib", PrincipalAccess{
		Entitlement: func(string) bool { return true },
		Visibility: func(database string) (*GraphVisibility, error) {
			return ResolveGraphVisibility(context.Background(), f.engine, roles, database)
		},
	})
}

// A service can tell whether its principal (the executing user, the
// impersonated one under impersonation) is collection-limited: SHOW CURRENT
// USER YIELD collectionLimited lists the databases where it holds a
// collection rule, and SHOW USER PRIVILEGES lists the rules in Neo4j's
// privilege rows. SHOW CURRENT USER's default columns stay Neo4j's.
func TestCollectionLimitedPrincipal(t *testing.T) {
	f := newCollectionAccessFixture(t)
	limited := func(ctx context.Context) []string {
		t.Helper()
		result, err := f.admin.Execute(ctx, "SHOW CURRENT USER YIELD user, collectionLimited RETURN collectionLimited", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0][0].([]string)
	}
	cases := []struct {
		name  string
		user  string
		roles []string
		want  []string
	}{
		{"normal login", "agent", []string{"agent_a"}, []string{"lib"}},
		{"impersonated limited", "impersonated_agent", []string{"agent_ab"}, []string{"lib"}},
		{"impersonated unlimited", "impersonated_editor", []string{"editor"}, []string{}},
	}
	for _, tc := range cases {
		ctx := f.principalContext(tc.user, tc.roles)
		require.Equal(t, tc.want, limited(ctx), tc.name)
		result, err := f.admin.Execute(ctx, "SHOW CURRENT USER YIELD collectionLimited RETURN 'lib' IN collectionLimited AS limited", nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, len(tc.want) > 0, result.Rows[0][0], tc.name)

		result, err = f.admin.Execute(ctx, "SHOW CURRENT USER", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"user", "roles", "passwordChangeRequired", "suspended", "home"}, result.Columns, tc.name)

		result, err = f.system.Execute(ctx, "SHOW USER PRIVILEGES YIELD access, action, segment, role, user WHERE segment CONTAINS 'MEMBER_OF' RETURN access, role, user", nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, len(tc.want) > 0, len(result.Rows) > 0, tc.name)
		for _, row := range result.Rows {
			require.Equal(t, tc.user, row[2], tc.name)
			require.Contains(t, tc.roles, row[1], tc.name)
		}
	}

	// Another user's privileges need administrator rights.
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		User:  &AuthenticatedUser{Name: "agent", Roles: []string{"agent_a"}},
		Users: func() []UserListing { return []UserListing{{Name: "other", Roles: []string{"agent_ab"}}} },
	})
	ctx = WithPrincipalAccess(ctx, "lib", PrincipalAccess{Entitlement: func(permission string) bool { return permission != "admin" }})
	_, err := f.system.Execute(ctx, "SHOW USER other PRIVILEGES", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Security.Forbidden")
}

// Visibility follows a graph name to the database its data is in, as
// permissions do: a statement selecting the library through an alias is as
// restricted as one naming it, and a permission-only attachment made later
// keeps the principal's visibility.
func TestCollectionAccessThroughAliasAndPermissionWrapper(t *testing.T) {
	f := newCollectionAccessFixture(t)
	access := PrincipalAccess{
		GraphAccess: func(graph string) (string, bool) {
			if graph == "library" {
				return "lib", true
			}
			return graph, true
		},
		Visibility: func(database string) (*GraphVisibility, error) {
			return ResolveGraphVisibility(context.Background(), f.engine, []string{"agent_a"}, database)
		},
	}
	ctx := WithPrincipalAccess(context.Background(), "library", access)
	result, err := f.admin.Execute(ctx, "MATCH (n:Document) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Rows[0][0])
	require.True(t, access.RestrictedOn("library"))

	ctx = WithDatabasePermissionResolver(ctx, "library", func(string, string) bool { return true })
	result, err = f.admin.Execute(ctx, "MATCH (n:Document) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Rows[0][0])
}

// When a node carries labels with entries of their own, the most specific
// entry wins: one whose label set strictly contains another's. A document is
// governed by its membership and entries naming the document label, never
// by an entry for another label it carries. Most specific entries with
// incomparable label sets leave the node hidden; equally specific entries
// whose WHEREs overlap are refused when the scope is declared.
func TestAccessScopeMostSpecificEntryWins(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	_, err := f.system.Execute(ctx, `CREATE OR REPLACE ACCESS SCOPE collections ON GRAPH lib
	  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
	  DERIVE (x:GraphNode) FROM DOCUMENT x.document_id
	  DERIVE (x:GraphNode:Identity) FROM (x)<-[:RELATION {kind: 'resolves_to'}]-(t:GraphNode)
	  DERIVE (x:Summary) FROM DOCUMENT x.owner_id`, nil)
	require.NoError(t, err)
	_, err = f.admin.Execute(ctx, `
		CREATE (:GraphNode:Identity {id: 'specific-hidden', document_id: 'd1'})
		CREATE (m:GraphNode {id: 'mention-d1', document_id: 'd1'})-[:RELATION {kind: 'resolves_to'}]->(:GraphNode:Identity {id: 'specific-visible', document_id: 'd2'})
		CREATE (:GraphNode:Summary {id: 'ambiguous', document_id: 'd1', owner_id: 'd1'})
		CREATE (:Document:GraphNode {id: 'unfiled-doc', document_id: 'd1'})`, nil)
	require.NoError(t, err)

	agent := []string{"agent_a"}
	// The identity entry, not the generic document_id entry, decides.
	require.Equal(t, []string{"specific-visible"}, f.ids(t, agent, "MATCH (n:Identity) RETURN n.id"))
	// Generic entry for a node with no more specific entry.
	require.Equal(t, []string{"mention-d1"}, f.ids(t, agent, "MATCH (n:GraphNode) WHERE n.id = 'mention-d1' RETURN n.id"))
	// (GraphNode) and (Summary) both apply and neither is more specific.
	require.Empty(t, f.ids(t, agent, "MATCH (n {id: 'ambiguous'}) RETURN n.id"))
	// A document isn't governed by the generic entry: unfiled stays hidden.
	require.Empty(t, f.ids(t, agent, "MATCH (n {id: 'unfiled-doc'}) RETURN n.id"))

	_, err = f.system.Execute(ctx, `CREATE OR REPLACE ACCESS SCOPE collections ON GRAPH lib
	  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
	  DERIVE (x:GraphNode:Identity) FROM DOCUMENT x.document_id
	  DERIVE (x:Identity:GraphNode) FROM DOCUMENT x.other_id`, nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
}

// A document in a denied collection stays hidden whatever DERIVE source it
// has, and so do the nodes derived from it; one merely ungranted still
// follows its sources (an attachment of a visible email).
func TestCollectionAccessDenyBeatsDerive(t *testing.T) {
	f := newCollectionAccessFixture(t)
	_, err := f.admin.Execute(context.Background(), `
		MATCH (d1:Document {id: 'd1'}), (b:Collection {id: 'B'})
		CREATE (d6:Document {id: 'd6'})-[:ATTACHMENT_OF]->(d1), (d6)-[:MEMBER_OF]->(b)
		CREATE (:Version {id: 'v6', document_id: 'd6'})`, nil)
	require.NoError(t, err)
	// agent_ab denies B: d6 and its version stay hidden.
	require.Equal(t, []string{"d1", "d5"}, f.ids(t, []string{"agent_ab"}, "MATCH (n:Document) RETURN n.id"))
	require.Equal(t, []string{"v1"}, f.ids(t, []string{"agent_ab"}, "MATCH (n:Version) RETURN n.id"))
	// agent_a merely isn't granted B: d6 follows d1.
	require.Equal(t, []string{"d1", "d3", "d5", "d6"}, f.ids(t, []string{"agent_a"}, "MATCH (n:Document) RETURN n.id"))
	require.Equal(t, []string{"v1", "v6"}, f.ids(t, []string{"agent_a"}, "MATCH (n:Version) RETURN n.id"))
}

// A restricted login's saved projection doesn't outlive what it can see:
// once a projected node is hidden from it (its document moved to another
// collection, or the scope replaced), the projection is refused for its
// properties and its algorithms; a node deleted since doesn't count.
func TestCollectionAccessGDSProjectionFollowsCurrentVisibility(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	ctx := context.Background()
	_, err := f.admin.Execute(ctx, "MATCH (d:Document) SET d.score = toInteger(substring(d.id, 1))", nil)
	require.NoError(t, err)
	result, err := f.as(t, agent, "MATCH (d:Document) RETURN gds.graph.project('saved', d, null, {sourceNodeProperties: {score: d.score}, targetNodeProperties: null}) AS g")
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Rows[0][0].(map[string]interface{})["nodeCount"])
	idOf := func(document string) interface{} {
		result, err := f.admin.Execute(ctx, "MATCH (n:Document {id: $id}) RETURN id(n) AS id", map[string]interface{}{"id": document})
		require.NoError(t, err)
		return result.Rows[0][0]
	}
	d1, d3 := idOf("d1"), idOf("d3")
	agentCtx := WithPrincipalAccess(ctx, "lib", PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
		return ResolveGraphVisibility(ctx, f.engine, agent, database)
	}})
	property := func(id interface{}) (interface{}, error) {
		result, err := f.admin.Execute(agentCtx, "RETURN gds.util.nodeProperty('saved', $id, 'score') AS score", map[string]interface{}{"id": id})
		if err != nil {
			return nil, err
		}
		return result.Rows[0][0], nil
	}
	value, err := property(d1)
	require.NoError(t, err)
	require.EqualValues(t, 1, value)

	// A projected node deleted since leaves the projection usable.
	_, err = f.admin.Execute(ctx, "MATCH (n:Document {id: 'd5'}) DETACH DELETE n", nil)
	require.NoError(t, err)
	value, err = property(d3)
	require.NoError(t, err)
	require.EqualValues(t, 3, value)

	// d1 moves to collection B, which agent_a isn't granted.
	_, err = f.admin.Execute(ctx, "MATCH (:Document {id: 'd1'})-[m:MEMBER_OF]->(:Collection {id: 'A'}) DELETE m WITH 1 AS one MATCH (d:Document {id: 'd1'}), (b:Collection {id: 'B'}) CREATE (d)-[:MEMBER_OF]->(b)", nil)
	require.NoError(t, err)
	require.Empty(t, f.ids(t, agent, "MATCH (n:Document {id: 'd1'}) RETURN n.id"))
	for _, id := range []interface{}{d1, d3} {
		_, err = property(id)
		require.Error(t, err, "the projection still holds d1")
		require.Contains(t, err.Error(), "no longer see")
	}
	_, err = f.as(t, agent, "CALL gds.louvain.stream('saved') YIELD nodeId RETURN count(*) AS n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no longer see")
	// The agent can still drop the stale projection.
	_, err = f.as(t, agent, "CALL gds.graph.drop('saved') YIELD graphName RETURN graphName")
	require.NoError(t, err)
}

// Two logins with the same roles keep their own GDS catalogs: neither sees,
// uses, collides with or drops the other's projections.
func TestCollectionAccessGDSCatalogPerPrincipal(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	alice, bob := f.principalContext("alice", agent), f.principalContext("bob", agent)
	run := func(ctx context.Context, statement string) [][]interface{} {
		result, err := f.admin.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		return result.Rows
	}
	run(alice, "MATCH (d:Document) RETURN gds.graph.project('topics', d) AS g")
	require.Empty(t, run(bob, "CALL gds.graph.list() YIELD graphName RETURN graphName"))
	require.Equal(t, [][]interface{}{{false}}, run(bob, "RETURN gds.graph.exists('topics') AS exists"))
	run(bob, "MATCH (d:Document {id: 'd1'}) RETURN gds.graph.project('topics', d) AS g")
	run(bob, "CALL gds.graph.drop('topics') YIELD graphName RETURN graphName")
	require.Equal(t, [][]interface{}{{"topics", int64(3)}}, run(alice, "CALL gds.graph.list() YIELD graphName, nodeCount RETURN graphName, nodeCount"))
}

// Vector index queries rank only what a restricted login can see: when
// hidden nodes and relationships are the nearest, its top-K are still K
// visible ones, on the MATCH … ORDER BY vector.similarity … LIMIT fast paths
// and on db.index.vector.queryNodes / queryRelationships.
func TestCollectionAccessVectorTopK(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	svc := search.NewServiceWithDimensions(f.lib, 3)
	t.Cleanup(func() { _ = svc.Close() })
	f.admin.SetSearchService(svc)
	for _, statement := range []string{
		"CREATE VECTOR INDEX doc_emb FOR (n:Document) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX link_emb FOR ()-[r:LINKS]-() ON (r.features) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		`MATCH (d:Document) SET d.emb = CASE d.id
			WHEN 'd2' THEN [1.0, 0.0, 0.0]
			WHEN 'd4' THEN [0.95, 0.05, 0.0]
			WHEN 'd1' THEN [0.6, 0.4, 0.0]
			WHEN 'd3' THEN [0.3, 0.7, 0.0]
			ELSE [0.0, 0.0, 1.0] END`,
		`MATCH (d1:Document {id: 'd1'}), (d2:Document {id: 'd2'}), (d3:Document {id: 'd3'}), (d4:Document {id: 'd4'}), (d5:Document {id: 'd5'})
		CREATE (d2)-[:LINKS {id: 'hidden-both', features: [1.0, 0.0, 0.0]}]->(d4)
		CREATE (d3)-[:LINKS {id: 'hidden-end', features: [0.95, 0.05, 0.0]}]->(d2)
		CREATE (d1)-[:LINKS {id: 'near', features: [0.6, 0.4, 0.0]}]->(d3)
		CREATE (d1)-[:LINKS {id: 'far', features: [0.3, 0.7, 0.0]}]->(d5)`,
	} {
		_, err := f.admin.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Eventually(t, func() bool {
		return svc.IsReady() && svc.CountPropertyVectorEntries("emb") == 5 && svc.HasRelationshipVectorEntries("LINKS", "features")
	}, 10*time.Second, 50*time.Millisecond, "the vector index holds the documents' and links' vectors")

	queries := []struct {
		statement    string
		unrestricted []string
		agent        []string
	}{
		{"MATCH (n:Document) RETURN n.id AS id, vector.similarity.cosine(n.emb, [1.0, 0.0, 0.0]) AS score ORDER BY score DESC LIMIT 2", []string{"d2", "d4"}, []string{"d1", "d3"}},
		{"CALL db.index.vector.queryNodes('doc_emb', 2, [1.0, 0.0, 0.0]) YIELD node RETURN node.id AS id", []string{"d2", "d4"}, []string{"d1", "d3"}},
		{"MATCH ()-[r:LINKS]->() RETURN r.id AS id, vector.similarity.cosine(r.features, [1.0, 0.0, 0.0]) AS score ORDER BY score DESC LIMIT 2", []string{"hidden-both", "hidden-end"}, []string{"far", "near"}},
		{"CALL db.index.vector.queryRelationships('link_emb', 2, [1.0, 0.0, 0.0]) YIELD relationship RETURN relationship.id AS id", []string{"hidden-both", "hidden-end"}, []string{"far", "near"}},
	}
	for _, query := range queries {
		require.Equal(t, query.unrestricted, f.ids(t, []string{"editor"}, query.statement), "unrestricted: "+query.statement)
		require.Equal(t, query.agent, f.ids(t, []string{"agent_a"}, query.statement), "agent: "+query.statement)
	}
}

// A node derived through any number of DERIVE steps follows its document,
// and a node whose only visible source runs through a cycle is decided the
// same whichever node of the cycle is asked first.
func TestAccessScopeLongDerivationChains(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	_, err := f.system.Execute(ctx, `CREATE OR REPLACE ACCESS SCOPE collections ON GRAPH lib
	  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
	  DERIVE (x:Step1) FROM (x)-[:OF]->(d)
	  DERIVE (x:Step2) FROM (x)-[:OF]->(t:Step1)
	  DERIVE (x:Step3) FROM (x)-[:OF]->(t:Step2)
	  DERIVE (x:Step4) FROM (x)-[:OF]->(t:Step3)
	  DERIVE (x:Step5) FROM (x)-[:OF]->(t:Step4)
	  DERIVE (x:Loop) FROM (x)-[:NEXT]->(t:Loop) OR FROM DOCUMENT x.document_id`, nil)
	require.NoError(t, err)
	_, err = f.admin.Execute(ctx, `
		MATCH (d1:Document {id: 'd1'}), (d2:Document {id: 'd2'})
		CREATE (:Step5 {id: 'a5'})-[:OF]->(:Step4 {id: 'a4'})-[:OF]->(:Step3 {id: 'a3'})-[:OF]->(:Step2 {id: 'a2'})-[:OF]->(:Step1 {id: 'a1'})-[:OF]->(d1)
		CREATE (:Step5 {id: 'b5'})-[:OF]->(:Step4 {id: 'b4'})-[:OF]->(:Step3 {id: 'b3'})-[:OF]->(:Step2 {id: 'b2'})-[:OF]->(:Step1 {id: 'b1'})-[:OF]->(d2)
		CREATE (p:Loop {id: 'p', document_id: 'd1'})-[:NEXT]->(q:Loop {id: 'q', document_id: 'd2'})-[:NEXT]->(p)`, nil)
	require.NoError(t, err)

	agent := []string{"agent_a"}
	require.Equal(t, []string{"a5"}, f.ids(t, agent, "MATCH (n:Step5) RETURN n.id"))

	// p is visible through d1; q only through p. Asking p first must not
	// leave q decided as hidden while p was still being decided.
	visibility, err := ResolveGraphVisibility(ctx, f.engine, agent, "lib")
	require.NoError(t, err)
	loops, err := f.lib.GetNodesByLabel("Loop")
	require.NoError(t, err)
	byID := map[string]*storage.Node{}
	for _, node := range loops {
		byID[node.Properties["id"].(string)] = node
	}
	for _, order := range [][]string{{"p", "q"}, {"q", "p"}} {
		eval := newVisibilityEvaluator(visibility, f.lib)
		for _, id := range order {
			require.True(t, eval.nodeVisible(byID[id]), "%s asked in order %v", id, order)
		}
	}
}

// A login holding an unrestricted role and a collection role is limited:
// collection rules restrict a login whatever else it holds, unlike Neo4j's
// union of privileges, so adding a role can never widen a collection limit.
func TestCollectionAccessCombinedRoles(t *testing.T) {
	f := newCollectionAccessFixture(t)
	combined := []string{"editor", "agent_a"}
	require.Equal(t, []string{"d1", "d3", "d5"}, f.ids(t, combined, "MATCH (n:Document) RETURN n.id"))
	result, err := f.admin.Execute(f.principalContext("both", combined), "SHOW CURRENT USER YIELD collectionLimited RETURN 'lib' IN collectionLimited AS limited", nil)
	require.NoError(t, err)
	require.Equal(t, true, result.Rows[0][0])
}

// A restricted principal's statement keeps a statement deadline's status
// (#1339): past its owner's deadline it fails with that owner's transaction
// timeout, through the clone that reads its view. A refusal stays a refusal
// under a live deadline, and a read still reads through the view.
func TestCollectionAccessKeepsStatementTimeoutStatus(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	restricted := func(ctx context.Context) context.Context {
		return WithPrincipalAccess(ctx, "lib", PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
			return ResolveGraphVisibility(context.Background(), f.engine, agent, database)
		}})
	}
	for _, test := range []struct {
		cause         error
		code, message string
	}{
		{nornicerrors.ErrClientTransactionTimeout, nornicerrors.TransactionTimedOutClientConfiguration, nornicerrors.TransactionTimedOutClientConfigurationMessage},
		{nornicerrors.ErrServerStatementTimeout, nornicerrors.TransactionTimedOut, nornicerrors.TransactionTimedOutMessage},
	} {
		t.Run(test.cause.Error(), func(t *testing.T) {
			expired, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), test.cause)
			defer cancel()
			_, err := f.admin.Execute(restricted(expired), "MATCH (n:Document) RETURN n.id", nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			code, message := nornicerrors.Neo4jStatus(err)
			require.Equal(t, test.code, code)
			require.Equal(t, test.message, message)
		})
	}

	live, cancel := context.WithTimeoutCause(context.Background(), time.Minute, nornicerrors.ErrClientTransactionTimeout)
	defer cancel()
	for _, statement := range []string{
		"CALL gds.graph.project('native', 'Document', '*')",
		// Refused while it runs: d3 has a relationship the agent can't see.
		"MATCH (n:Document {id: 'd3'}) DETACH DELETE n",
	} {
		_, err := f.admin.Execute(restricted(live), statement, nil)
		require.Error(t, err, statement)
		require.NotErrorIs(t, err, context.DeadlineExceeded, statement)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", code, statement)
	}
	result, err := f.admin.Execute(restricted(live), "MATCH (n:Document) RETURN n.id", nil)
	require.NoError(t, err)
	var ids []string
	for _, row := range result.Rows {
		ids = append(ids, row[0].(string))
	}
	sort.Strings(ids)
	require.Equal(t, []string{"d1", "d3", "d5"}, ids)
}

// SHOW USER [name] PRIVILEGES lists exactly what SHOW PRIVILEGES lists for
// the roles the user holds and PUBLIC, every kind of privilege (procedure
// grants, IMPERSONATE grants and denials, graph grants and denials), with
// the user column: for a named user, the current user and an impersonated
// user alike. Another role's privileges never appear.
func TestShowUserPrivilegesListsEveryPrivilegeKind(t *testing.T) {
	f := newCollectionAccessFixture(t)
	ctx := context.Background()
	procedures := auth.NewProcedurePrivilegeStore(f.engine)
	require.NoError(t, procedures.Grant(ctx, "agent_ab", "db.labels", true, false))
	require.NoError(t, procedures.Grant(ctx, "agent_a", "db.labels", false, false))
	impersonation := auth.NewImpersonatePrivilegeStore(f.engine)
	require.NoError(t, impersonation.Set(ctx, "agent_ab", auth.ImpersonateUser("alice"), false, false))
	require.NoError(t, impersonation.Set(ctx, "agent_ab", auth.ImpersonateUser("bob"), true, false))
	require.NoError(t, impersonation.Set(ctx, "agent_a", auth.ImpersonateUser("carol"), false, false))

	rows := func(ctx context.Context, statement string) []string {
		t.Helper()
		result, err := f.system.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		var out []string
		for _, row := range result.Rows {
			out = append(out, fmt.Sprint(row...))
		}
		sort.Strings(out)
		return out
	}
	const columns = "access, action, resource, graph, segment, role, immutable"
	var want []string
	for _, row := range rows(ctx, "SHOW PRIVILEGES YIELD * WHERE role IN ['agent_ab', 'public'] RETURN "+columns) {
		want = append(want, row+"abuser")
	}
	sort.Strings(want)
	kinds := strings.Join(want, "\n")
	for _, kind := range []string{"GRANTEDexecute_boosted", "GRANTEDimpersonatedatabase*USER(alice)", "DENIEDimpersonatedatabase*USER(bob)", "GRANTEDmatch", "DENIEDmatch"} {
		require.Contains(t, kinds, kind)
	}
	require.NotContains(t, kinds, "carol")
	require.NotContains(t, kinds, "GRANTEDexecutedatabase")

	user := &AuthenticatedUser{Name: "abuser", Roles: []string{"agent_ab"}}
	named := WithRequestIdentity(ctx, &RequestIdentity{
		User:  &AuthenticatedUser{Name: "admin", Roles: []string{"admin"}},
		Users: func() []UserListing { return []UserListing{{Name: "abuser", Roles: []string{"agent_ab"}}} },
	})
	current := WithRequestIdentity(ctx, &RequestIdentity{User: user})
	impersonated := WithRequestIdentity(ctx, &RequestIdentity{User: user, ImpersonatedBy: &AuthenticatedUser{Name: "svc", Roles: []string{"admin"}}})
	for name, check := range map[string]struct {
		ctx       context.Context
		statement string
	}{
		"named user":        {named, "SHOW USER abuser PRIVILEGES YIELD * RETURN " + columns + ", user"},
		"current user":      {current, "SHOW USER PRIVILEGES YIELD * RETURN " + columns + ", user"},
		"impersonated user": {impersonated, "SHOW USER PRIVILEGES YIELD * RETURN " + columns + ", user"},
	} {
		require.Equal(t, want, rows(check.ctx, check.statement), name)
	}
}

// db.index.stats reports whole-index statistics (entry and distinct-value
// counts over hidden records too), so its contract (UnrestrictedView)
// refuses a restricted login; an unrestricted one still reads them.
func TestCollectionAccessIndexStatsRefused(t *testing.T) {
	f := newCollectionAccessFixture(t)
	_, err := f.admin.Execute(context.Background(), "CREATE INDEX doc_id FOR (n:Document) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = f.as(t, []string{"agent_a"}, "CALL db.index.stats()")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Security.Forbidden")
	require.Contains(t, err.Error(), "db.index.stats")
	_, err = f.as(t, []string{"editor"}, "CALL db.index.stats()")
	require.NoError(t, err)
}

// Replacing the access scope can hide a projected node as membership changes
// do: a version visible through its document's DERIVE rule is hidden once
// the scope no longer derives versions, and the agent's saved projection of
// it is then refused; the agent can still drop it.
func TestCollectionAccessGDSProjectionRefusedAfterScopeReplacement(t *testing.T) {
	f := newCollectionAccessFixture(t)
	agent := []string{"agent_a"}
	ctx := context.Background()
	_, err := f.admin.Execute(ctx, "MATCH (v:Version) SET v.score = 1", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"v1"}, f.ids(t, agent, "MATCH (n:Version) RETURN n.id"))
	_, err = f.as(t, agent, "MATCH (v:Version) RETURN gds.graph.project('saved', v, null, {sourceNodeProperties: {score: v.score}, targetNodeProperties: null}) AS g")
	require.NoError(t, err)
	result, err := f.admin.Execute(ctx, "MATCH (n:Version {id: 'v1'}) RETURN id(n) AS id", nil)
	require.NoError(t, err)
	id := result.Rows[0][0]
	property := func() (*ExecuteResult, error) {
		return f.admin.Execute(WithPrincipalAccess(ctx, "lib", PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
			return ResolveGraphVisibility(ctx, f.engine, agent, database)
		}}), "RETURN gds.util.nodeProperty('saved', $id, 'score') AS score", map[string]interface{}{"id": id})
	}
	result, err = property()
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Rows[0][0])

	_, err = f.system.Execute(ctx, `CREATE OR REPLACE ACCESS SCOPE collections ON GRAPH lib
	  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id`, nil)
	require.NoError(t, err)
	require.Empty(t, f.ids(t, agent, "MATCH (n:Version) RETURN n.id"))
	_, err = property()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no longer see")
	_, err = f.as(t, agent, "CALL gds.graph.drop('saved') YIELD graphName RETURN graphName")
	require.NoError(t, err)
}
