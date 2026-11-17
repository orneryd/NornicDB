// request_access_mode_public_test.go — Neo4j 5.26 reference behavior for a
// client that asks for READ access (Neo4j Go driver SessionConfig.AccessMode,
// which sends the RUN/BEGIN option "mode"="r"): every statement that updates
// the graph or its schema is refused with
// Neo.ClientError.Statement.AccessMode and changes nothing, while READ requests
// still run reads and WRITE requests still run updates.
package bolt

import (
	"context"
	"fmt"
	"strings"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requestAccessModeDatabase is the one synthetic database these tests use.
const requestAccessModeDatabase = "accessmode"

// accessModeIndexTargets names the index and the label one entry's schema case
// uses, so the entries of one test neither share a name nor declare the same
// index twice over the shared synthetic database.
func accessModeIndexTargets(entry string) (string, string) {
	suffix := strings.NewReplacer(" ", "_", "-", "_").Replace(entry)
	return "access_mode_index_" + suffix, "AccessModeIndexed_" + suffix
}

func accessModeIndexStatement(entry string) string {
	name, label := accessModeIndexTargets(entry)
	return fmt.Sprintf("CREATE INDEX %s FOR (n:%s) ON (n.id)", name, label)
}

// requestAccessModeFixture drives one Bolt server over one isolated in-memory
// database with the official driver.
type requestAccessModeFixture struct {
	driver neo4jdriver.DriverWithContext
}

func newRequestAccessModeFixture(t *testing.T) *requestAccessModeFixture {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	manager := &mockDBManager{
		stores: map[string]storage.Engine{
			requestAccessModeDatabase: storage.NewNamespacedEngine(base, requestAccessModeDatabase),
		},
		defaultDB: requestAccessModeDatabase,
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, manager)
	port := serveBoltIntegrationServer(t, server)

	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	require.NoError(t, driver.VerifyConnectivity(context.Background()))
	return &requestAccessModeFixture{driver: driver}
}

// session opens a session with the requested access mode on the synthetic
// database.
func (f *requestAccessModeFixture) session(t *testing.T, mode neo4jdriver.AccessMode) neo4jdriver.SessionWithContext {
	t.Helper()
	session := f.driver.NewSession(context.Background(), neo4jdriver.SessionConfig{
		AccessMode:   mode,
		DatabaseName: requestAccessModeDatabase,
	})
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	return session
}

// attempt runs one entry's statement on a session of the requested mode and
// closes that session before the caller checks the database's state, so an
// effect check never shares a connection with the request under test.
func (f *requestAccessModeFixture) attempt(t *testing.T, entry accessModeReadEntry, mode neo4jdriver.AccessMode, query string) error {
	t.Helper()
	ctx := context.Background()
	session := f.session(t, mode)
	defer func() { _ = session.Close(ctx) }()
	return entry.attempt(ctx, session, query)
}

// execute runs statements in one WRITE session, which the fixtures and the
// state checks use so they never depend on the mode under test.
func (f *requestAccessModeFixture) execute(t *testing.T, statements ...string) {
	t.Helper()
	ctx := context.Background()
	session := f.session(t, neo4jdriver.AccessModeWrite)
	for _, statement := range statements {
		result, err := session.Run(ctx, statement, nil)
		require.NoError(t, err, statement)
		_, err = result.Consume(ctx)
		require.NoError(t, err, statement)
	}
}

// boolean reads a comparison as its rendered Cypher value through a WRITE
// session, so an effect check never depends on the mode under test.
func (f *requestAccessModeFixture) boolean(t *testing.T, query string) string {
	t.Helper()
	ctx := context.Background()
	session := f.session(t, neo4jdriver.AccessModeWrite)
	result, err := session.Run(ctx, query, nil)
	require.NoError(t, err, query)
	record, err := result.Single(ctx)
	require.NoError(t, err, query)
	value, ok := record.Values[0].(string)
	require.True(t, ok, "verification query must render a string")
	return value
}

// indexNames lists the database's index names through a WRITE session.
func (f *requestAccessModeFixture) indexNames(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	session := f.session(t, neo4jdriver.AccessModeWrite)
	result, err := session.Run(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	records, err := result.Collect(ctx)
	require.NoError(t, err)
	names := make([]string, 0, len(records))
	for _, record := range records {
		if name, found := record.Get("name"); found {
			if rendered, ok := name.(string); ok {
				names = append(names, rendered)
			}
		}
	}
	return names
}

// accessModeUpdateCase is one statement the reference server refuses for a READ
// request and runs for a WRITE request. verify renders whether the statement's
// effect is present, so "false" proves a refused statement changed nothing.
type accessModeUpdateCase struct {
	name   string
	query  string
	setup  []string
	verify string
}

var accessModeUpdateCases = []accessModeUpdateCase{
	{
		name:   "create node",
		query:  "CREATE (:AccessModeCreated {id: 'created'})",
		verify: "MATCH (n:AccessModeCreated) RETURN toString(count(n) > 0) AS v",
	},
	{
		name:   "merge node",
		query:  "MERGE (:AccessModeMerged {id: 'merged'})",
		verify: "MATCH (n:AccessModeMerged) RETURN toString(count(n) > 0) AS v",
	},
	{
		name:   "set property",
		query:  "MATCH (n:AccessModeSet {id: 'target'}) SET n.touched = true",
		setup:  []string{"MERGE (:AccessModeSet {id: 'target'})"},
		verify: "MATCH (n:AccessModeSet {id: 'target'}) RETURN toString(coalesce(n.touched, false)) AS v",
	},
	{
		name:   "delete node",
		query:  "MATCH (n:AccessModeDeleted {id: 'target'}) DELETE n",
		setup:  []string{"MERGE (:AccessModeDeleted {id: 'target'})"},
		verify: "MATCH (n:AccessModeDeleted {id: 'target'}) RETURN toString(count(n) = 0) AS v",
	},
	{
		name:  "create relationship",
		query: "MATCH (a:AccessModePair {id: 'a'}), (b:AccessModePair {id: 'b'}) CREATE (a)-[:ACCESS_MODE_EDGE]->(b)",
		setup: []string{"MERGE (:AccessModePair {id: 'a'})", "MERGE (:AccessModePair {id: 'b'})"},
		verify: "MATCH (:AccessModePair)-[r:ACCESS_MODE_EDGE]->(:AccessModePair) " +
			"RETURN toString(count(r) > 0) AS v",
	},
	{
		name:   "registered write procedure",
		query:  "CALL apoc.cypher.doIt('CREATE (:AccessModeProcedureWrite)', {}) YIELD value RETURN value",
		verify: "MATCH (n:AccessModeProcedureWrite) RETURN toString(count(n) > 0) AS v",
	},
	{
		name:   "dynamic statement text",
		query:  "CALL apoc.cypher.run('CREATE (:AccessModeDynamicWrite)', {}) YIELD value RETURN value",
		verify: "MATCH (n:AccessModeDynamicWrite) RETURN toString(count(n) > 0) AS v",
	},
}

// accessModeReadEntry is one public way a session runs a statement.
type accessModeReadEntry struct {
	name    string
	attempt func(ctx context.Context, session neo4jdriver.SessionWithContext, query string) error
}

var requestAccessModeReadEntries = []accessModeReadEntry{
	{
		name: "auto-commit",
		attempt: func(ctx context.Context, session neo4jdriver.SessionWithContext, query string) error {
			result, err := session.Run(ctx, query, nil)
			if err != nil {
				return err
			}
			_, err = result.Consume(ctx)
			return err
		},
	},
	{
		name: "explicit transaction",
		attempt: func(ctx context.Context, session neo4jdriver.SessionWithContext, query string) error {
			transaction, err := session.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = transaction.Rollback(ctx) }()
			// The transaction's requested mode must outlive a completed read
			// statement and its PULL.
			read, err := transaction.Run(ctx, "MATCH (n:AccessModeSet) RETURN toString(count(n)) AS v", nil)
			if err != nil {
				return err
			}
			if _, err = read.Consume(ctx); err != nil {
				return err
			}
			result, err := transaction.Run(ctx, query, nil)
			if err != nil {
				return err
			}
			if _, err = result.Consume(ctx); err != nil {
				return err
			}
			return transaction.Commit(ctx)
		},
	},
	{
		name: "ExecuteRead",
		attempt: func(ctx context.Context, session neo4jdriver.SessionWithContext, query string) error {
			_, err := session.ExecuteRead(ctx, func(transaction neo4jdriver.ManagedTransaction) (any, error) {
				result, runErr := transaction.Run(ctx, query, nil)
				if runErr != nil {
					return nil, runErr
				}
				return result.Consume(ctx)
			})
			return err
		},
	},
}

var requestAccessModeWriteEntries = []accessModeReadEntry{
	requestAccessModeReadEntries[0],
	requestAccessModeReadEntries[1],
	{
		name: "ExecuteWrite",
		attempt: func(ctx context.Context, session neo4jdriver.SessionWithContext, query string) error {
			_, err := session.ExecuteWrite(ctx, func(transaction neo4jdriver.ManagedTransaction) (any, error) {
				result, runErr := transaction.Run(ctx, query, nil)
				if runErr != nil {
					return nil, runErr
				}
				return result.Consume(ctx)
			})
			return err
		},
	},
}

// forEachParser runs body with each configured Cypher parser, because the
// refusal is decided before either parser executes the statement.
func forEachParser(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			body(t)
		})
	}
}

// TestBoltReadAccessModeRefusesUpdates proves the reference outcome for every
// READ entry point: the update is refused with the AccessMode status and the
// database is unchanged.
func TestBoltReadAccessModeRefusesUpdates(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRequestAccessModeFixture(t)
		for _, entry := range requestAccessModeReadEntries {
			t.Run(entry.name, func(t *testing.T) {
				for _, update := range accessModeUpdateCases {
					t.Run(update.name, func(t *testing.T) {
						fixture.execute(t, update.setup...)
						require.Equal(t, "false", fixture.boolean(t, update.verify), "fixture state before the attempt")

						err := fixture.attempt(t, entry, neo4jdriver.AccessModeRead, update.query)
						var status *neo4jdriver.Neo4jError
						require.ErrorAs(t, err, &status, "a READ request must refuse: %s", update.query)
						require.Equal(t, "Neo.ClientError.Statement.AccessMode", status.Code)
						require.Equal(t, "false", fixture.boolean(t, update.verify), "a refused statement must change nothing")
					})
				}
				t.Run("schema index", func(t *testing.T) {
					index, _ := accessModeIndexTargets(entry.name)
					statement := accessModeIndexStatement(entry.name)
					require.NotContains(t, fixture.indexNames(t), index)

					err := fixture.attempt(t, entry, neo4jdriver.AccessModeRead, statement)
					var status *neo4jdriver.Neo4jError
					require.ErrorAs(t, err, &status, "a READ request must refuse schema DDL: %s", statement)
					require.Equal(t, "Neo.ClientError.Statement.AccessMode", status.Code)
					require.NotContains(t, fixture.indexNames(t), index,
						"a refused schema statement must change nothing")
				})
			})
		}
	})
}

// TestBoltReadAccessModeAllowsReads proves a READ request is a restriction on
// updates only: reads, registered READ procedures and EXPLAIN still run.
func TestBoltReadAccessModeAllowsReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRequestAccessModeFixture(t)
		fixture.execute(t, "MERGE (:AccessModeSeed {id: 'seed'})")
		for _, entry := range requestAccessModeReadEntries {
			t.Run(entry.name, func(t *testing.T) {
				for _, query := range []string{
					"RETURN 1 AS one",
					"MATCH (n:AccessModeSeed) RETURN toString(count(n)) AS v",
					"CALL db.labels() YIELD label RETURN label",
					"EXPLAIN CREATE (:AccessModeExplained)",
					"CALL apoc.cypher.run('RETURN 1 AS x',{}) YIELD value RETURN value",
				} {
					t.Run(query, func(t *testing.T) {
						require.NoError(t, fixture.attempt(t, entry, neo4jdriver.AccessModeRead, query),
							"a READ request must run: %s", query)
					})
				}
			})
		}
	})
}

// TestBoltWriteAccessModeRunsUpdates is the control: the same statements run
// when the client asks for WRITE access.
func TestBoltWriteAccessModeRunsUpdates(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRequestAccessModeFixture(t)
		for _, entry := range requestAccessModeWriteEntries {
			t.Run(entry.name, func(t *testing.T) {
				for _, update := range accessModeUpdateCases {
					t.Run(update.name, func(t *testing.T) {
						fixture.execute(t, update.setup...)
						require.NoError(t, fixture.attempt(t, entry, neo4jdriver.AccessModeWrite, update.query),
							"a WRITE request must run: %s", update.query)
						require.Equal(t, "true", fixture.boolean(t, update.verify), "a WRITE request must run the update")
					})
				}
				t.Run("schema index", func(t *testing.T) {
					index, _ := accessModeIndexTargets(entry.name)
					statement := accessModeIndexStatement(entry.name)
					require.NoError(t, fixture.attempt(t, entry, neo4jdriver.AccessModeWrite, statement),
						"a WRITE request must run: %s", statement)
					require.Contains(t, fixture.indexNames(t), index)
				})
			})
		}
	})
}
