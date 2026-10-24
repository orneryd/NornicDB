package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public-route fixture: one database served over Execute, Bolt and HTTP, each
// in autocommit and explicit transactions, in the default synchronous mode.

type indexRoute struct {
	name     string
	explicit bool
}

func (r indexRoute) String() string {
	if r.explicit {
		return r.name + "/explicit"
	}
	return r.name + "/autocommit"
}

var indexRoutes = []indexRoute{{"Execute", false}, {"Execute", true}, {"Bolt", false}, {"Bolt", true}, {"HTTP", false}, {"HTTP", true}}

type indexOutcome struct {
	rows [][]any
	code string
	err  error
}

type indexEnv struct {
	t       *testing.T
	db      *nornicdb.DB
	srv     *server.Server
	store   storage.Engine
	bolt    *bolt.Server
	served  chan error
	boltURI string
	drivers sync.Map
	client  *http.Client
	once    sync.Once
}

func openIndexEnv(t *testing.T) *indexEnv {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Auth.Enabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.AsyncWritesEnabled = false
	config.Database.MVCCLifecycleEnabled = false
	config.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	serverConfig := server.DefaultConfig()
	serverConfig.Port = 0
	serverConfig.Headless = true
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = config
	srv, err := server.New(db, nil, serverConfig)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	env := &indexEnv{t: t, db: db, srv: srv, client: &http.Client{Timeout: 60 * time.Second}}
	env.store, err = srv.GetDatabaseManager().GetStorage("nornic")
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	env.bolt = bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, srv.GetDatabaseManager())
	env.served = make(chan error, 1)
	go func() { env.served <- env.bolt.Serve(listener) }()
	env.boltURI = "bolt://" + listener.Addr().String()
	t.Cleanup(env.close)
	return env
}

func (env *indexEnv) close() {
	env.once.Do(func() {
		env.drivers.Range(func(_, value any) bool {
			_ = value.(neo4j.DriverWithContext).Close(context.Background())
			return true
		})
		_ = env.bolt.Close()
		<-env.served
		require.NoError(env.t, env.srv.Stop(context.Background()))
		require.NoError(env.t, env.db.Close())
	})
}

// driverFor keeps one driver per client: the Go driver's connector is not
// safe to open connections from many goroutines at once.
func (env *indexEnv) driverFor(client any) neo4j.DriverWithContext {
	if existing, ok := env.drivers.Load(client); ok {
		return existing.(neo4j.DriverWithContext)
	}
	driver, err := neo4j.NewDriverWithContext(env.boltURI, neo4j.NoAuth())
	require.NoError(env.t, err)
	actual, loaded := env.drivers.LoadOrStore(client, driver)
	if loaded {
		_ = driver.Close(context.Background())
	}
	return actual.(neo4j.DriverWithContext)
}

// run executes query on route; exec is the client's own executor (Execute)
// and its identity (Bolt driver).
func (env *indexEnv) run(ctx context.Context, exec *cypher.StorageExecutor, route indexRoute, query string, params map[string]any) indexOutcome {
	switch route.name {
	case "Execute":
		return runIndexExecute(ctx, exec, route.explicit, query, params)
	case "Bolt":
		return env.runBolt(ctx, exec, route.explicit, query, params)
	default:
		return env.runHTTP(ctx, route.explicit, query, params)
	}
}

func runIndexExecute(ctx context.Context, exec *cypher.StorageExecutor, explicit bool, query string, params map[string]any) indexOutcome {
	failed := func(err error) indexOutcome {
		if explicit {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
		code, _ := nornicerrors.Neo4jStatus(err)
		return indexOutcome{err: err, code: code}
	}
	if explicit {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			return failed(err)
		}
	}
	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		return failed(err)
	}
	if explicit {
		if _, err := exec.Execute(ctx, "COMMIT", nil); err != nil {
			return failed(err)
		}
	}
	return indexOutcome{rows: result.Rows}
}

func (env *indexEnv) runBolt(ctx context.Context, client any, explicit bool, query string, params map[string]any) indexOutcome {
	failed := func(err error) indexOutcome {
		var failure *neo4j.Neo4jError
		code := ""
		if errors.As(err, &failure) {
			code = failure.Code
		}
		return indexOutcome{err: err, code: code}
	}
	session := env.driverFor(client).NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	var records []*neo4j.Record
	if explicit {
		tx, err := session.BeginTransaction(ctx)
		if err != nil {
			return failed(err)
		}
		defer tx.Close(context.Background())
		result, err := tx.Run(ctx, query, params)
		if err == nil {
			records, err = result.Collect(ctx)
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			return failed(err)
		}
	} else {
		result, err := session.Run(ctx, query, params)
		if err == nil {
			records, err = result.Collect(ctx)
		}
		if err != nil {
			return failed(err)
		}
	}
	rows := make([][]any, len(records))
	for i, record := range records {
		rows[i] = record.Values
	}
	return indexOutcome{rows: rows}
}

func (env *indexEnv) postHTTP(ctx context.Context, url string, body any) (server.TransactionResponse, error) {
	var decoded server.TransactionResponse
	payload, err := json.Marshal(body)
	if err != nil {
		return decoded, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return decoded, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := env.client.Do(request)
	if err != nil {
		return decoded, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return decoded, fmt.Errorf("http %d: decode: %w", response.StatusCode, err)
	}
	return decoded, nil
}

func indexHTTPOutcome(response server.TransactionResponse, err error) indexOutcome {
	if err != nil {
		return indexOutcome{err: err}
	}
	if len(response.Errors) > 0 {
		failure := response.Errors[0]
		return indexOutcome{err: fmt.Errorf("%s: %s", failure.Code, failure.Message), code: failure.Code}
	}
	var rows [][]any
	if len(response.Results) > 0 {
		for _, row := range response.Results[0].Data {
			rows = append(rows, row.Row)
		}
	}
	return indexOutcome{rows: rows}
}

func (env *indexEnv) runHTTP(ctx context.Context, explicit bool, query string, params map[string]any) indexOutcome {
	base := "http://" + env.srv.Addr() + "/db/nornic/tx"
	body := map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}}
	if !explicit {
		return indexHTTPOutcome(env.postHTTP(ctx, base+"/commit", body))
	}
	opened, err := env.postHTTP(ctx, base, body)
	outcome := indexHTTPOutcome(opened, err)
	if outcome.err != nil || opened.Commit == "" {
		if opened.Commit != "" {
			request, _ := http.NewRequest(http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"), nil)
			if reply, err := env.client.Do(request); err == nil {
				reply.Body.Close()
			}
		}
		if outcome.err == nil {
			outcome.err = errors.New("explicit HTTP transaction returned no commit URL")
		}
		return outcome
	}
	committed := indexHTTPOutcome(env.postHTTP(ctx, opened.Commit, map[string]any{"statements": []any{}}))
	if committed.err != nil {
		return committed
	}
	return outcome
}

func mustRunIndex(t *testing.T, exec *cypher.StorageExecutor, query string, params map[string]any) [][]any {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

func indexInt(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	t.Fatalf("not an integer: %T %v", value, value)
	return 0
}

// sameStoredNode reports whether a storage-level node ID (namespace-prefixed)
// names the database-level node id.
func sameStoredNode(stored storage.NodeID, id storage.NodeID) bool {
	return stored == id || strings.HasSuffix(string(stored), ":"+string(id))
}

var indexKinds = []struct {
	name   string
	ddl    string
	reader string // a seek on the counter node's key, run while its index entries are replaced
}{
	{"unique-constraint", "CREATE CONSTRAINT doc_id_unique FOR (n:Doc) REQUIRE n.id IS UNIQUE", "MATCH (n:Doc {id:$id}) RETURN n.c"},
	{"property-index", "CREATE INDEX doc_id FOR (n:Doc) ON (n.id)", "MATCH (n:Doc {id:$id}) RETURN n.c"},
	{"composite-index", "CREATE INDEX doc_tenant_id FOR (n:Doc) ON (n.tenant, n.id)", "MERGE (n:Doc {tenant:'t', id:$id}) RETURN n.c"},
}

// TestNodeIndexEntriesStayVisibleDuringUnchangedKeyUpdate pauses a counter
// update after its commit, inside its index maintenance, and seeks the node
// by its unchanged key on every public route. The seek must find the one
// existing node: before the fix the maintenance removed the entry, unlocked
// and re-inserted it, and a seek in between matched nothing (a MERGE created
// a duplicate).
func TestNodeIndexEntriesStayVisibleDuringUnchangedKeyUpdate(t *testing.T) {
	for _, kind := range indexKinds {
		for _, route := range indexRoutes {
			t.Run(kind.name+"/"+route.String(), func(t *testing.T) {
				env := openIndexEnv(t)
				admin := cypher.NewStorageExecutor(env.store)
				mustRunIndex(t, admin, kind.ddl, nil)
				mustRunIndex(t, admin, "CREATE (:Doc {id:'stable', tenant:'t', c:0})", nil)
				nodes, err := env.store.GetNodesByLabel("Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 1)
				target := nodes[0].ID

				paused, resume := make(chan struct{}), make(chan struct{})
				var pauseOnce, resumeOnce sync.Once
				release := func() { resumeOnce.Do(func() { close(resume) }) }
				defer release()
				restore := storage.SetNodeIndexEntriesReplacedHookForTesting(func(id storage.NodeID) {
					if sameStoredNode(id, target) {
						pauseOnce.Do(func() {
							close(paused)
							<-resume
						})
					}
				})
				defer restore()

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				written := make(chan indexOutcome, 1)
				go func() {
					written <- runIndexExecute(ctx, cypher.NewStorageExecutor(env.store), false, "MATCH (n:Doc {id:'stable'}) SET n.c = n.c + 1 RETURN n.c", nil)
				}()
				select {
				case <-paused:
				case <-ctx.Done():
					t.Fatal("the counter update never reached index maintenance")
				}
				read := env.run(ctx, cypher.NewStorageExecutor(env.store), route, kind.reader, map[string]any{"id": "stable"})
				release()
				writer := <-written
				require.NoError(t, writer.err)
				require.Len(t, writer.rows, 1)
				require.NoError(t, read.err)
				require.Len(t, read.rows, 1, "a seek on an unchanged key during index maintenance must find the node")
				require.Equal(t, [][]any{{int64(1), int64(1)}}, mustRunIndex(t, admin, "MATCH (n:Doc) RETURN count(n), sum(n.c)", nil),
					"exactly the one counter node, incremented once")
			})
		}
	}
}
