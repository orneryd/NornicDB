package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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

// Public-route fixture: a write-behind database served over Execute, Bolt
// and HTTP, autocommit and explicit.

type wboRoute struct {
	name     string
	explicit bool
}

var wboRoutes = []wboRoute{{"Execute", false}, {"Execute", true}, {"Bolt", false}, {"Bolt", true}, {"HTTP", false}, {"HTTP", true}}

func (r wboRoute) String() string { return fmt.Sprintf("%s/explicit=%t", r.name, r.explicit) }

type wboOutcome struct {
	rows int
	code string
	err  error
}

func (o wboOutcome) transient() bool { return strings.HasPrefix(o.code, "Neo.TransientError.") }

type wboEnv struct {
	t       *testing.T
	db      *nornicdb.DB
	srv     *server.Server
	store   storage.Engine
	badger  *storage.BadgerEngine
	bolt    *bolt.Server
	served  chan error
	boltURI string
	drivers sync.Map
	client  *http.Client
	once    sync.Once
}

func wboOpen(t *testing.T, dir string, flushInterval time.Duration) *wboEnv {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Auth.Enabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.AsyncWritesEnabled = true
	config.Database.AsyncFlushInterval = flushInterval
	config.Database.MVCCLifecycleEnabled = false
	config.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(dir, config)
	require.NoError(t, err)
	serverConfig := server.DefaultConfig()
	serverConfig.Port = 0
	serverConfig.Headless = true
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = config
	srv, err := server.New(db, nil, serverConfig)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	env := &wboEnv{t: t, db: db, srv: srv, client: &http.Client{Timeout: 90 * time.Second}}
	env.store, err = srv.GetDatabaseManager().GetStorage("nornic")
	require.NoError(t, err)
	base := db.GetBaseStorageForManager()
	for {
		wrapper, ok := base.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		base = wrapper.GetInnerEngine()
	}
	env.badger, _ = base.(*storage.BadgerEngine)
	require.NotNil(t, env.badger, "fixture requires Badger storage, got %T", base)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	env.bolt = bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, srv.GetDatabaseManager())
	env.served = make(chan error, 1)
	go func() { env.served <- env.bolt.Serve(listener) }()
	env.boltURI = "bolt://" + listener.Addr().String()
	t.Cleanup(env.close)
	return env
}

func (env *wboEnv) close() {
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

// driverFor keeps one driver per worker: one driver opening connections from
// many goroutines races inside the client.
func (env *wboEnv) driverFor(worker int) neo4j.DriverWithContext {
	if existing, ok := env.drivers.Load(worker); ok {
		return existing.(neo4j.DriverWithContext)
	}
	driver, err := neo4j.NewDriverWithContext(env.boltURI, neo4j.NoAuth())
	require.NoError(env.t, err)
	actual, loaded := env.drivers.LoadOrStore(worker, driver)
	if loaded {
		_ = driver.Close(context.Background())
	}
	return actual.(neo4j.DriverWithContext)
}

func wboExecute(ctx context.Context, exec *cypher.StorageExecutor, explicit bool, query string, params map[string]any) wboOutcome {
	failed := func(err error) wboOutcome {
		if explicit {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
		code, _ := nornicerrors.Neo4jStatus(err)
		return wboOutcome{err: err, code: code}
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
	return wboOutcome{rows: len(result.Rows)}
}

func wboBoltError(err error) wboOutcome {
	var failure *neo4j.Neo4jError
	code := ""
	if errors.As(err, &failure) {
		code = failure.Code
	}
	return wboOutcome{err: err, code: code}
}

func (env *wboEnv) runBolt(ctx context.Context, worker int, explicit bool, query string, params map[string]any) wboOutcome {
	session := env.driverFor(worker).NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	if !explicit {
		result, err := session.Run(ctx, query, params)
		if err != nil {
			return wboBoltError(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return wboBoltError(err)
		}
		return wboOutcome{rows: len(records)}
	}
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return wboBoltError(err)
	}
	defer tx.Close(context.Background())
	result, err := tx.Run(ctx, query, params)
	if err != nil {
		return wboBoltError(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return wboBoltError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return wboBoltError(err)
	}
	return wboOutcome{rows: len(records)}
}

func (env *wboEnv) postHTTP(ctx context.Context, url string, body any) (server.TransactionResponse, error) {
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

func wboHTTPOutcome(response server.TransactionResponse) wboOutcome {
	if len(response.Errors) > 0 {
		failure := response.Errors[0]
		return wboOutcome{err: fmt.Errorf("%s: %s", failure.Code, failure.Message), code: failure.Code}
	}
	rows := 0
	if len(response.Results) > 0 {
		rows = len(response.Results[0].Data)
	}
	return wboOutcome{rows: rows}
}

func (env *wboEnv) runHTTP(ctx context.Context, explicit bool, query string, params map[string]any) wboOutcome {
	base := "http://" + env.srv.Addr()
	body := map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}}
	if !explicit {
		response, err := env.postHTTP(ctx, base+"/db/nornic/tx/commit", body)
		if err != nil {
			return wboOutcome{err: err}
		}
		return wboHTTPOutcome(response)
	}
	opened, err := env.postHTTP(ctx, base+"/db/nornic/tx", body)
	if err != nil {
		return wboOutcome{err: err}
	}
	outcome := wboHTTPOutcome(opened)
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
	committed, err := env.postHTTP(ctx, opened.Commit, map[string]any{"statements": []any{}})
	if err != nil {
		return wboOutcome{err: err}
	}
	final := wboHTTPOutcome(committed)
	if final.err == nil {
		final.rows = outcome.rows
	}
	return final
}

func (env *wboEnv) run(ctx context.Context, worker int, exec *cypher.StorageExecutor, route wboRoute, query string, params map[string]any) wboOutcome {
	switch route.name {
	case "Execute":
		return wboExecute(ctx, exec, route.explicit, query, params)
	case "Bolt":
		return env.runBolt(ctx, worker, route.explicit, query, params)
	default:
		return env.runHTTP(ctx, route.explicit, query, params)
	}
}

func wboMustRun(t *testing.T, exec *cypher.StorageExecutor, query string, params map[string]any) *cypher.ExecuteResult {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result
}

func wboInt(t *testing.T, value any) int64 {
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

// NOR-136 through the public routes: the loser reads the node and stalls in
// RETURN; an autocommit peer is acknowledged into the buffer, which does not
// flush on its own; the loser then commits. The peer's write must survive,
// and the loser either commits on top of it or gets a retryable conflict.
func TestWriteBehindUnflushedPeerPublicRoutes(t *testing.T) {
	env := wboOpen(t, t.TempDir(), time.Hour)
	admin := cypher.NewStorageExecutor(env.store)
	wboMustRun(t, admin, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	staged, resume := make(chan struct{}, 1), make(chan struct{}, 1)
	previous := cypher.PluginFunctionLookup
	cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "test.wboAwait" {
			return func(value string) string {
				staged <- struct{}{}
				<-resume
				return value
			}, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { cypher.PluginFunctionLookup = previous })
	for i, route := range wboRoutes {
		t.Run(route.String(), func(t *testing.T) {
			id := fmt.Sprintf("doc-%d", i)
			wboMustRun(t, admin, "CREATE (:Doc {id:$id, note:'seed', other:'seed'})", map[string]any{"id": id})
			require.NoError(t, env.badger.FlushWriteBehind())
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			finished := make(chan wboOutcome, 1)
			go func() {
				finished <- env.run(ctx, i, cypher.NewStorageExecutor(env.store), route,
					"MATCH (n:Doc {id:$id}) SET n.note='loser' RETURN test.wboAwait(n.id) AS observed", map[string]any{"id": id})
			}()
			select {
			case <-staged:
			case outcome := <-finished:
				t.Fatalf("the loser finished before its barrier: %+v", outcome)
			case <-ctx.Done():
				t.Fatal("the loser never reached its barrier")
			}
			wboMustRun(t, admin, "MATCH (n:Doc {id:$id}) SET n.other='peer'", map[string]any{"id": id})
			resume <- struct{}{}
			outcome := <-finished
			require.NoError(t, env.badger.FlushWriteBehind())
			result := wboMustRun(t, admin, "MATCH (n:Doc {id:$id}) RETURN n.note, n.other", map[string]any{"id": id})
			t.Logf("loser: code=%q err=%v; stored %v", outcome.code, outcome.err, result.Rows)
			require.Len(t, result.Rows, 1)
			require.Equal(t, "peer", result.Rows[0][1], "the peer's acknowledged write was lost")
			if outcome.err == nil {
				require.Equal(t, "loser", result.Rows[0][0], "the loser's acknowledged write was lost")
			} else {
				require.True(t, outcome.transient(), "the loser must get a retryable conflict: %v", outcome.err)
			}
		})
	}
}

// NOR-136 / NOR-138 through the public routes: concurrent counter increments
// on every route race with label changes while the buffer flushes. Every
// acknowledged increment survives, label counts match a recount, and the
// database restarts cleanly with the same data.
func TestWriteBehindConcurrentPublicWritesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	env := wboOpen(t, dir, 2*time.Millisecond)
	admin := cypher.NewStorageExecutor(env.store)
	ids := []string{"s0", "s1", "s2"}
	for _, id := range ids {
		wboMustRun(t, admin, "CREATE (:Doc {id:$id, cnt:0})", map[string]any{"id": id})
	}
	forms := []string{
		"MATCH (n:Doc {id:$id}) SET n.cnt = n.cnt + 1 RETURN n.id",
		"MATCH (n:Doc {id:$id}) SET n:Tagged, n.cnt = n.cnt + 1 RETURN n.id",
		"MATCH (n:Doc {id:$id}) REMOVE n:Tagged SET n.cnt = n.cnt + 1 RETURN n.id",
		"MATCH (n:Doc {id:$id}) SET n += {cnt: n.cnt + 1, note: $note} RETURN n.id",
	}
	var successes [3]atomic.Int64
	var transient atomic.Int64
	var wg sync.WaitGroup
	const workers, iterations = 6, 60
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker)*7919 + time.Now().UnixNano()))
			exec := cypher.NewStorageExecutor(env.store)
			for i := 0; i < iterations; i++ {
				which := rng.Intn(len(ids))
				route := wboRoutes[rng.Intn(len(wboRoutes))]
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				outcome := env.run(ctx, worker, exec, route, forms[rng.Intn(len(forms))], map[string]any{"id": ids[which], "note": fmt.Sprintf("w%d-%d", worker, i)})
				cancel()
				switch {
				case outcome.err == nil && outcome.rows == 1:
					successes[which].Add(1)
				case outcome.err == nil:
					t.Errorf("%s matched %d rows", route, outcome.rows)
				case outcome.transient():
					transient.Add(1)
				default:
					t.Errorf("%s: unexpected failure %s %v", route, outcome.code, outcome.err)
				}
			}
		}(worker)
	}
	wg.Wait()
	t.Logf("transient=%d successes=%v", transient.Load(), []int64{successes[0].Load(), successes[1].Load(), successes[2].Load()})

	check := func(stage string, exec *cypher.StorageExecutor) {
		t.Helper()
		for i, id := range ids {
			result := wboMustRun(t, exec, "MATCH (n:Doc {id:$id}) RETURN n.cnt", map[string]any{"id": id})
			require.Len(t, result.Rows, 1, "%s: %s", stage, id)
			require.Equal(t, successes[i].Load(), wboInt(t, result.Rows[0][0]), "%s: %s lost acknowledged increments", stage, id)
		}
		for _, label := range []string{"Doc", "Tagged"} {
			counted := wboMustRun(t, exec, "MATCH (n:"+label+") RETURN count(n)", nil)
			scanned := wboMustRun(t, exec, "MATCH (n) WHERE $label IN labels(n) RETURN count(n)", map[string]any{"label": label})
			require.Equal(t, wboInt(t, scanned.Rows[0][0]), wboInt(t, counted.Rows[0][0]), "%s: %s count must match a recount", stage, label)
		}
	}
	require.NoError(t, env.badger.FlushWriteBehind())
	check("flushed", admin)
	env.close()
	restarted := wboOpen(t, dir, 2*time.Millisecond)
	check("restarted", cypher.NewStorageExecutor(restarted.store))
}
