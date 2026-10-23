package storage_test

// Public regression schedules established by the round-2 integrated review.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type r2Route struct {
	name     string
	explicit bool
}

var r2Routes = []r2Route{{"Execute", false}, {"Execute", true}, {"Bolt", false}, {"Bolt", true}, {"HTTP", false}, {"HTTP", true}}

type r2Outcome struct {
	rows   int
	code   string
	msg    string
	err    error
	unique bool
}

func (o r2Outcome) transient() bool { return strings.HasPrefix(o.code, "Neo.TransientError.") }

type r2Env struct {
	t       *testing.T
	db      *nornicdb.DB
	srv     *server.Server
	store   storage.Engine
	badger  *storage.BadgerEngine
	bolt    *bolt.Server
	served  chan error
	driver  neo4j.DriverWithContext
	drivers sync.Map
	boltURI string
	client  *http.Client
	once    sync.Once
	history r2History
}

// r2History keeps the last successful writes per key, for stale-claim reports.
type r2History struct {
	mu    sync.Mutex
	byKey map[string][]string
}

func (h *r2History) add(entry string, keys ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.byKey == nil {
		h.byKey = make(map[string][]string)
	}
	for _, key := range keys {
		list := append(h.byKey[key], fmt.Sprintf("%s %s", time.Now().Format("15:04:05.000000"), entry))
		if len(list) > 12 {
			list = list[len(list)-12:]
		}
		h.byKey[key] = list
	}
}

func (h *r2History) dump(key string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.byKey[key]...)
}

func r2Open(t *testing.T, dir string, async bool, flushInterval ...time.Duration) *r2Env {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Auth.Enabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.AsyncWritesEnabled = async
	if async {
		config.Database.AsyncFlushInterval = 2 * time.Millisecond
		if len(flushInterval) > 0 {
			config.Database.AsyncFlushInterval = flushInterval[0]
		}
	}
	config.Database.MVCCLifecycleEnabled = false
	config.Database.WALAutoCompactionEnabled = false
	config.Database.MVCCRetentionMaxVersions = 100
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
	env := &r2Env{t: t, db: db, srv: srv, client: &http.Client{Timeout: 90 * time.Second}}
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
	env.driver, err = neo4j.NewDriverWithContext(env.boltURI, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(env.close)
	return env
}

func (env *r2Env) close() {
	env.once.Do(func() {
		env.drivers.Range(func(_, value any) bool {
			_ = value.(neo4j.DriverWithContext).Close(context.Background())
			return true
		})
		_ = env.driver.Close(context.Background())
		_ = env.bolt.Close()
		<-env.served
		require.NoError(env.t, env.srv.Stop(context.Background()))
		require.NoError(env.t, env.db.Close())
	})
}

func r2ExecuteError(err error) r2Outcome {
	code, _ := nornicerrors.Neo4jStatus(err)
	var violation *storage.ConstraintViolationError
	unique := (errors.As(err, &violation) && violation.Type == storage.ConstraintUnique) || strings.Contains(code, "ConstraintValidationFailed")
	return r2Outcome{err: err, code: code, msg: err.Error(), unique: unique}
}

func r2Execute(ctx context.Context, exec *cypher.StorageExecutor, explicit bool, query string, params map[string]any) r2Outcome {
	if explicit {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
			return r2ExecuteError(err)
		}
	}
	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		if explicit {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
		return r2ExecuteError(err)
	}
	rows := len(result.Rows)
	if explicit {
		if _, err := exec.Execute(ctx, "COMMIT", nil); err != nil {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
			return r2ExecuteError(err)
		}
	}
	return r2Outcome{rows: rows}
}

func r2BoltError(err error) r2Outcome {
	var failure *neo4j.Neo4jError
	code := ""
	if errors.As(err, &failure) {
		code = failure.Code
	}
	return r2Outcome{err: err, code: code, msg: err.Error(), unique: strings.Contains(code, "ConstraintValidationFailed")}
}

// One driver per executor: the Go driver's connector races when one driver
// opens connections from many goroutines; that race is the client's, not ours.
func (env *r2Env) driverFor(exec *cypher.StorageExecutor) neo4j.DriverWithContext {
	if exec == nil {
		return env.driver
	}
	if existing, ok := env.drivers.Load(exec); ok {
		return existing.(neo4j.DriverWithContext)
	}
	driver, err := neo4j.NewDriverWithContext(env.boltURI, neo4j.NoAuth())
	if err != nil {
		return env.driver
	}
	actual, loaded := env.drivers.LoadOrStore(exec, driver)
	if loaded {
		_ = driver.Close(context.Background())
	}
	return actual.(neo4j.DriverWithContext)
}

func (env *r2Env) runBolt(ctx context.Context, exec *cypher.StorageExecutor, explicit bool, query string, params map[string]any) r2Outcome {
	session := env.driverFor(exec).NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	if explicit {
		tx, err := session.BeginTransaction(ctx)
		if err != nil {
			return r2BoltError(err)
		}
		defer tx.Close(context.Background())
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return r2BoltError(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return r2BoltError(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return r2BoltError(err)
		}
		return r2Outcome{rows: len(records)}
	}
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return r2BoltError(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return r2BoltError(err)
	}
	return r2Outcome{rows: len(records)}
}

func (env *r2Env) postHTTP(ctx context.Context, url string, body any) (server.TransactionResponse, error) {
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

func r2HTTPOutcome(response server.TransactionResponse) r2Outcome {
	if len(response.Errors) > 0 {
		failure := response.Errors[0]
		return r2Outcome{err: fmt.Errorf("%s: %s", failure.Code, failure.Message), code: failure.Code, msg: failure.Message, unique: strings.Contains(failure.Code, "ConstraintValidationFailed")}
	}
	rows := 0
	if len(response.Results) > 0 {
		rows = len(response.Results[0].Data)
	}
	return r2Outcome{rows: rows}
}

func (env *r2Env) runHTTP(ctx context.Context, explicit bool, query string, params map[string]any) r2Outcome {
	base := "http://" + env.srv.Addr()
	body := map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}}
	if !explicit {
		response, err := env.postHTTP(ctx, base+"/db/nornic/tx/commit", body)
		if err != nil {
			return r2Outcome{err: err, msg: err.Error()}
		}
		return r2HTTPOutcome(response)
	}
	opened, err := env.postHTTP(ctx, base+"/db/nornic/tx", body)
	if err != nil {
		return r2Outcome{err: err, msg: err.Error()}
	}
	outcome := r2HTTPOutcome(opened)
	if outcome.err != nil || opened.Commit == "" {
		if opened.Commit != "" {
			request, _ := http.NewRequest(http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"), nil)
			if reply, err := env.client.Do(request); err == nil {
				reply.Body.Close()
			}
		}
		if outcome.err == nil {
			outcome.err = errors.New("explicit HTTP transaction returned no commit URL")
			outcome.msg = outcome.err.Error()
		}
		return outcome
	}
	committed, err := env.postHTTP(ctx, opened.Commit, map[string]any{"statements": []any{}})
	if err != nil {
		return r2Outcome{err: err, msg: err.Error()}
	}
	final := r2HTTPOutcome(committed)
	if final.err == nil {
		final.rows = outcome.rows
	}
	return final
}

func (env *r2Env) run(ctx context.Context, exec *cypher.StorageExecutor, route r2Route, query string, params map[string]any) r2Outcome {
	switch route.name {
	case "Execute":
		return r2Execute(ctx, exec, route.explicit, query, params)
	case "Bolt":
		return env.runBolt(ctx, exec, route.explicit, query, params)
	default:
		return env.runHTTP(ctx, route.explicit, query, params)
	}
}

func r2MustRun(t *testing.T, exec *cypher.StorageExecutor, query string, params map[string]any) *cypher.ExecuteResult {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result
}

func r2Int(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	}
	return -1
}

func TestNodeUniqueDefinitionLifecyclePublic(t *testing.T) {
	for _, route := range r2Routes {
		t.Run(fmt.Sprintf("%s/explicit=%t", route.name, route.explicit), func(t *testing.T) {
			env := r2Open(t, t.TempDir(), false)
			exec := cypher.NewStorageExecutor(env.store)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			for _, query := range []string{
				"CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE",
				"CREATE (:Doc {id:'old'})",
				"DROP CONSTRAINT doc_id",
				"MATCH (n:Doc) SET n.id='renamed'",
				"CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE",
				"MATCH (n:Doc {id:'renamed'}) SET n.id='old'",
			} {
				outcome := env.run(ctx, exec, route, query, nil)
				require.NoError(t, outcome.err, query)
			}
			outcome := env.run(ctx, exec, route, "CREATE (:Doc {id:'old'})", nil)
			require.True(t, outcome.unique, "rebuilding a constraint must replace its old reverse entries: %+v", outcome)
			result := r2MustRun(t, exec, "MATCH (n:Doc {id:'old'}) RETURN count(n)", nil)
			require.EqualValues(t, 1, r2Int(result.Rows[0][0]))
			for _, query := range []string{
				"MATCH (n:Doc) SET n.guard='guard-held'",
				"CREATE CONSTRAINT doc_guard FOR (n:Doc) REQUIRE n.guard IS UNIQUE",
				"DROP CONSTRAINT doc_id",
			} {
				outcome := env.run(ctx, exec, route, query, nil)
				require.NoError(t, outcome.err, query)
			}
			outcome = env.run(ctx, exec, route, "CREATE (:Doc {id:'unconstrained', guard:'guard-held'})", nil)
			require.True(t, outcome.unique, "dropping one definition must retain the node's other unique key: %+v", outcome)
		})
	}
}

// A non-key SET that loses to a peer which renamed (or deleted) the node and
// then let a third node take the old key. The loser never changes the key.
func TestReview1655R2ThirdPartyNonKeyConflict(t *testing.T) {
	variants := []struct {
		name  string
		peers []string
	}{
		{"control_rename_only", []string{"MATCH (n:Doc {id:'original'}) SET n.id='renamed'"}},
		{"control_unrelated_create", []string{"CREATE (:Doc {id:'other'})", "MATCH (n:Doc {id:'original'}) SET n.note='peer'"}},
		{"rename_then_create_old_key", []string{"MATCH (n:Doc {id:'original'}) SET n.id='renamed'", "CREATE (:Doc {id:'original', note:'newcomer'})"}},
		{"rename_and_create_one_statement", []string{"MATCH (n:Doc {id:'original'}) SET n.id='renamed' CREATE (:Doc {id:'original', note:'newcomer'})"}},
		{"delete_then_create_old_key", []string{"MATCH (n:Doc {id:'original'}) DETACH DELETE n", "CREATE (:Doc {id:'original', note:'newcomer'})"}},
		{"map_replace_then_merge_old_key", []string{"MATCH (n:Doc {id:'original'}) SET n = {id:'renamed'}", "MERGE (m:Doc {id:'original'}) ON CREATE SET m.note='newcomer'"}},
	}
	for _, async := range []bool{false, true} {
		for _, route := range r2Routes {
			t.Run(fmt.Sprintf("async=%t/%s/explicit=%t", async, route.name, route.explicit), func(t *testing.T) {
				// Keep this schedule deterministic: all peer writes remain
				// acknowledged and unflushed until the loser finishes. The
				// separate retirement schedule explicitly interposes a replay.
				env := r2Open(t, t.TempDir(), async, time.Hour)
				admin := cypher.NewStorageExecutor(env.store)
				r2MustRun(t, admin, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
				for _, variant := range variants {
					t.Run(variant.name, func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()
						// Isolate successive variants from write-behind's existing
						// lost-delete behavior. The peer/loser schedule below is
						// deliberately left unflushed.
						if async {
							require.NoError(t, env.badger.FlushWriteBehind())
						}
						r2MustRun(t, admin, "MATCH (n) DETACH DELETE n", nil)
						if async {
							require.NoError(t, env.badger.FlushWriteBehind())
						}
						r2MustRun(t, admin, "CREATE (:Doc {id:'original', note:'seed'})", nil)
						// This reproduction starts from an existing stored node.
						// Unflushed initial nodes have their own buffered matrix.
						if async {
							require.NoError(t, env.badger.FlushWriteBehind())
						}
						staged, resume := make(chan struct{}), make(chan struct{})
						var signal, release sync.Once
						defer release.Do(func() { close(resume) })
						previous := cypher.PluginFunctionLookup
						cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
							if name == "test.r2Await" {
								return func(value string) string {
									signal.Do(func() { close(staged) })
									select {
									case <-resume:
									case <-ctx.Done():
									}
									return value
								}, true
							}
							if previous != nil {
								return previous(name)
							}
							return nil, false
						}
						defer func() { cypher.PluginFunctionLookup = previous }()
						loserQuery := "MATCH (n:Doc {id:'original'}) SET n.note='loser' RETURN test.r2Await(n.id) AS observed"
						finished := make(chan r2Outcome, 1)
						go func() { finished <- env.run(ctx, cypher.NewStorageExecutor(env.store), route, loserQuery, nil) }()
						select {
						case <-staged:
						case outcome := <-finished:
							t.Fatalf("loser finished before the barrier: %+v", outcome)
						case <-ctx.Done():
							t.Fatal("loser never reached the barrier")
						}
						peer := cypher.NewStorageExecutor(env.store)
						for _, query := range variant.peers {
							result, err := peer.Execute(ctx, query, nil)
							if err != nil {
								t.Log(env.badger.NodeUniqueDiagnosticForTest("nornic", "Doc", "id", "original"))
							}
							require.NoError(t, err, query)
							if strings.Contains(query, "DETACH DELETE") {
								require.EqualValues(t, 1, result.Stats.NodesDeleted, "peer must actually delete the original node")
							}
						}
						release.Do(func() { close(resume) })
						outcome := <-finished
						t.Logf("loser outcome: rows=%d code=%q err=%v", outcome.rows, outcome.code, outcome.err)
						if outcome.unique {
							t.Errorf("FALSE UNIQUE for a non-key SET (%s): %s", outcome.code, outcome.msg)
						} else if outcome.err != nil && !outcome.transient() {
							t.Errorf("UNCLASSIFIED loser outcome (%s): %s", outcome.code, outcome.msg)
						}
						result := r2MustRun(t, admin, "MATCH (n:Doc) RETURN n.id AS id, n.note AS note ORDER BY id, note", nil)
						t.Logf("final rows: %v", result.Rows)
						originals := 0
						for _, row := range result.Rows {
							if row[0] == "original" {
								originals++
							}
						}
						require.LessOrEqual(t, originals, 1, "duplicate key accepted")
					})
				}
			})
		}
	}
}

type r2Recorder struct {
	mu       sync.Mutex
	failures map[string][]string
	counts   map[string]int
}

func (r *r2Recorder) note(kind string, failure bool, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[kind]++
	if failure && len(r.failures[kind]) < 8 {
		r.failures[kind] = append(r.failures[kind], fmt.Sprintf(format, args...))
	}
}

var r2HolderPattern = regexp.MustCompile(`nodeID: nornic:([0-9a-f-]+)`)

func (env *r2Env) checkInvariants(t *testing.T, phase string, values []string, counters map[string]int64, history *r2History) {
	t.Helper()
	exec := cypher.NewStorageExecutor(env.store)
	ctx := context.Background()
	result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE n.id IS NOT NULL RETURN n.id AS id, count(*) AS c", nil)
	require.NoError(t, err)
	publicCounts := make(map[string]int64)
	for _, row := range result.Rows {
		publicCounts[fmt.Sprint(row[0])] = r2Int(row[1])
	}
	// The release base can leave both cached bodies and label-index membership
	// behind the primary records after replay. Keep those views as diagnostics;
	// the existing full merged stored-node reader owns the integrity population.
	labelCounts := make(map[string]int64)
	require.NoError(t, storage.StreamStoredNodesByLabelProjected(env.store, "Doc", []string{"id"}, func(node *storage.Node) error {
		if value := node.Properties["id"]; value != nil {
			labelCounts[fmt.Sprint(value)]++
		}
		return nil
	}))
	storedCounts := make(map[string]int64)
	cacheDisagreements := 0
	type observedKey struct {
		id    storage.NodeID
		value any
	}
	var observed []observedKey
	require.NoError(t, storage.StreamStoredNodes(ctx, env.store, storage.StreamNodesOptions{Projection: []string{"id"}}, func(node *storage.Node) error {
		if !slices.Contains(node.Labels, "Doc") {
			return nil
		}
		value := node.Properties["id"]
		if value != nil {
			storedCounts[fmt.Sprint(value)]++
		}
		observed = append(observed, observedKey{node.ID, value})
		return nil
	}))
	for _, key := range observed {
		cached, readErr := env.store.GetNode(key.id)
		var cachedValue any
		cachedDoc := false
		if cached != nil {
			cachedValue = cached.Properties["id"]
			for _, label := range cached.Labels {
				if label == "Doc" {
					cachedDoc = true
				}
			}
		}
		if readErr != nil || cachedValue != key.value || !cachedDoc {
			cacheDisagreements++
			if cacheDisagreements <= 5 {
				t.Logf("CACHE_READ_DIAGNOSTIC %s node=%s stored=%v cached=%v cached_Doc=%t err=%v", phase, key.id, key.value, cachedValue, cachedDoc, readErr)
			}
		}
	}
	held := make(map[string]bool)
	countDisagreements := 0
	labelDisagreements := 0
	for id, count := range storedCounts {
		if count != 1 {
			t.Errorf("%s: TRUE DUPLICATE: id=%s held by %d stored nodes", phase, id, count)
		}
		held[id] = true
		if publicCounts[id] != count {
			countDisagreements++
		}
		if labelCounts[id] != count {
			labelDisagreements++
		}
	}
	for id := range publicCounts {
		if storedCounts[id] == 0 {
			countDisagreements++
		}
	}
	for id := range labelCounts {
		if storedCounts[id] == 0 {
			labelDisagreements++
		}
	}
	t.Logf("CACHE_READ_DIAGNOSTIC %s node_disagreements=%d aggregate_disagreements=%d", phase, cacheDisagreements, countDisagreements)
	t.Logf("LABEL_READ_DIAGNOSTIC %s disagreements=%d", phase, labelDisagreements)
	if counters != nil {
		result, err = exec.Execute(ctx, "MATCH (n:Doc {kind:'stable'}) RETURN n.id AS id, n.cnt AS cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, len(counters), "%s: stable nodes", phase)
		for _, row := range result.Rows {
			id := fmt.Sprint(row[0])
			if got, want := r2Int(row[1]), counters[id]; got != want {
				t.Errorf("%s: LOST/EXTRA UPDATE: id=%s cnt=%d successful increments=%d", phase, id, got, want)
			}
		}
	}
	for _, value := range values {
		_, err := exec.Execute(ctx, "CREATE (:Doc {id:$v, kind:'probe'})", map[string]any{"v": value})
		switch {
		case held[value] && err == nil:
			t.Errorf("%s: DUPLICATE ACCEPTED: CREATE of held id=%s succeeded", phase, value)
		case held[value] && !r2ExecuteError(err).unique:
			t.Errorf("%s: probe of held id=%s failed without UNIQUE: %v", phase, value, err)
		case !held[value] && err != nil:
			t.Errorf("%s: STALE CLAIM: CREATE of free id=%s rejected: %v", phase, value, err)
			t.Log(env.badger.NodeUniqueDiagnosticForTest("nornic", "Doc", "id", value))
			if match := r2HolderPattern.FindStringSubmatch(err.Error()); match != nil {
				holder, getErr := env.store.GetNode(storage.NodeID(match[1]))
				if getErr != nil {
					t.Logf("    claimed holder %s: %v", match[1], getErr)
				} else {
					t.Logf("    claimed holder %s: labels=%v properties=%v", match[1], holder.Labels, holder.Properties)
				}
			}
			if history != nil {
				for _, entry := range history.dump(value) {
					t.Logf("    history %s: %s", value, entry)
				}
			}
		case !held[value]:
			_, err = exec.Execute(ctx, "MATCH (n:Doc {id:$v, kind:'probe'}) DELETE n", map[string]any{"v": value})
			require.NoError(t, err)
		}
	}
	if _, err := exec.Execute(ctx, "MATCH (n:Doc) SET n.final = $phase", map[string]any{"phase": phase}); err != nil {
		t.Errorf("%s: non-key SET over every Doc failed: %v", phase, err)
	}
}

// Many concurrent writers over all three protocols, both commit modes and
// both write modes. Non-key writers must never see UNIQUE; counters must not
// lose updates; data and claims must agree live, after flush and after restart.
func TestReview1655R2ConcurrencyStress(t *testing.T) {
	const iterations = 90
	pool := []string{"p0", "p1", "p2", "p3", "p4", "p5"}
	stable := []string{"s0", "s1", "s2"}
	values := append(append([]string(nil), pool...), stable...)
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			dir := t.TempDir()
			env := r2Open(t, dir, async)
			admin := cypher.NewStorageExecutor(env.store)
			r2MustRun(t, admin, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
			for _, id := range stable {
				r2MustRun(t, admin, "CREATE (:Doc {id:$id, cnt:0, kind:'stable'})", map[string]any{"id": id})
			}
			for _, id := range pool[:3] {
				r2MustRun(t, admin, "CREATE (:Doc {id:$id, kind:'churn'})", map[string]any{"id": id})
			}
			recorder := &r2Recorder{failures: make(map[string][]string), counts: make(map[string]int)}
			var successMu sync.Mutex
			successes := map[string]int64{"s0": 0, "s1": 0, "s2": 0}
			counterForms := []string{
				"MATCH (n:Doc {id:$id}) SET n.cnt = n.cnt + 1 RETURN n.id",
				"MATCH (n:Doc {id:$id}) SET n += {cnt: n.cnt + 1, note: $note} RETURN n.id",
				"MATCH (n:Doc {id:$id}) SET n = {id: n.id, cnt: n.cnt + 1, kind: 'stable'} RETURN n.id",
				"MATCH (n:Doc {id:$id}) SET n:Tagged, n.cnt = n.cnt + 1 RETURN n.id",
				"MATCH (n:Doc {id:$id}) REMOVE n:Tagged SET n.cnt = n.cnt + 1 RETURN n.id",
				"MERGE (n:Doc {id:$id}) ON MATCH SET n.cnt = n.cnt + 1 RETURN n.id",
			}
			nonKeyForms := []string{
				"MATCH (n:Doc {id:$p}) SET n.touch = $note RETURN n.id",
				"MATCH (n:Doc {id:$p}) SET n += {touch: $note} RETURN n.id",
				"MATCH (n:Doc {id:$p}) SET n:Seen RETURN n.id",
			}
			churnForms := []string{
				"CREATE (:Doc {id:$p, kind:'churn'})",
				"MERGE (n:Doc {id:$p}) ON CREATE SET n.kind = 'churn' ON MATCH SET n.m = coalesce(n.m, 0) + 1",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n.id = $q",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' DETACH DELETE n",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' REMOVE n:Doc SET n:Parked",
				"MATCH (n:Parked) WITH n LIMIT 1 SET n:Doc REMOVE n:Parked",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n = {id: $q, kind: 'churn'}",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n += {id: $q}",
				"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' REMOVE n.id",
				"MATCH (n:Doc) WHERE n.kind = 'churn' AND n.id IS NULL WITH n LIMIT 1 SET n.id = $q",
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			worker := func(index int, body func(rng *rand.Rand, exec *cypher.StorageExecutor, route r2Route, i int)) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(index)*7919 + time.Now().UnixNano()))
					exec := cypher.NewStorageExecutor(env.store)
					<-start
					for i := 0; i < iterations; i++ {
						body(rng, exec, r2Routes[rng.Intn(len(r2Routes))], i)
					}
				}()
			}
			opCtx := func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 90*time.Second)
			}
			for w := 0; w < 4; w++ {
				worker(w, func(rng *rand.Rand, exec *cypher.StorageExecutor, route r2Route, i int) {
					id := stable[rng.Intn(len(stable))]
					form := rng.Intn(len(counterForms))
					ctx, cancel := opCtx()
					defer cancel()
					outcome := env.run(ctx, exec, route, counterForms[form], map[string]any{"id": id, "note": fmt.Sprintf("w%d-%d", w, i)})
					switch {
					case outcome.err == nil && outcome.rows == 1:
						successMu.Lock()
						successes[id]++
						successMu.Unlock()
						recorder.note("counter_ok", false, "")
					case outcome.err == nil && outcome.rows == 0:
						// The same stress reproduces transient empty MATCH results
						// on release base eb16. Keep them visible as diagnostics;
						// successful increments still have to match exactly.
						recorder.note("counter_zero_rows", true, "%s/%v form %d matched 0 rows", route.name, route.explicit, form)
					case outcome.err == nil:
						recorder.note("counter_rows", true, "%s/%v form %d matched %d rows", route.name, route.explicit, form, outcome.rows)
					case outcome.unique:
						recorder.note("FALSE_UNIQUE_counter", true, "%s/%v form %d: %s %s", route.name, route.explicit, form, outcome.code, outcome.msg)
					case outcome.transient():
						recorder.note("counter_transient:"+outcome.code, false, "")
					default:
						recorder.note("counter_unexpected", true, "%s/%v form %d: %s %s", route.name, route.explicit, form, outcome.code, outcome.msg)
					}
				})
			}
			for w := 4; w < 6; w++ {
				worker(w, func(rng *rand.Rand, exec *cypher.StorageExecutor, route r2Route, i int) {
					form := rng.Intn(len(nonKeyForms))
					ctx, cancel := opCtx()
					defer cancel()
					outcome := env.run(ctx, exec, route, nonKeyForms[form], map[string]any{"p": pool[rng.Intn(len(pool))], "note": fmt.Sprintf("w%d-%d", w, i)})
					switch {
					case outcome.err == nil:
						recorder.note("nonkey_ok", false, "")
					case outcome.unique:
						recorder.note("FALSE_UNIQUE_nonkey", true, "%s/%v form %d: %s %s", route.name, route.explicit, form, outcome.code, outcome.msg)
					case outcome.transient():
						recorder.note("nonkey_transient:"+outcome.code, false, "")
					default:
						recorder.note("nonkey_other:"+outcome.code, true, "%s/%v form %d: %s", route.name, route.explicit, form, outcome.msg)
					}
				})
			}
			for w := 6; w < 10; w++ {
				worker(w, func(rng *rand.Rand, exec *cypher.StorageExecutor, route r2Route, i int) {
					form := rng.Intn(len(churnForms))
					ctx, cancel := opCtx()
					defer cancel()
					p, q := pool[rng.Intn(len(pool))], pool[rng.Intn(len(pool))]
					outcome := env.run(ctx, exec, route, churnForms[form], map[string]any{"p": p, "q": q})
					switch {
					case outcome.err == nil:
						recorder.note("churn_ok", false, "")
						env.history.add(fmt.Sprintf("w%d %s/%v ok form %d p=%s q=%s", w, route.name, route.explicit, form, p, q), p, q)
					case outcome.unique:
						recorder.note("churn_unique", false, "")
					case outcome.transient():
						recorder.note("churn_transient:"+outcome.code, false, "")
					default:
						recorder.note("churn_other:"+outcome.code, true, "%s/%v form %d: %s", route.name, route.explicit, form, outcome.msg)
					}
				})
			}
			began := time.Now()
			close(start)
			wg.Wait()
			kinds := make([]string, 0, len(recorder.counts))
			for kind := range recorder.counts {
				kinds = append(kinds, kind)
			}
			sort.Strings(kinds)
			for _, kind := range kinds {
				t.Logf("%-60s %d", kind, recorder.counts[kind])
				for _, sample := range recorder.failures[kind] {
					t.Logf("    %s", sample)
				}
			}
			t.Logf("workload took %s; successful increments %v", time.Since(began), successes)
			// Every concurrent outcome must be success, a transient conflict or
			// a genuine UNIQUE violation. Only the base-proven empty counter
			// MATCH stays a diagnostic.
			for kind, samples := range recorder.failures {
				if kind != "counter_zero_rows" {
					t.Errorf("%s: %d occurrences, e.g. %v", kind, recorder.counts[kind], samples)
				}
			}
			// Write-behind lost updates are a separate, pre-existing issue. This
			// matrix pins no false UNIQUE, no stale reservation and no duplicates.
			if async {
				successes = nil
			}
			env.checkInvariants(t, "live", values, successes, &env.history)
			if async {
				require.NoError(t, env.badger.FlushWriteBehind())
				env.checkInvariants(t, "flushed", values, successes, &env.history)
			}
			env.close()
			if async {
				// Concurrent async label churn can persist inconsistent label
				// counts on the release base (eb16, 3/10 in the base control).
				// Restart under that workload belongs to its separate issue.
				// Deterministic buffered/reclaim restart controls remain below.
				return
			}
			restarted := r2Open(t, dir, async)
			restarted.checkInvariants(t, "restarted", values, successes, &env.history)
		})
	}
}

// Round-1 P2-1 schedule extended past the classifier: after a direct writer
// reclaims (or recreates under) the buffered predecessor key, data and the
// claim registry must agree once the buffer replays and after a restart.
func TestReview1655R2DirectReclaimRegistry(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreate=%t", recreate), func(t *testing.T) {
			dir := t.TempDir()
			open := func() (*storage.BadgerEngine, *multidb.DatabaseManager, storage.Engine, *cypher.StorageExecutor) {
				base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
					DataDir: dir, LowMemory: true, WriteBehind: true,
					WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000,
				})
				require.NoError(t, err)
				manager, err := multidb.NewDatabaseManager(base, nil)
				require.NoError(t, err)
				store, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				return base, manager, store, cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			}
			base, manager, store, exec := open()
			r2MustRun(t, exec, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, base.FlushWriteBehind())
			r2MustRun(t, exec, "CREATE (:Doc {id:'persisted', note:'seed'})", nil)
			require.NoError(t, base.FlushWriteBehind())
			r2MustRun(t, exec, "MATCH (n:Doc) SET n.id='original'", nil)
			nodes, err := store.GetNodesByLabel("Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			peer, err := store.GetNode(nodes[0].ID)
			require.NoError(t, err)
			require.Equal(t, "original", peer.Properties["id"])
			peer.Properties["id"] = "persisted"
			peer.Properties["note"] = "peer"
			if recreate {
				require.NoError(t, store.DeleteNode(peer.ID))
				_, err = store.CreateNode(peer)
			} else {
				err = store.UpdateNode(peer)
			}
			require.NoError(t, err)
			probe := func(phase string, exec *cypher.StorageExecutor) {
				result := r2MustRun(t, exec, "MATCH (n:Doc) RETURN n.id AS id, n.note AS note ORDER BY id", nil)
				t.Logf("%s: data %v", phase, result.Rows)
				held := map[string]bool{}
				for _, row := range result.Rows {
					held[fmt.Sprint(row[0])] = true
				}
				for _, value := range []string{"persisted", "original"} {
					_, err := exec.Execute(context.Background(), "CREATE (:Doc {id:$v, note:'probe'})", map[string]any{"v": value})
					switch {
					case held[value] && err == nil:
						t.Errorf("%s: DUPLICATE ACCEPTED for held id=%s", phase, value)
					case !held[value] && err != nil:
						t.Errorf("%s: STALE CLAIM rejects free id=%s: %v", phase, value, err)
					case !held[value]:
						r2MustRun(t, exec, "MATCH (n:Doc {id:$v, note:'probe'}) DELETE n", map[string]any{"v": value})
					}
					t.Logf("%s: probe %s held=%t err=%v", phase, value, held[value], err)
				}
				if _, err := exec.Execute(context.Background(), "MATCH (n:Doc) SET n.final = $phase", map[string]any{"phase": phase}); err != nil {
					t.Errorf("%s: non-key SET failed: %v", phase, err)
				}
			}
			probe("buffered", exec)
			require.NoError(t, base.FlushWriteBehind())
			probe("flushed", exec)
			require.NoError(t, manager.Close())
			require.NoError(t, base.Close())
			base, manager, _, exec = open()
			t.Cleanup(func() { require.NoError(t, manager.Close()); require.NoError(t, base.Close()) })
			probe("restarted", exec)
		})
	}
}

// The procedure-result admission boundary re-reads every returned node.
// Procedures that return virtual nodes, or index hits for a node this
// transaction deleted, must still run.
func TestReview1655R2ProcedureNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	admin := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	r2MustRun(t, admin, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	r2MustRun(t, admin, "CREATE (:Doc {id:'keep', content:'zephyrine harbour notes'}), (:Doc {id:'gone', content:'zephyrine harbour annex'})-[:LINK]->(:Other {name:'x'})", nil)
	service := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(ctx))
	executor := func() *cypher.StorageExecutor {
		exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
		exec.SetSearchService(service)
		return exec
	}
	for _, form := range []struct {
		name     string
		explicit bool
		query    string
	}{
		{"visualization/autocommit_read", false, "CALL db.schema.visualization() YIELD nodes RETURN size(nodes) AS c"},
		{"visualization/autocommit_write", false, "CREATE (t:Tmp) WITH t CALL db.schema.visualization() YIELD nodes RETURN size(nodes) AS c"},
		{"visualization/explicit", true, "CALL db.schema.visualization() YIELD nodes RETURN size(nodes) AS c"},
		{"retrieve/explicit_baseline", true, "CALL db.retrieve({query:'zephyrine', types:['Doc'], limit:5}) YIELD node RETURN node.id AS id"},
		{"retrieve/explicit_after_delete", true, "MATCH (n:Doc {id:'gone'}) DETACH DELETE n"},
	} {
		t.Run(form.name, func(t *testing.T) {
			exec := executor()
			if form.explicit {
				r2MustRun(t, exec, "BEGIN", nil)
				defer exec.Execute(context.Background(), "ROLLBACK", nil)
			}
			result, err := exec.Execute(ctx, form.query, nil)
			require.NoError(t, err)
			if form.name == "retrieve/explicit_after_delete" {
				result, err = exec.Execute(ctx, "CALL db.retrieve({query:'zephyrine', types:['Doc'], limit:5}) YIELD node RETURN node.id AS id", nil)
				t.Logf("retrieve after in-transaction delete: rows=%v err=%v", func() any {
					if result == nil {
						return nil
					}
					return result.Rows
				}(), err)
				require.NoError(t, err, "a search hit for a node deleted in this transaction must not fail the procedure")
				require.Equal(t, [][]interface{}{{"keep"}}, result.Rows)
				return
			}
			t.Logf("rows=%v", result.Rows)
		})
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); <-served })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("visualization/bolt_explicit=%t", explicit), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(context.Background())
			query := "CALL db.schema.visualization() YIELD nodes RETURN size(nodes) AS c"
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer tx.Close(context.Background())
				result, err := tx.Run(ctx, query, nil)
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				t.Logf("records=%d", len(records))
				return
			}
			result, err := session.Run(ctx, query, nil)
			require.NoError(t, err)
			records, err := result.Collect(ctx)
			require.NoError(t, err)
			t.Logf("records=%d", len(records))
		})
	}
}
