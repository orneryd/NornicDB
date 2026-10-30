package nornicdb

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// searchConsumerConfig is one engine configuration of the production wrapper
// chains (search_state.go): how its DB is opened and how a write reaches its
// storage (directly, or through the replication apply entrance).
type searchConsumerConfig struct {
	name string
	open func(t *testing.T) (*DB, func(t *testing.T, cmd replication.CommandType, payload any))
}

func searchConsumerConfigs() []searchConsumerConfig {
	base := func(t *testing.T, inMemory bool) *Config {
		config := DefaultConfig()
		config.Memory.EmbeddingEnabled = false
		config.Memory.DecayEnabled = false
		config.Memory.AutoLinksEnabled = false
		if !inMemory {
			config.Database.DataDir = t.TempDir()
		}
		return config
	}
	direct := func(t *testing.T, db *DB) func(t *testing.T, cmd replication.CommandType, payload any) {
		store := db.GetBaseStorageForManager()
		return func(t *testing.T, cmd replication.CommandType, payload any) {
			t.Helper()
			switch cmd {
			case replication.CmdCreateNode:
				p := payload.(*storage.Node)
				_, err := store.CreateNode(p)
				require.NoError(t, err)
			case replication.CmdUpdateNode:
				p := payload.(*storage.Node)
				require.NoError(t, store.UpdateNode(p))
			case replication.CmdDeleteNode:
				require.NoError(t, store.DeleteNode(storage.NodeID(payload.(string))))
			case replication.CmdCreateEdge:
				p := payload.(*storage.Edge)
				require.NoError(t, store.CreateEdge(p))
			case replication.CmdDeleteEdge:
				require.NoError(t, store.DeleteEdge(storage.EdgeID(payload.(string))))
			case replication.CmdDeleteByPrefix:
				_, _, err := store.DeleteByPrefix(payload.(string))
				require.NoError(t, err)
			}
		}
	}
	replicated := func(role string) func(t *testing.T) (*DB, func(t *testing.T, cmd replication.CommandType, payload any)) {
		return func(t *testing.T) (*DB, func(t *testing.T, cmd replication.CommandType, payload any)) {
			t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
			t.Setenv("NORNICDB_CLUSTER_DATA_DIR", filepath.Join(t.TempDir(), "replication"))
			t.Setenv("NORNICDB_CLUSTER_HA_ROLE", role)
			t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "127.0.0.1:65534")
			t.Setenv("NORNICDB_CLUSTER_BIND_ADDR", "127.0.0.1:0")
			t.Setenv("NORNICDB_CLUSTER_HA_AUTO_FAILOVER", "false")
			config := base(t, false)
			db, err := Open(config.Database.DataDir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.bgWg.Wait()
			require.NotNil(t, db.replicationAdapter)
			_, isReplicated := db.GetBaseStorageForManager().(*replication.ReplicatedEngine)
			require.True(t, isReplicated, "the configuration under test has no ReplicatedEngine")
			return db, func(t *testing.T, cmd replication.CommandType, payload any) {
				t.Helper()
				data := searchConsumerCommandData(t, cmd, payload)
				require.NoError(t, db.replicationAdapter.ApplyCommand(context.Background(), &replication.Command{Type: cmd, Data: data}))
			}
		}
	}
	return []searchConsumerConfig{
		{name: "standalone", open: func(t *testing.T) (*DB, func(t *testing.T, cmd replication.CommandType, payload any)) {
			config := base(t, false)
			db, err := Open(config.Database.DataDir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.bgWg.Wait()
			return db, direct(t, db)
		}},
		{name: "in-memory", open: func(t *testing.T) (*DB, func(t *testing.T, cmd replication.CommandType, payload any)) {
			config := base(t, true)
			db, err := Open("", config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.bgWg.Wait()
			return db, direct(t, db)
		}},
		{name: "ha primary", open: replicated("primary")},
		{name: "ha standby", open: replicated("standby")},
	}
}

// searchConsumerCommandData encodes the same images and delete commands used by the replication transport.
func searchConsumerCommandData(t *testing.T, cmd replication.CommandType, payload any) []byte {
	t.Helper()
	var data []byte
	var err error
	switch cmd {
	case replication.CmdCreateNode, replication.CmdUpdateNode:
		data, err = storage.EncodeNodePayload(payload.(*storage.Node))
	case replication.CmdCreateEdge:
		data, err = storage.EncodeEdgePayload(payload.(*storage.Edge))
	case replication.CmdDeleteNode:
		data = []byte(payload.(string))
	case replication.CmdDeleteEdge:
		data, err = storage.EncodeCommandPayload(struct{ EdgeID string }{payload.(string)})
	case replication.CmdDeleteByPrefix:
		data, err = storage.EncodeCommandPayload(struct{ Prefix string }{payload.(string)})
	default:
		t.Fatalf("unexpected replication command: %d", cmd)
	}
	require.NoError(t, err)
	return data
}

// Every storage event and namespace reset reaches search in every production
// wrapper configuration, through whichever entrance writes there (#1482
// round 4: registration stopped at ReplicatedEngine). Removing a forwarder or
// a registration fails a row.
func TestSearchConsumesStorageInEveryConfiguration(t *testing.T) {
	for _, cfg := range searchConsumerConfigs() {
		t.Run(cfg.name, func(t *testing.T) {
			db, apply := cfg.open(t)
			ctx := context.Background()
			dims := db.config.Memory.EmbeddingDimensions
			if dims <= 0 {
				dims = 1024
			}
			vector := make([]float32, dims)
			vector[0] = 1
			apply(t, replication.CmdCreateNode, &storage.Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "oldword"}})
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			svc, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			require.Equal(t, 1, consumerDBHits(t, svc, "oldword"))

			apply(t, replication.CmdUpdateNode, &storage.Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "newword"}})
			settleQueuedRefreshes()
			require.Equal(t, 1, consumerDBHits(t, svc, "newword"), "node update")
			apply(t, replication.CmdCreateNode, &storage.Node{ID: "tenant:b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "bword"}})
			settleQueuedRefreshes()
			require.Equal(t, 1, consumerDBHits(t, svc, "bword"), "node create")

			apply(t, replication.CmdCreateEdge, &storage.Edge{ID: "tenant:r", StartNode: "tenant:a", EndNode: "tenant:b", Type: "CITES", Properties: map[string]any{"vec": vector}})
			settleQueuedRefreshes()
			require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "edge create")
			apply(t, replication.CmdDeleteEdge, "tenant:r")
			settleQueuedRefreshes()
			require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "edge delete")
			apply(t, replication.CmdDeleteNode, "tenant:b")
			settleQueuedRefreshes()
			require.Zero(t, consumerDBHits(t, svc, "bword"), "node delete")

			apply(t, replication.CmdDeleteByPrefix, "tenant:")
			fresh, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			require.NotSame(t, svc, fresh, "namespace reset kept the dropped database's service")
			require.Zero(t, fresh.FulltextDocCount(), "namespace reset kept the dropped database's documents")
		})
	}
}

// openPersistedSearchDB opens a DB that persists search indexes under dataDir.
func openPersistedSearchDB(t *testing.T, dataDir string) *DB {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.DataDir = dataDir
	config.Database.PersistSearchIndexes = true
	db, err := Open(dataDir, config)
	require.NoError(t, err)
	db.bgWg.Wait()
	return db
}

// A database whose search state is only persisted (the DB reopened; nothing
// loaded its service) is reset by a DeleteByPrefix and by a restore like a
// loaded one: the next build reads storage, not the dropped or replaced
// database's files (#1482 round 4).
func TestColdPersistedSearchStateIsReset(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "prefix delete", true: "restore"}[restore], func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			db := openPersistedSearchDB(t, dataDir)
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			vector := make([]float32, db.config.Memory.EmbeddingDimensions)
			vector[0] = 1
			_, err := tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "backupword"}, ChunkEmbeddings: [][]float32{vector}})
			require.NoError(t, err)
			svc, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			requireIndexed(t, svc, 1, "seed")
			backup := filepath.Join(t.TempDir(), "backup.bin")
			require.NoError(t, db.Backup(ctx, backup))
			node, err := tenant.GetNode("a")
			require.NoError(t, err)
			node.Properties["content"] = "laterword"
			require.NoError(t, tenant.UpdateNode(node))
			settleQueuedRefreshes()
			require.NoError(t, db.Close())

			db = openPersistedSearchDB(t, dataDir)
			t.Cleanup(func() { _ = db.Close() })
			// Startup may load the tenant's service; unload it, keeping its
			// files, as Close does, so its state is only persisted.
			db.invalidateSearchState(func(name string) bool { return name == "tenant" }, false)
			require.NotContains(t, db.searchServiceNames(), "tenant", "the tenant's service is loaded; the state under test is cold")
			base, err := searchPersistenceBasePath(dataDir, "tenant")
			require.NoError(t, err)
			require.DirExists(t, base, "the tenant has no persisted search state")
			tenant = storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			if restore {
				require.NoError(t, db.Restore(ctx, backup))
			} else {
				_, _, err = db.GetBaseStorageForManager().DeleteByPrefix("tenant:")
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "freshword"}, ChunkEmbeddings: [][]float32{vector}})
				require.NoError(t, err)
			}
			fresh, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			settleQueuedRefreshes()
			require.Equal(t, 1, fresh.FulltextDocCount(), "the tenant's index is not what storage holds")
			require.Zero(t, consumerDBHits(t, fresh, "laterword"), "a persisted index of replaced storage was loaded")
			if restore {
				require.Equal(t, 1, consumerDBHits(t, fresh, "backupword"))
			} else {
				require.Equal(t, 1, consumerDBHits(t, fresh, "freshword"))
			}
		})
	}
}

// A service retired while it may still write (a persist timer, a build end)
// writes under its own generation's directory, which no later generation
// loads: CURRENT names the live one, and leftovers are removed.
func TestRetiredSearchGenerationIsNeverLoaded(t *testing.T) {
	dataDir := t.TempDir()
	db := openPersistedSearchDB(t, dataDir)
	t.Cleanup(func() { _ = db.Close() })
	_, err := db.GetOrCreateSearchService("tenant", nil)
	require.NoError(t, err)
	base, err := searchPersistenceBasePath(dataDir, "tenant")
	require.NoError(t, err)
	first, err := readSearchStateToken(filepath.Join(base, searchStateCurrentFile))
	require.NoError(t, err)

	db.ResetSearchService("tenant")
	_, err = os.Stat(base)
	require.True(t, errors.Is(err, fs.ErrNotExist), "reset left the persisted state")
	// The retired service writes late into its own directory.
	require.NoError(t, os.MkdirAll(filepath.Join(base, first), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, first, "bm25.v2"), []byte("stale"), 0o600))

	_, err = db.GetOrCreateSearchService("tenant", nil)
	require.NoError(t, err)
	second, err := readSearchStateToken(filepath.Join(base, searchStateCurrentFile))
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	require.NoDirExists(t, filepath.Join(base, first), "a retired generation's files remain")

	// A late write after the next generation exists is removed when that
	// generation is next opened, and CURRENT keeps naming it.
	require.NoError(t, os.MkdirAll(filepath.Join(base, first), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, first, "bm25.v2"), []byte("stale"), 0o600))
	require.NoError(t, db.Close())
	reopened := openPersistedSearchDB(t, dataDir)
	t.Cleanup(func() { _ = reopened.Close() })
	_, err = reopened.GetOrCreateSearchService("tenant", nil)
	require.NoError(t, err)
	current, err := readSearchStateToken(filepath.Join(base, searchStateCurrentFile))
	require.NoError(t, err)
	require.Equal(t, second, current)
	require.NoDirExists(t, filepath.Join(base, first), "a retired generation's late files remain")
}

// Search state persisted in the earlier layout (files directly in <db>/, as
// the offline importer writes them) is kept as the current generation.
func TestLegacySearchStateLayoutBecomesTheCurrentGeneration(t *testing.T) {
	dataDir := t.TempDir()
	base, err := searchPersistenceBasePath(dataDir, "tenant")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(base, "hnsw_ivf"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "bm25.v2"), []byte("legacy"), 0o600))
	db := openPersistedSearchDB(t, dataDir)
	t.Cleanup(func() { _ = db.Close() })
	db.searchServiceCreationMu.Lock()
	dir, err := db.activeSearchStateDir("tenant")
	db.searchServiceCreationMu.Unlock()
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "bm25.v2"))
	require.NoError(t, err)
	require.Equal(t, "legacy", string(data))
	require.DirExists(t, filepath.Join(dir, "hnsw_ivf"))
	require.NoFileExists(t, filepath.Join(base, "bm25.v2"))
}

// failingReadEngine fails the next reads of one node or relationship.
type failingReadEngine struct {
	storage.Engine
	node  storage.NodeID
	edge  storage.EdgeID
	fails atomic.Int32
}

var errTransientRead = errors.New("transient read failure")

func (e *failingReadEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == e.node && e.fails.Add(-1) >= 0 {
		return nil, errTransientRead
	}
	return e.Engine.GetNode(id)
}

func (e *failingReadEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if id == e.edge && e.fails.Add(-1) >= 0 {
		return nil, errTransientRead
	}
	return e.Engine.GetEdge(id)
}

// A refresh whose storage read fails is retried, not lost: from the deferred
// drain of storage events, and from a synchronous caller (Cypher, the DB API,
// Qdrant), which all reach RefreshNode / RefreshEdge (bot thread on #1482:
// "Retain embedded refreshes after transient read failures").
func TestRefreshAfterTransientReadFailureIsRetried(t *testing.T) {
	for _, path := range []string{"drain", "synchronous"} {
		t.Run(path, func(t *testing.T) {
			db, _ := openConsumerDB(t, false)
			ctx := context.Background()
			failing := &failingReadEngine{Engine: storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant"), node: "a", edge: "r"}
			svc, err := db.EnsureSearchIndexes(ctx, "tenant", failing, SearchIndexBuildWait)
			require.NoError(t, err)
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			_, err = tenant.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}})
			require.NoError(t, err)
			dims := db.config.Memory.EmbeddingDimensions
			vector := make([]float32, dims)
			vector[0] = 1
			failing.fails.Store(2)
			_, err = tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "retryword"}})
			require.NoError(t, err)
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"vec": vector}}))
			if path == "synchronous" {
				// Indexed by the drain once its retry succeeded; then made stale
				// (payload API), so only the synchronous refresh's retry can
				// restore it.
				require.Eventually(t, func() bool {
					return consumerDBHits(t, svc, "retryword") == 1 && svc.HasRelationshipVectorEntries("CITES", "vec")
				}, 20*searchMutationDebounceDelay*8, searchMutationDebounceDelay)
				require.NoError(t, svc.RemoveNode("a"))
				require.NoError(t, svc.RemoveEdge("r"))
				require.Zero(t, consumerDBHits(t, svc, "retryword"))
				failing.fails.Store(2)
				require.ErrorIs(t, svc.RefreshNode("a"), errTransientRead)
				require.ErrorIs(t, svc.RefreshEdge("r"), errTransientRead)
			}
			require.Eventually(t, func() bool {
				return consumerDBHits(t, svc, "retryword") == 1 && svc.HasRelationshipVectorEntries("CITES", "vec")
			}, 20*searchMutationDebounceDelay*8, searchMutationDebounceDelay, "a refresh whose read failed was lost")
		})
	}
}

// A replaced service is never what a consumer resolves: the Cypher result
// cache keys by the service instance, and executors resolve per use.
func TestResolvedSearchServiceFollowsReset(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	resolve := db.SearchServiceResolver()
	name := db.defaultDatabaseName()
	require.Same(t, svc, resolve(name))
	generation := func() uint64 {
		db.searchServicesMu.RLock()
		defer db.searchServicesMu.RUnlock()
		return db.searchServices[name].generation
	}
	before := generation()
	db.searchServicesMu.RLock()
	old := db.searchServices[name]
	db.searchServicesMu.RUnlock()
	db.ResetSearchService(name)
	// The replaced entry takes no further refresh, retried or queued.
	db.searchRefreshRetry(old)(search.RefreshRetry{Node: svc.ReserveNodeMutation("a")}, errors.New("read failed"))
	require.False(t, old.hasPending(), "a replaced entry still queues refreshes")
	replaced := resolve(name)
	require.NotNil(t, replaced)
	require.NotSame(t, svc, replaced)
	require.NotEqual(t, svc.Instance(), replaced.Instance())
	require.Equal(t, before+1, generation(), "a reset did not start the next generation")
	require.True(t, svc.Closed(), "the replaced service was not closed")
}

// productionGoFiles lists the non-test Go files under the repository's pkg/
// and cmd/ trees.
func productionGoFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, root := range []string{"../../pkg", "../../cmd"} {
		require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				files = append(files, path)
			}
			return nil
		}))
	}
	return files
}

// engineWrapperExclusions are the engine wrappers that are never a DB's event
// source, so they need not forward the event subscription.
var engineWrapperExclusions = map[string]string{
	"cypher.transactionStorageWrapper": "a transaction's view; the DB subscribes on the engine the transaction commits to",
	"auth.storeTransaction":            "a transaction's view over the system store; never subscribed",
	"storage.RemoteEngine":             "a remote store; its own process publishes its events",
	"storage.CompositeEngine":          "a composite root; search is never built on one (the server excludes it), its constituents subscribe themselves",
	"storage.MemoryEngine":             "embeds *BadgerEngine, whose subscription it inherits",
	"cypher.systemGraphView":           "the system graph's view for procedures; never subscribed",
}

// Every engine wrapper in production code forwards the event subscription
// (storage.EventSubscriber), or is excluded with its reason: a wrapper added
// without it would silently cut search off from storage (#1482 round 4).
// Compile-time assertions in each wrapper's package check the method set.
func TestEveryEngineWrapperForwardsEventSubscription(t *testing.T) {
	fset := token.NewFileSet()
	wrappers := make(map[string]bool)
	subscribers := make(map[string]bool)
	for _, path := range productionGoFiles(t) {
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		pkg := file.Name.Name
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range st.Fields.List {
						// An embedded engine: the type is itself an engine
						// over the one it embeds.
						if len(field.Names) == 0 && isEngineType(field.Type, pkg) {
							wrappers[pkg+"."+ts.Name.Name] = true
						}
					}
				}
			case *ast.FuncDecl:
				if d.Recv == nil || len(d.Recv.List) != 1 {
					continue
				}
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				id, ok := recv.(*ast.Ident)
				if !ok {
					continue
				}
				switch d.Name.Name {
				case "GetInnerEngine":
					wrappers[pkg+"."+id.Name] = true
				case "SubscribeEvents":
					subscribers[pkg+"."+id.Name] = true
				}
			}
		}
	}
	require.Contains(t, wrappers, "replication.ReplicatedEngine")
	var missing []string
	for name := range wrappers {
		if !subscribers[name] && engineWrapperExclusions[name] == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "engine wrappers that do not forward SubscribeEvents")
}

// isEngineType reports a storage.Engine field or embedding (Engine inside
// package storage).
func isEngineType(expr ast.Expr, pkg string) bool {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		x, ok := e.X.(*ast.Ident)
		return ok && x.Name == "storage" && e.Sel.Name == "Engine"
	case *ast.Ident:
		return pkg == "storage" && e.Name == "Engine"
	}
	return false
}

// searchServiceHolders are the production structs allowed to keep a
// *search.Service, with the reason.
var searchServiceHolders = map[string]string{
	"nornicdb.dbSearchService": "the owner's entry for one generation of a database's search state",
	"cypher.StorageExecutor":   "an executor no database wired; a wired one resolves per use (SetSearchServiceResolver)",
	"main.searchServiceProbe":  "metrics probe; constructed with no service (nil)",
	"eval.Harness":             "offline evaluation over a service it is given; no database owns it",
	"ir.NornicRetriever":       "offline evaluation retriever; no database owns its service",
}

// No consumer outside the owner keeps a search service: it resolves the
// database's current one on each use, so a reset or restore cannot leave it
// with the service it replaced (#1482 round 4).
func TestNoConsumerRetainsASearchService(t *testing.T) {
	fset := token.NewFileSet()
	var holders, setters []string
	for _, path := range productionGoFiles(t) {
		if strings.Contains(filepath.ToSlash(path), "/pkg/search/") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		pkg := file.Name.Name
		ast.Inspect(file, func(n ast.Node) bool {
			// An executor is given a resolver, not a service: only the
			// executor's own unwired fallback sets one.
			if call, ok := n.(*ast.CallExpr); ok && pkg != "cypher" {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetSearchService" {
					setters = append(setters, fset.Position(call.Pos()).String())
				}
			}
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				if holdsSearchService(field.Type) {
					holders = append(holders, pkg+"."+ts.Name.Name)
				}
			}
			return true
		})
	}
	var unexpected []string
	for _, name := range holders {
		if searchServiceHolders[name] == "" {
			unexpected = append(unexpected, name)
		}
	}
	sort.Strings(unexpected)
	require.Empty(t, unexpected, "structs keeping a *search.Service outside the owner")
	require.Empty(t, setters, "executors given a service instead of a resolver (SetSearchServiceResolver)")
}

func holdsSearchService(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		sel, ok := e.X.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		x, ok := sel.X.(*ast.Ident)
		return ok && x.Name == "search" && sel.Sel.Name == "Service"
	case *ast.MapType:
		return holdsSearchService(e.Value)
	case *ast.ArrayType:
		return holdsSearchService(e.Elt)
	}
	return false
}

var _ = search.DefaultSearchOptions
