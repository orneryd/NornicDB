package nornicdb

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// closeCountingEmbedder counts Close calls and notes one made while an
// embedding was still running. With gate set, each embedding signals entered
// and waits for gate, ignoring cancellation as a native model call does.
type closeCountingEmbedder struct {
	staticTestEmbedder
	closes               atomic.Int32
	running              atomic.Int32
	closedWhileEmbedding atomic.Bool
	gate                 chan struct{}
	entered              chan struct{}
	enterOnce            sync.Once
}

func newCloseCountingEmbedder(dims int) *closeCountingEmbedder {
	return &closeCountingEmbedder{staticTestEmbedder: staticTestEmbedder{dims: dims}}
}

func (e *closeCountingEmbedder) hold() {
	if e.gate != nil {
		e.enterOnce.Do(func() { close(e.entered) })
		<-e.gate
	}
}

func (e *closeCountingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.running.Add(1)
	defer e.running.Add(-1)
	e.hold()
	return e.staticTestEmbedder.Embed(ctx, text)
}

func (e *closeCountingEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.running.Add(1)
	defer e.running.Add(-1)
	e.hold()
	return e.staticTestEmbedder.EmbedBatch(ctx, texts)
}

func (e *closeCountingEmbedder) Close() error {
	if e.running.Load() > 0 {
		e.closedWhileEmbedding.Store(true)
	}
	e.closes.Add(1)
	return nil
}

// Only embed_lifecycle.go touches the queue field. Every other read goes
// through GetEmbedQueue, under db.mu, so none can race StopEmbedQueue or a
// SetEmbedder that replaces the queue (F-EMBED).
func TestEmbedQueueFieldOwnedByLifecycle(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var outside []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || name == "embed_lifecycle.go" {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "embedQueue" {
				outside = append(outside, fset.Position(sel.Pos()).String())
			}
			return true
		})
	}
	require.Empty(t, outside, "db.embedQueue used outside embed_lifecycle.go; read it with GetEmbedQueue")
}

// Every way the database gets an embedder (install, a cached install, a
// provider reload, the per-database registry) leaves it the database's to
// close: Close closes it once, and a closed database refuses it and closes
// it at once. A registry that refuses to build after Close passes too.
func TestEmbedderEntryPointsOwnedUntilClose(t *testing.T) {
	registryConfig := func(db *DB) *embed.Config {
		return &embed.Config{Provider: "openai", Model: "registry-test", Dimensions: db.config.Memory.EmbeddingDimensions}
	}
	entries := []struct {
		name    string
		produce func(db *DB) []*closeCountingEmbedder
	}{
		{"install", func(db *DB) []*closeCountingEmbedder {
			e := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
			db.SetEmbedder(e)
			return []*closeCountingEmbedder{e}
		}},
		{"cached install", func(db *DB) []*closeCountingEmbedder {
			e := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
			db.SetEmbedder(embed.NewCachedEmbedder(e, 16))
			return []*closeCountingEmbedder{e}
		}},
		{"provider reload", func(db *DB) []*closeCountingEmbedder {
			first := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
			second := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
			db.SetEmbedder(first)
			db.SetEmbedder(second)
			return []*closeCountingEmbedder{first, second}
		}},
		{"registry", func(db *DB) []*closeCountingEmbedder {
			var built []*closeCountingEmbedder
			db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
				e := newCloseCountingEmbedder(cfg.Dimensions)
				built = append(built, e)
				return e, nil
			}
			db.embedderForConfig(registryConfig(db))
			return built
		}},
	}
	for _, entry := range entries {
		t.Run(entry.name+"/before close", func(t *testing.T) {
			db := openEmbeddingPauseDB(t, t.TempDir())
			built := entry.produce(db)
			require.NotEmpty(t, built)
			require.NoError(t, db.Close())
			for _, e := range built {
				require.EqualValues(t, 1, e.closes.Load(), "Close closes each embedder the database took, once")
			}
		})
		t.Run(entry.name+"/after close", func(t *testing.T) {
			db := openEmbeddingPauseDB(t, t.TempDir())
			require.NoError(t, db.Close())
			for _, e := range entry.produce(db) {
				require.EqualValues(t, 1, e.closes.Load(), "a closed database closes the embedder it refuses")
			}
		})
	}
}

// The installed embedder is also the registry's default: Close still
// closes it once.
func TestInstalledRegistryDefaultClosedOnce(t *testing.T) {
	db := openEmbeddingPauseDB(t, t.TempDir())
	e := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
	db.SetEmbedder(e)
	config := &embed.Config{Provider: "openai", Model: "default", Dimensions: db.config.Memory.EmbeddingDimensions}
	db.SetDefaultEmbedConfig(config)
	require.Same(t, e, db.embedderForConfig(config), "the registry serves the installed embedder")
	require.NoError(t, db.Close())
	require.EqualValues(t, 1, e.closes.Load())
}

// A registry model still loading when the database closes is refused and
// closed, not handed out or left open.
func TestRegistryCreateRacingCloseIsClosed(t *testing.T) {
	db := openEmbeddingPauseDB(t, t.TempDir())
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(sync.OnceFunc(func() { close(release) }))
	var built atomic.Pointer[closeCountingEmbedder]
	db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
		close(entered)
		<-release
		e := newCloseCountingEmbedder(cfg.Dimensions)
		built.Store(e)
		return e, nil
	}
	got := make(chan embed.Embedder, 1)
	go func() {
		got <- db.embedderForConfig(&embed.Config{Provider: "openai", Model: "racing", Dimensions: db.config.Memory.EmbeddingDimensions})
	}()
	<-entered
	require.NoError(t, db.Close())
	close(release)
	select {
	case e := <-got:
		require.Nil(t, e, "a closed database hands out no new model")
	case <-time.After(10 * time.Second):
		t.Fatal("the registry never returned")
	}
	require.NotNil(t, built.Load())
	require.EqualValues(t, 1, built.Load().closes.Load(), "the model built during Close is closed")
}

// A queue created while the pause changes follows the pause: it is created
// in the new state or told it, whichever way the two interleave.
func TestEmbeddingPauseAppliesToRecreatedQueue(t *testing.T) {
	ctx := context.Background()
	db := openEmbeddingPauseDB(t, t.TempDir())
	t.Cleanup(func() { _ = db.Close() })
	for i := range 20 {
		paused := i%2 == 0
		db.StopEmbedQueue()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := db.SetEmbeddingPaused(ctx, paused)
			if err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			db.SetEmbedder(newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions))
		}()
		wg.Wait()
		require.Equal(t, paused, db.EmbeddingStatus().Paused)
		require.Equal(t, paused, db.GetEmbedQueue().Paused(), "round %d", i)
	}
}

// embedQueueReaders calls every DB method that reads the queue, each from
// its own goroutine, until stop. started is done once each has run once.
func embedQueueReaders(t *testing.T, db *DB, stop <-chan struct{}) (started *sync.WaitGroup, done *sync.WaitGroup) {
	t.Helper()
	readers := map[string]func(ctx context.Context){
		"EmbeddingStatus":        func(context.Context) { _ = db.EmbeddingStatus() },
		"EmbedQueueStats":        func(context.Context) { _ = db.EmbedQueueStats() },
		"PendingEmbeddingsCount": func(context.Context) { _ = db.PendingEmbeddingsCount() },
		"EmbeddingFailures":      func(ctx context.Context) { _, _ = db.EmbeddingFailures(ctx, 1) },
		"RetryEmbeddingFailures": func(ctx context.Context) { _, _ = db.RetryEmbeddingFailures(ctx, []storage.NodeID{"missing"}) },
		"WaitForEmbeddings":      func(ctx context.Context) { _ = db.WaitForEmbeddings(ctx) },
		"EmbedExisting":          func(ctx context.Context) { _, _ = db.EmbedExisting(ctx) },
		"SetEmbeddingPaused":     func(ctx context.Context) { _, _ = db.SetEmbeddingPaused(ctx, false) },
		"ResetEmbedWorker": func(context.Context) {
			_ = db.ResetEmbedWorker()
			time.Sleep(time.Millisecond) // it restarts the worker; don't flood it
		},
	}
	started, done = &sync.WaitGroup{}, &sync.WaitGroup{}
	for _, read := range readers {
		started.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			for first := true; ; first = false {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				read(ctx)
				cancel()
				if first {
					started.Done()
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	return started, done
}

// Every queue reader races StopEmbedQueue without a data race or a nil
// queue. The DB holds only a queue, so no other goroutine orders the two
// sides through shared locks and hides a race from the detector.
func TestEmbedQueueReadersRaceQueueStop(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	db := &DB{embedQueue: NewEmbedQueue(nil, engine, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true})}
	stop := make(chan struct{})
	started, done := embedQueueReaders(t, db, stop)
	started.Wait()
	time.Sleep(20 * time.Millisecond)
	db.StopEmbedQueue()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	done.Wait()
	require.Nil(t, db.EmbedQueueStats())
}

// The same readers keep working while the queue is stopped and recreated
// and while the database closes.
func TestEmbedQueueReadersSurviveRecreationAndClose(t *testing.T) {
	db := openEmbeddingPauseDB(t, t.TempDir())
	stop := make(chan struct{})
	started, done := embedQueueReaders(t, db, stop)
	started.Wait()
	for range 5 {
		db.StopEmbedQueue()
		db.SetEmbedder(newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions))
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, db.Close())
	close(stop)
	done.Wait()
}

// Close leaves the embedder open while a worker is still using it, waits
// for that claim, and then closes the embedder once.
func TestCloseClosesEmbedderAfterInFlightClaim(t *testing.T) {
	ctx := context.Background()
	db := openEmbeddingPauseDB(t, t.TempDir())
	t.Cleanup(func() { _ = db.Close() })
	e := newCloseCountingEmbedder(db.config.Memory.EmbeddingDimensions)
	e.gate, e.entered = make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(e.gate) })
	t.Cleanup(release)
	db.SetEmbedder(e)
	_, err := db.ExecuteCypher(ctx, "CREATE (:Doc {content: 'in flight at close'})", nil)
	require.NoError(t, err)
	select {
	case <-e.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("no claim reached the provider")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("DB.Close returned (%v) while a claim was in flight", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.Zero(t, e.closes.Load(), "the embedder isn't closed under an in-flight claim")
	release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("DB.Close didn't return after the claim finished")
	}
	require.EqualValues(t, 1, e.closes.Load(), "Close closes the embedder once its claim is done")
	require.False(t, e.closedWhileEmbedding.Load())
}

func openAsyncEmbeddingPauseDB(t *testing.T, dir string) *DB {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = true
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = true
	config.Database.StrictDurability = false
	// Nothing reaches disk on a timer during the test: what a crash keeps
	// is only what the pause itself made durable.
	config.Database.AsyncFlushInterval = time.Hour
	config.Database.WALSyncMode = "batch"
	config.Database.WALSyncInterval = time.Hour
	db, err := Open(dir, config)
	require.NoError(t, err)
	return db
}

// copyTree copies dir as a crash would leave it on disk: the files as they
// are, with the database still open.
func copyTree(t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(dst, src); err != nil {
			dst.Close()
			return err
		}
		return dst.Close()
	}))
}

// With async writes, a pause holds across an orderly restart and across a
// crash right after the pause returned: the pause makes its own write
// durable instead of waiting for the next timed sync.
func TestEmbeddingPauseSurvivesRestartWithAsyncWrites(t *testing.T) {
	ctx := context.Background()
	t.Run("orderly close", func(t *testing.T) {
		dir := t.TempDir()
		db := openAsyncEmbeddingPauseDB(t, dir)
		_, err := db.SetEmbeddingPaused(ctx, true)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		db = openAsyncEmbeddingPauseDB(t, dir)
		t.Cleanup(func() { _ = db.Close() })
		require.True(t, db.EmbeddingStatus().Paused)
	})
	t.Run("crash", func(t *testing.T) {
		dir := t.TempDir()
		db := openAsyncEmbeddingPauseDB(t, dir)
		t.Cleanup(func() { _ = db.Close() })
		_, err := db.SetEmbeddingPaused(ctx, true)
		require.NoError(t, err)
		crashed := t.TempDir()
		copyTree(t, dir, crashed)
		recovered := openAsyncEmbeddingPauseDB(t, crashed)
		t.Cleanup(func() { _ = recovered.Close() })
		require.True(t, recovered.EmbeddingStatus().Paused, "the pause survives a crash after it returned")
	})
}

// failingControlEngine fails reads or writes of the stored pause state.
type failingControlEngine struct {
	storage.Engine
	failWrites atomic.Bool
	failReads  atomic.Bool
}

var errControlStore = errors.New("storing the pause state failed")

func isControlNode(id storage.NodeID) bool {
	return strings.HasSuffix(string(id), string(embeddingControlNodeID))
}

func (e *failingControlEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.failWrites.Load() && isControlNode(node.ID) {
		return "", errControlStore
	}
	return e.Engine.CreateNode(node)
}

func (e *failingControlEngine) UpdateNode(node *storage.Node) error {
	if e.failWrites.Load() && isControlNode(node.ID) {
		return errControlStore
	}
	return e.Engine.UpdateNode(node)
}

func (e *failingControlEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.failReads.Load() && isControlNode(id) {
		return nil, errControlStore
	}
	return e.Engine.GetNode(id)
}

// A pause or resume whose state can't be stored fails and changes nothing:
// not the reported state, not the queue. A state that can't be read at Open
// starts paused, and a resume clears it.
func TestEmbeddingPauseStoreFailureLeavesStateUnchanged(t *testing.T) {
	ctx := context.Background()
	engine := &failingControlEngine{Engine: storage.NewMemoryEngine()}
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	queue := NewEmbedQueue(nil, engine, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true})
	t.Cleanup(queue.Close)
	db := &DB{baseStorage: engine, embedQueue: queue}

	engine.failWrites.Store(true)
	_, err := db.SetEmbeddingPaused(ctx, true)
	require.Error(t, err)
	require.False(t, db.EmbeddingStatus().Paused)
	require.False(t, queue.Paused())

	engine.failWrites.Store(false)
	_, err = db.SetEmbeddingPaused(ctx, true)
	require.NoError(t, err)
	require.True(t, db.EmbeddingStatus().Paused)
	require.True(t, queue.Paused())

	engine.failWrites.Store(true)
	_, err = db.SetEmbeddingPaused(ctx, false)
	require.Error(t, err)
	require.True(t, db.EmbeddingStatus().Paused)
	require.True(t, queue.Paused())

	engine.failWrites.Store(false)
	engine.failReads.Store(true)
	reopened := &DB{baseStorage: engine}
	reopened.loadEmbeddingPause()
	require.True(t, reopened.EmbeddingStatus().Paused, "an unreadable state starts paused")
	engine.failReads.Store(false)
	_, err = reopened.SetEmbeddingPaused(ctx, false)
	require.NoError(t, err)
	require.False(t, reopened.EmbeddingStatus().Paused)
}
