//go:build closeout_l2_e13

package nornicdb

// Report entry 13: a journaled commit that start-up applies gets no
// change-data-capture event. Change capture DIFF, synchronous strict writes.
// A crash copy of the store taken before commit y, given the later journal
// (the state a crash between journal and store write leaves), is opened as
// the server opens it. Expected: y is restored and the feed after the cursor
// recorded before y holds y's event exactly once, also after a second restart.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func l2E13Config() *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	cfg.Database.WALAutoCompactionEnabled = false
	return cfg
}

// l2E13Life is one process lifetime as the server builds it: nornicdb.Open,
// then the database manager over the base storage, then the managed executor.
type l2E13Life struct {
	db      *DB
	manager *multidb.DatabaseManager
	exec    *cypher.StorageExecutor
}

func l2E13Open(t *testing.T, dir string, setDiff bool) *l2E13Life {
	t.Helper()
	ctx := context.Background()
	db, err := Open(dir, l2E13Config())
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	manager, err := multidb.NewDatabaseManager(ctx, db.GetBaseStorageForManager(), nil)
	if err != nil {
		_ = db.Close()
		t.Fatalf("manager: %v", err)
	}
	if setDiff {
		if err := manager.SetDatabaseOption(ctx, db.defaultDatabaseName(), multidb.OptionTxLogEnrichment, "DIFF"); err != nil {
			t.Fatalf("set txLogEnrichment: %v", err)
		}
	}
	engine, err := manager.GetStorage(db.defaultDatabaseName())
	if err != nil {
		t.Fatalf("manager storage: %v", err)
	}
	return &l2E13Life{db: db, manager: manager, exec: cypher.NewStorageExecutor(engine)}
}

func (l *l2E13Life) close(t *testing.T) {
	t.Helper()
	if err := l.db.Close(); err != nil {
		t.Errorf("db close: %v", err)
	}
	if err := l.manager.Close(); err != nil {
		t.Errorf("manager close: %v", err)
	}
}

func (l *l2E13Life) run(t *testing.T, query string, params map[string]interface{}) *cypher.ExecuteResult {
	t.Helper()
	res, err := l.exec.Execute(context.Background(), query, params)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return res
}

func (l *l2E13Life) cursor(t *testing.T) string {
	t.Helper()
	res := l.run(t, "CALL db.cdc.current() YIELD id RETURN id", nil)
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("cdc.current rows %#v", res.Rows)
	}
	s, ok := res.Rows[0][0].(string)
	if !ok {
		t.Fatalf("cursor %#v", res.Rows[0][0])
	}
	return s
}

func (l *l2E13Life) feedNames(t *testing.T, cursor string) []string {
	t.Helper()
	res := l.run(t, "CALL db.cdc.query($from) YIELD id, event RETURN event.state.after.properties.name", map[string]interface{}{"from": cursor})
	names := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) != 1 {
			t.Fatalf("feed row %#v", row)
		}
		names = append(names, fmt.Sprint(row[0]))
	}
	return names
}

func (l *l2E13Life) countK(t *testing.T) int64 {
	t.Helper()
	res := l.run(t, "MATCH (n:K) RETURN count(n)", nil)
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("count rows %#v", res.Rows)
	}
	switch v := res.Rows[0][0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	t.Fatalf("count %#v", res.Rows[0][0])
	return -1
}

func l2E13Copy(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "copy")
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatalf("copy store: %v", err)
	}
	return dst
}

func l2E13Splice(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(dst, "wal")); err != nil {
		t.Fatalf("remove wal: %v", err)
	}
	if err := os.CopyFS(filepath.Join(dst, "wal"), os.DirFS(filepath.Join(src, "wal"))); err != nil {
		t.Fatalf("splice wal: %v", err)
	}
}

// l2E13Prepare writes the baseline, records the cursor, copies the store, then
// commits y on the original. It returns the original, the crash copy (still
// without the later journal) and the cursor.
func l2E13Prepare(t *testing.T) (orig, crash, cursor, ns string) {
	t.Helper()
	orig = filepath.Join(t.TempDir(), "store")
	life := l2E13Open(t, orig, true)
	life.run(t, "CREATE (:K {name: 'baseline'})", nil)
	cursor = life.cursor(t)
	ns = life.db.defaultDatabaseName()
	life.close(t)
	crash = l2E13Copy(t, orig)
	life = l2E13Open(t, orig, false)
	life.run(t, "CREATE (:K {name: 'y'})", nil)
	life.close(t)
	return orig, crash, cursor, ns
}

func l2E13Parsers() []struct {
	name string
	set  func() func()
} {
	return []struct {
		name string
		set  func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}}
}

func TestL2_e13_Repro(t *testing.T) {
	defect := false
	var figures []string
	for _, p := range l2E13Parsers() {
		restore := p.set()
		orig, crash, cursor, _ := l2E13Prepare(t)
		l2E13Splice(t, orig, crash)
		life := l2E13Open(t, crash, false)
		nodes := life.countK(t)
		first := life.feedNames(t, cursor)
		life.close(t)
		life = l2E13Open(t, crash, false)
		second := life.feedNames(t, cursor)
		life.close(t)
		restore()
		want := []string{"y"}
		bad := nodes != 2 || !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want)
		if bad {
			defect = true
		}
		figures = append(figures, fmt.Sprintf("parser=%s nodes=%d feedAfterRecovery=%v feedAfterSecondRestart=%v", p.name, nodes, first, second))
	}
	for _, f := range figures {
		t.Log(f)
	}
	if defect {
		t.Logf("L2RESULT E13 DEFECT %v", figures)
		t.Errorf("recovered commit y: node or change event missing (want 2 K nodes and feed [y] after both restarts)")
		return
	}
	t.Logf("L2RESULT E13 OK %v", figures)
}

// Control: the original store, which applied y itself, has y's event in the
// feed after the cursor, also after a restart: the feed query and the capture
// option work in this harness.
func TestL2_e13_Control(t *testing.T) {
	for _, p := range l2E13Parsers() {
		restore := p.set()
		orig, _, cursor, _ := l2E13Prepare(t)
		life := l2E13Open(t, orig, false)
		nodes := life.countK(t)
		names := life.feedNames(t, cursor)
		life.close(t)
		restore()
		t.Logf("parser=%s control nodes=%d feed=%v", p.name, nodes, names)
		if nodes != 2 || !reflect.DeepEqual(names, []string{"y"}) {
			t.Errorf("parser=%s: control store: nodes=%d feed=%v, want 2 and [y]", p.name, nodes, names)
		}
	}
}

// Cause: entry 13 names the order in Open: the journal is replayed before the
// database's capture mode (its persisted txLogEnrichment option) is known to
// the engine. This test rebuilds Open's storage chain on two crash copies and
// runs RecoverBufferedCommitsAtOpen once without and once after
// multidb.LoadChangeCaptureModes (the step the pin's Open runs first,
// pkg/nornicdb/db.go:951), and logs the replayed count and the number of
// change events of the default database each leaves.
func TestL2_e13_Cause(t *testing.T) {
	orig, crash, _, ns := l2E13Prepare(t)
	crash2 := l2E13Copy(t, crash)
	l2E13Splice(t, orig, crash)
	l2E13Splice(t, orig, crash2)
	for _, loadModes := range []bool{false, true} {
		dir := crash
		if loadModes {
			dir = crash2
		}
		badgerOpts, walCfg := resolveDurabilityOptions(dir, l2E13Config())
		walCfg.RecordPolicy = storage.WALRecordsLarge
		engine, err := storage.NewBadgerEngineWithOptions(badgerOpts)
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
		wal, err := storage.NewWAL(walCfg.Dir, walCfg)
		if err != nil {
			_ = engine.Close()
			t.Fatalf("wal: %v", err)
		}
		_ = storage.NewWALEngine(engine, wal) // attaches the journal as Open does
		if loadModes {
			if err := multidb.LoadChangeCaptureModes(context.Background(), engine, "system"); err != nil {
				t.Fatalf("load capture modes: %v", err)
			}
		}
		capture, ok := storage.ChangeCaptureOf(engine)
		mode := "n/a"
		if ok {
			mode = capture.ChangeCapture(ns).String()
		}
		before := -1
		if ok {
			if evs, err := capture.ChangeEvents(ns, 0, 0, -1); err == nil {
				before = len(evs)
			}
		}
		recovered, recErr := storage.RecoverBufferedCommitsAtOpen(engine, wal)
		after := -1
		if ok {
			if evs, err := capture.ChangeEvents(ns, 0, 0, -1); err == nil {
				after = len(evs)
			}
		}
		t.Logf("loadModesFirst=%v modeAtReplay=%s recovered=%d recoverErr=%v eventsBefore=%d eventsAfter=%d", loadModes, mode, recovered, recErr, before, after)
		_ = engine.Close()
		_ = wal.Close()
	}
}
