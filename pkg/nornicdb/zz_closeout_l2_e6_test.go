//go:build closeout_l2_e6

package nornicdb_test

// Report entry 6: with asynchronous writes, a CREATE whose label,
// relationship type or property key is longer than 65,000 bytes is
// acknowledged, and the next open of the database fails while replaying that
// commit ("Key with size ... exceeded 65000 limit"). Expected: the statement
// is refused, or the store opens (and keeps what it acknowledged).

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E6LongLen = 70000

func l2E6Config(async bool) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Database.AsyncWritesEnabled = async
	cfg.Database.AsyncFlushInterval = 50 * time.Millisecond
	return cfg
}

func l2E6Count(ctx context.Context, db *nornicdb.DB, query string) (int64, error) {
	res, err := db.ExecuteCypher(ctx, query, nil)
	if err != nil {
		return -1, err
	}
	if res == nil || len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return -1, fmt.Errorf("unexpected count result %#v", res)
	}
	switch v := res.Rows[0][0].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return -1, fmt.Errorf("count value %#v (%T)", res.Rows[0][0], res.Rows[0][0])
	}
}

type l2E6Case struct {
	name   string
	create string
	count  string
	want   int64
}

func l2E6Cases(long string) []l2E6Case {
	return []l2E6Case{
		{name: "label", create: "CREATE (n:`" + long + "` {k:1}) RETURN n.k", count: "MATCH (n) WHERE n.k = 1 RETURN count(n)", want: 1},
		{name: "reltype", create: "CREATE (:A {k:2})-[:`" + long + "`]->(:B {k:3})", count: "MATCH (n) WHERE n.k IN [2,3] RETURN count(n)", want: 2},
		{name: "propkey", create: "CREATE (n:LongKey {`" + long + "`: 1, k: 9}) RETURN n.k", count: "MATCH (n) WHERE n.k = 9 RETURN count(n)", want: 1},
	}
}

// l2E6Outcome is one run of a case on a fresh store.
type l2E6Outcome struct {
	stmtErr     error
	countBefore int64
	closeErr    error
	reopenErr   error
	countAfter  int64
}

func (o l2E6Outcome) String() string {
	short := func(err error) string {
		if err == nil {
			return "nil"
		}
		s := err.Error()
		if len(s) > 220 {
			s = s[:220] + "..."
		}
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("stmtErr=%s countBefore=%d closeErr=%s reopenErr=%s countAfter=%d",
		short(o.stmtErr), o.countBefore, short(o.closeErr), short(o.reopenErr), o.countAfter)
}

// defect: the statement was acknowledged and then the store does not open, or
// opens without the acknowledged data.
func (o l2E6Outcome) defect(want int64) bool {
	if o.reopenErr != nil {
		return true
	}
	return o.stmtErr == nil && o.countAfter != want
}

func l2E6Run(t *testing.T, async bool, c l2E6Case) l2E6Outcome {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store")
	cfg := l2E6Config(async)
	db, err := nornicdb.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	var o l2E6Outcome
	_, o.stmtErr = db.ExecuteCypher(ctx, c.create, nil)
	o.countBefore, err = l2E6Count(ctx, db, c.count)
	if err != nil {
		t.Logf("count before close failed: %v", err)
	}
	o.closeErr = db.Close()
	db2, err := nornicdb.Open(dir, l2E6Config(async))
	o.reopenErr = err
	o.countAfter = -1
	if err == nil {
		o.countAfter, err = l2E6Count(ctx, db2, c.count)
		if err != nil {
			t.Logf("count after reopen failed: %v", err)
		}
		if cerr := db2.Close(); cerr != nil {
			t.Logf("close after reopen: %v", cerr)
		}
	}
	return o
}

func l2E6Parsers() []struct {
	name string
	set  func() func()
} {
	return []struct {
		name string
		set  func() func()
	}{
		{"nornic", config.WithNornicParser},
		{"antlr", config.WithANTLRParser},
	}
}

func TestL2_e6_Repro(t *testing.T) {
	long := strings.Repeat("L", l2E6LongLen)
	defects := 0
	var lines []string
	for _, p := range l2E6Parsers() {
		restore := p.set()
		for _, c := range l2E6Cases(long) {
			o := l2E6Run(t, true, c)
			bad := o.defect(c.want)
			if bad {
				defects++
			}
			lines = append(lines, fmt.Sprintf("parser=%s case=%s async defect=%v %s", p.name, c.name, bad, o))
		}
		restore()
	}
	// Node ID longer than the key limit through the storage API (the entry's
	// last row: refused at once on both builds; kept as a check).
	for _, async := range []bool{false, true} {
		ctx := context.Background()
		dir := filepath.Join(t.TempDir(), "store")
		db, err := nornicdb.Open(dir, l2E6Config(async))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		_, createErr := db.GetStorage().CreateNode(ctx, &storage.Node{ID: storage.NodeID(strings.Repeat("I", l2E6LongLen)), Labels: []string{"X"}, Properties: map[string]any{"k": int64(5)}})
		closeErr := db.Close()
		db2, reopenErr := nornicdb.Open(dir, l2E6Config(async))
		after := int64(-1)
		if reopenErr == nil {
			after, _ = l2E6Count(ctx, db2, "MATCH (n) WHERE n.k = 5 RETURN count(n)")
			_ = db2.Close()
		}
		bad := reopenErr != nil || (createErr == nil && after != 1)
		if bad {
			defects++
		}
		lines = append(lines, fmt.Sprintf("case=nodeid async=%v defect=%v createErr=%v closeErr=%v reopenErr=%v countAfter=%d", async, bad, createErr != nil, closeErr != nil, reopenErr, after))
	}
	for _, l := range lines {
		t.Log(l)
	}
	if defects > 0 {
		t.Logf("L2RESULT E6 DEFECT %d of 8 runs acknowledged then failed to reopen or lost the write", defects)
		t.Errorf("acknowledged over-long key: store does not reopen (or lost the write) in %d runs", defects)
		return
	}
	t.Logf("L2RESULT E6 OK 0 of 8 runs left an unopenable store")
}

// Control: synchronous writes refuse the same statements and the store reopens;
// an ordinary-length label with asynchronous writes survives a restart.
func TestL2_e6_Control(t *testing.T) {
	long := strings.Repeat("L", l2E6LongLen)
	for _, p := range l2E6Parsers() {
		restore := p.set()
		for _, c := range l2E6Cases(long) {
			o := l2E6Run(t, false, c)
			t.Logf("parser=%s case=%s sync %s", p.name, c.name, o)
			if o.reopenErr != nil {
				t.Errorf("parser=%s case=%s: synchronous store did not reopen: %v", p.name, c.name, o.reopenErr)
			}
			if o.stmtErr == nil && o.countAfter != c.want {
				t.Errorf("parser=%s case=%s: synchronous acknowledged write lost: count %d", p.name, c.name, o.countAfter)
			}
		}
		short := l2E6Case{name: "short-label", create: "CREATE (n:ShortLabel {k:1}) RETURN n.k", count: "MATCH (n) WHERE n.k = 1 RETURN count(n)", want: 1}
		o := l2E6Run(t, true, short)
		t.Logf("parser=%s case=%s async %s", p.name, short.name, o)
		if o.stmtErr != nil || o.reopenErr != nil || o.countAfter != 1 {
			t.Errorf("parser=%s: async short label round trip failed: %s", p.name, o)
		}
		restore()
	}
}

// Cause: on the storage engine with write-behind and the buffered-commit
// journal (the chain nornicdb.Open builds for asynchronous writes), the commit
// of a node with an over-long label is accepted and journaled; only the later
// flush hits the key limit, and the journal still holds the record, so
// RecoverBufferedCommitsAtOpen replays it and fails on the next open.
func TestL2_e6_Cause(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	ctx := context.Background()
	dir := t.TempDir()
	open := func() (*storage.BadgerEngine, *storage.WAL) {
		engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour})
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
		walCfg := storage.DefaultWALConfig()
		walCfg.Dir = filepath.Join(dir, "wal")
		walCfg.BatchSyncInterval = time.Hour
		wal, err := storage.NewWAL(walCfg.Dir, walCfg)
		if err != nil {
			_ = engine.Close()
			t.Fatalf("wal: %v", err)
		}
		engine.SetBufferedCommitWAL(wal)
		return engine, wal
	}
	engine, wal := open()
	tx, err := engine.BeginTransaction(ctx, storage.OrdinaryBegin)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.SetImplicit(true); err != nil {
		t.Fatalf("implicit: %v", err)
	}
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	_, createErr := tx.CreateNode(ctx, &storage.Node{ID: "test:k", Labels: []string{strings.Repeat("L", l2E6LongLen)}, Properties: map[string]any{"a": int64(1)}})
	commitErr := error(nil)
	if createErr == nil {
		commitErr = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	entries, readErr := storage.ReadWALEntriesFromDir(ctx, filepath.Join(dir, "wal"))
	flushErr := engine.FlushWriteBehind(ctx)
	closeErr := engine.Close()
	walCloseErr := wal.Close()
	t.Logf("create=%v commit=%v journalEntries=%d readErr=%v flush=%v close=%v walClose=%v", createErr, commitErr, len(entries), readErr, flushErr, closeErr, walCloseErr)

	engine, wal = open()
	recovered, recoverErr := storage.RecoverBufferedCommitsAtOpen(engine, wal)
	t.Logf("RecoverBufferedCommitsAtOpen recovered=%d err=%v", recovered, recoverErr)
	_ = engine.Close()
	_ = wal.Close()
	if commitErr == nil && createErr == nil && recoverErr != nil {
		t.Logf("cause shown: the commit was accepted before the key limit was checked; replay fails at the key write")
	}
}
