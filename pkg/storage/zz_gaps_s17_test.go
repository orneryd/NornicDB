//go:build report_gaps

package storage_test

// Report gap S17: which caller wrote the namespace schema record that a
// refused autocommit CREATE INDEX / DROP INDEX read. The E17 workload of the
// earlier cause run (4 writers CREATE (:W {v:$v}) in autocommit; the test
// goroutine CREATE INDEX w_v IF NOT EXISTS / DROP INDEX w_v IF EXISTS in
// autocommit) on the stack nornicdb.Open builds (strict durability, journal
// sync immediate), through the database manager and the Cypher executor.
// The schema manager's persister of namespace "nornic" is wrapped
// (zz_gaps_s17_export_test.go) so every write of the schema record is
// recorded with its Badger version, goroutine and stack. For each refused
// statement: the versions of the schema record above its read timestamp and
// which recorded call wrote each (same goroutine as the refused statement or
// another). Rounds of 300 statements until 4 refusals or 9,000 statements.
// Diagnostic: fails only on harness errors.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestGapS17SchemaRecordWriter(t *testing.T) {
	ctx := context.Background()
	const wanted, maxStatements, pairs, writers = 4, 9000, 150, 4
	refused, total, rounds, writes, baselines := 0, 0, 0, 0, 0
	ddlGID := storage.GapS17GoroutineID()
	for refused < wanted && total < maxStatements {
		rounds++
		func() {
			cfg := nornicdb.DefaultConfig()
			cfg.Memory.DecayEnabled = false
			cfg.Database.AsyncWritesEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.StrictDurability = true
			cfg.Database.WALSyncMode = "immediate"
			db, err := nornicdb.Open(t.TempDir(), cfg)
			if err != nil {
				t.Fatalf("nornicdb.Open: %v", err)
			}
			defer func() { _ = db.Close() }()
			capture, err := storage.NewGapS17Engine(db.GetBaseStorageForManager())
			if err != nil {
				t.Fatalf("seam: %v", err)
			}
			manager, err := multidb.NewDatabaseManager(ctx, capture, nil)
			if err != nil {
				t.Fatalf("database manager: %v", err)
			}
			defer func() { _ = manager.Close() }()
			name := manager.DefaultDatabaseName()
			engine, err := manager.GetStorage(name)
			if err != nil {
				t.Fatalf("GetStorage: %v", err)
			}
			capture.InstallPersisterRecorder(name)
			if rounds == 1 {
				t.Logf("GAPS17 stack default_db=%q engine=%T test_goroutine=%d", name, engine, ddlGID)
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					exec := cypher.NewStorageExecutor(engine)
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						if _, err := exec.Execute(ctx, "CREATE (:W {v: $v})", map[string]interface{}{"v": int64(w*1000000 + i)}); err == nil {
							mu.Lock()
							writes++
							mu.Unlock()
						}
					}
				}(w)
			}
			defer func() { close(stop); wg.Wait() }()
			ddl := cypher.NewStorageExecutor(engine)
			for i := 0; i < pairs && refused < wanted; i++ {
				for _, statement := range []string{
					"CREATE INDEX w_v IF NOT EXISTS FOR (n:W) ON (n.v)",
					"DROP INDEX w_v IF EXISTS",
				} {
					total++
					capture.TakeBegin()
					before := capture.PersisterWrites()
					_, execErr := ddl.Execute(ctx, statement, nil)
					begin := capture.TakeBegin()
					if execErr == nil {
						if baselines < 2 && capture.PersisterWrites() > before {
							baselines++
							t.Logf("GAPS17 ACCEPTED (baseline) round=%d %q persister_calls_during_statement=%d recent=[%s]", rounds, statement, capture.PersisterWrites()-before, capture.RecentWrites(capture.PersisterWrites()-before))
						}
						continue
					}
					refused++
					t.Logf("GAPS17 parser=%s REFUSED #%d round=%d statement=%d %q err=%q persister_calls_during_statement=%d %s",
						config.GetParserType(), refused, rounds, total, statement, execErr.Error(), capture.PersisterWrites()-before, capture.Describe(begin, name, ddlGID))
				}
			}
		}()
	}
	t.Logf("GAPS17 summary parser=%s refused=%d statements=%d rounds=%d writer_commits=%d", config.GetParserType(), refused, total, rounds, writes)
	if writes == 0 {
		t.Errorf("harness: the writers committed nothing")
	}
}

// TestGapS17DropRefusal: the same workload, run until DROP INDEX refusals are
// captured. Phase "e17": the E17 workload unchanged (writers CREATE (:W)),
// up to 15,000 statements or 3 DROP refusals. Phase "new-labels": each writer
// moves to a new label every 50 writes (CREATE (:W_<w>_<n> {v})), up to 6,000
// statements or 3 DROP refusals. Every DROP refusal is logged with the writer
// of the conflicting schema-record version; CREATE refusals are counted and
// the first two per phase logged.
func TestGapS17DropRefusal(t *testing.T) {
	gapS17Phase(t, "e17", func(w, i int) string { return "W" }, 15000, 3)
	gapS17Phase(t, "new-labels", func(w, i int) string { return fmt.Sprintf("W_%d_%d", w, i/50) }, 6000, 3)
}

func gapS17Phase(t *testing.T, phase string, label func(w, i int) string, maxStatements, wantDrops int) {
	ctx := context.Background()
	const pairs, writers = 150, 4
	drops, creates, total, rounds, writes := 0, 0, 0, 0, 0
	ddlGID := storage.GapS17GoroutineID()
	for drops < wantDrops && total < maxStatements {
		rounds++
		func() {
			cfg := nornicdb.DefaultConfig()
			cfg.Memory.DecayEnabled = false
			cfg.Database.AsyncWritesEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.StrictDurability = true
			cfg.Database.WALSyncMode = "immediate"
			db, err := nornicdb.Open(t.TempDir(), cfg)
			if err != nil {
				t.Fatalf("nornicdb.Open: %v", err)
			}
			defer func() { _ = db.Close() }()
			capture, err := storage.NewGapS17Engine(db.GetBaseStorageForManager())
			if err != nil {
				t.Fatalf("seam: %v", err)
			}
			manager, err := multidb.NewDatabaseManager(ctx, capture, nil)
			if err != nil {
				t.Fatalf("database manager: %v", err)
			}
			defer func() { _ = manager.Close() }()
			name := manager.DefaultDatabaseName()
			engine, err := manager.GetStorage(name)
			if err != nil {
				t.Fatalf("GetStorage: %v", err)
			}
			capture.InstallPersisterRecorder(name)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					exec := cypher.NewStorageExecutor(engine)
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						if _, err := exec.Execute(ctx, "CREATE (:"+label(w, i)+" {v: $v})", map[string]interface{}{"v": int64(w*1000000 + i)}); err == nil {
							mu.Lock()
							writes++
							mu.Unlock()
						}
					}
				}(w)
			}
			defer func() { close(stop); wg.Wait() }()
			ddl := cypher.NewStorageExecutor(engine)
			for i := 0; i < pairs && drops < wantDrops; i++ {
				for _, statement := range []string{
					"CREATE INDEX w_v IF NOT EXISTS FOR (n:W) ON (n.v)",
					"DROP INDEX w_v IF EXISTS",
				} {
					total++
					capture.TakeBegin()
					before := capture.PersisterWrites()
					_, execErr := ddl.Execute(ctx, statement, nil)
					begin := capture.TakeBegin()
					if execErr == nil {
						continue
					}
					isDrop := strings.HasPrefix(statement, "DROP")
					if isDrop {
						drops++
					} else {
						creates++
						if creates > 2 {
							continue
						}
					}
					t.Logf("GAPS17D phase=%s parser=%s REFUSED %s round=%d statement=%d %q err=%q persister_calls_during_statement=%d %s",
						phase, config.GetParserType(), map[bool]string{true: "DROP", false: "CREATE"}[isDrop], rounds, total, statement, execErr.Error(), capture.PersisterWrites()-before, capture.Describe(begin, name, ddlGID))
				}
			}
		}()
	}
	t.Logf("GAPS17D summary phase=%s parser=%s drop_refused=%d create_refused=%d statements=%d rounds=%d writer_commits=%d", phase, config.GetParserType(), drops, creates, total, rounds, writes)
}
