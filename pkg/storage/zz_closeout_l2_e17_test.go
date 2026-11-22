//go:build closeout_l2_e17

package storage_test

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E17 Cause2: the E17 workload (4 writers CREATE (:W {v:$v}) in autocommit,
// the main goroutine CREATE INDEX / DROP INDEX in autocommit) through the
// Cypher executor on a persistent Badger engine in namespace "nornic", with
// the engine wrapped (L2E17CaptureEngine, zz_closeout_l2_e17_export_test.go)
// so each schema statement's begin is recorded. For every refused statement
// it logs the decisive values of both commit checks: the schema snapshot
// sequence pair, and the Badger versions committed above the statement's
// snapshot, split into "lag" (assigned before the statement's begin, missing
// from its snapshot) and "window" (assigned while it held the exclusive
// barrier). Server, HTTP, WAL and strict durability are left out: the
// refusals come from storage (the error texts name storage's two checks).
// Diagnostic only; it fails only on harness errors.

const (
	l2E17C2Writers = 4
	l2E17C2Pairs   = 150
)

func TestL2_e17_Cause2(t *testing.T) {
	ctx := context.Background()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	capture := storage.NewL2E17CaptureEngine(base)
	ns := storage.NewNamespacedEngine(capture, "nornic")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	writes, writeErrors := 0, 0
	for w := 0; w < l2E17C2Writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			exec := cypher.NewStorageExecutor(ns)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, err := exec.Execute(ctx, "CREATE (:W {v: $v})", map[string]interface{}{"v": int64(w*1000000 + i)})
				mu.Lock()
				if err == nil {
					writes++
				} else {
					writeErrors++
				}
				mu.Unlock()
			}
		}(w)
	}

	main := cypher.NewStorageExecutor(ns)
	refused, total, baselines := 0, 0, 0
	for i := 0; i < l2E17C2Pairs; i++ {
		for _, statement := range []string{
			"CREATE INDEX w_v IF NOT EXISTS FOR (n:W) ON (n.v)",
			"DROP INDEX w_v IF EXISTS",
		} {
			total++
			capture.L2E17TakeSchemaBegin()
			_, execErr := main.Execute(ctx, statement, nil)
			_, endAssigned, _ := capture.L2E17OracleState()
			begin, begins := capture.L2E17TakeSchemaBegin()
			if execErr != nil {
				refused++
				if refused <= 12 {
					t.Logf("L2CAUSE E17 parser=%s REFUSED #%d %q err=%q schema_begins=%d %s",
						config.GetParserType(), refused, statement, execErr.Error(), begins, capture.L2E17Describe(begin, "nornic", endAssigned))
				}
				continue
			}
			if baselines < 3 && begin != nil {
				baselines++
				t.Logf("L2CAUSE E17 parser=%s ACCEPTED (baseline) %q schema_begins=%d %s",
					config.GetParserType(), statement, begins, capture.L2E17Describe(begin, "nornic", endAssigned))
			}
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("L2CAUSE E17 summary parser=%s refused=%d/%d writer_commits=%d writer_errors=%d", config.GetParserType(), refused, total, writes, writeErrors)
	if writes == 0 {
		t.Errorf("harness: the writers committed nothing")
	}
}
