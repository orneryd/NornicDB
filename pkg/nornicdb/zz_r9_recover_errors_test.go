package nornicdb

// R9-1 diagnostic (campaign scratch test, no product change).
// Store: v1.5.0 "asyncdocs-snap-clean" written by the upgrade lane. Order A as in TestV152SnapshotRebuild:
// open with storage upgrade allowed, close, write "damaged" over MANIFEST, start again.
// Per iteration, on two identical damaged copies:
//   REAL    nornicdb.Open on copy 1 (the start-up auto-recovery the reader exercises): pass/fail.
//   REPLICA the steps of recoverBadgerFromSnapshotAndWAL (storage_recovery.go) on copy 2, with the
//           options Open uses (resolveDurabilityOptions + CatalogUpgradeV4 + memory policy), printing
//           every ReplayResult error and the journal entry it belongs to.
// Env: R9_DIAG_AREA (scratch dir under /work/tnrun/v152-upg), R9_DIAG_N (iterations, default 12).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const r9RecSource = "/work/tnrun/v152-upg/v150/uor/stores/asyncdocs-snap-clean/data"

func r9RecConfig(upgrade bool) *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = time.Hour
	cfg.Database.AllowStorageUpgrade = upgrade
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.StrictDurability = false
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return cfg
}

func r9RecCopy(t *testing.T, dst string) {
	t.Helper()
	if out, err := exec.Command("cp", "-a", "--sparse=always", r9RecSource, dst).CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, out)
	}
}

func TestR9RecoverErrorsV150AsyncdocsSnapClean(t *testing.T) {
	area := os.Getenv("R9_DIAG_AREA")
	if area == "" {
		t.Skip("R9_DIAG_AREA not set")
	}
	n := 12
	if v, err := strconv.Atoi(os.Getenv("R9_DIAG_N")); err == nil && v > 0 {
		n = v
	}
	root := filepath.Join("/work/tnrun/v152-upg", area)
	realFail, replicaFail := 0, 0
	for i := 0; i < n; i++ {
		work := filepath.Join(root, strconv.Itoa(i))
		if err := os.RemoveAll(work); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(work, 0o755); err != nil {
			t.Fatal(err)
		}
		base := filepath.Join(work, "base")
		r9RecCopy(t, base)
		// Order A: upgrade, close, damage.
		db, err := Open(base, r9RecConfig(true))
		if err != nil {
			fmt.Printf("R9REC iter=%d upgrade-open-failed err=%v\n", i, err)
			os.RemoveAll(work)
			continue
		}
		credKey := db.remoteCredentialKey
		memPolicy := db.memoryPolicy
		if err := db.Close(); err != nil {
			t.Fatalf("close after upgrade: %v", err)
		}
		if err := os.WriteFile(filepath.Join(base, "MANIFEST"), []byte("damaged"), 0o644); err != nil {
			t.Fatal(err)
		}
		real := filepath.Join(work, "real")
		replica := filepath.Join(work, "replica")
		for _, d := range []string{real, replica} {
			if out, err := exec.Command("cp", "-a", "--sparse=always", base, d).CombinedOutput(); err != nil {
				t.Fatalf("copy damaged: %v %s", err, out)
			}
		}

		// REAL: the start-up path.
		rdb, rerr := Open(real, r9RecConfig(true))
		if rerr != nil {
			realFail++
			fmt.Printf("R9REC iter=%d REAL FAILED err=%v\n", i, rerr)
		} else {
			fmt.Printf("R9REC iter=%d REAL ok\n", i)
			_ = rdb.Close()
		}

		// REPLICA: recoverBadgerFromSnapshotAndWAL's steps with Open's options, printing the errors.
		cfg := r9RecConfig(true)
		opts, _ := resolveDurabilityOptions(replica, cfg)
		up, err := multidb.NewCatalogUpgradeV4("system", credKey)
		if err != nil {
			t.Fatal(err)
		}
		opts.CatalogUpgradeV4 = up
		opts.MemoryPolicy = memPolicy
		snap, snapErr := latestSnapshotPath(filepath.Join(replica, "snapshots"))
		if snapErr != nil {
			snap = ""
		}
		backup := replica + ".corrupted"
		if err := os.Rename(replica, backup); err != nil {
			t.Fatal(err)
		}
		if snap != "" {
			rel, _ := filepath.Rel(replica, snap)
			snap = filepath.Join(backup, rel)
		}
		if err := os.MkdirAll(replica, 0o755); err != nil {
			t.Fatal(err)
		}
		opts.DataDir = replica
		fresh, err := storage.NewBadgerEngineWithOptions(opts)
		if err != nil {
			t.Fatalf("fresh store: %v", err)
		}
		res, st, rerr2 := storage.RecoverIntoEngine(context.Background(), fresh, filepath.Join(backup, "wal"), snap)
		fmt.Printf("R9REC iter=%d REPLICA applied=%d skipped=%d failed=%d err=%v snapshotNodes=%d snapshotEdges=%d walEntries=%d writeBehind=%v\n",
			i, res.Applied, res.Skipped, res.Failed, rerr2, st.SnapshotNodes, st.SnapshotEdges, st.WALEntries, opts.WriteBehind)
		if res.Failed > 0 {
			replicaFail++
		}
		for _, e := range res.Errors {
			fmt.Printf("R9REC iter=%d REPLICA-ERROR seq=%d op=%v err=%v\n", i, e.Sequence, e.Operation, e.Error)
			_ = storage.VisitWALEntriesAfterFromDir(context.Background(), filepath.Join(backup, "wal"), e.Sequence-1, func(w storage.WALEntry) error {
				if w.Sequence == e.Sequence {
					data := string(w.Data)
					if len(data) > 600 {
						data = data[:600] + "...(" + strconv.Itoa(len(w.Data)) + " bytes)"
					}
					fmt.Printf("R9REC iter=%d REPLICA-ENTRY seq=%d op=%v db=%q data=%s\n", i, w.Sequence, w.Operation, w.Database, strings.ReplaceAll(data, "\n", " "))
				}
				return nil
			})
		}
		_ = fresh.Close()
		if err := os.RemoveAll(work); err != nil {
			t.Logf("cleanup %s: %v", work, err)
		}
	}
	fmt.Printf("R9REC SUMMARY iterations=%d real_failed=%d replica_failed=%d\n", n, realFail, replicaFail)
	_ = os.RemoveAll(root)
}
