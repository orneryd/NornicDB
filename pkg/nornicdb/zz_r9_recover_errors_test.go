package nornicdb

// R9-1 diagnostic (campaign scratch test, no product change).
// Store: v1.5.0 "asyncdocs-snap-clean" written by the upgrade lane. Order A as in TestV152SnapshotRebuild:
// open with storage upgrade allowed, close, write "damaged" over MANIFEST, start again.
// Per iteration, on two identical damaged copies:
//   REAL    nornicdb.Open on copy 1 (the start-up auto-recovery the reader exercises): pass/fail.
//   REPLICA the steps of recoverBadgerFromSnapshotAndWAL (storage_recovery.go) on copy 2, with the
//           options Open uses (resolveDurabilityOptions + CatalogUpgradeV4 + memory policy), printing
//           every ReplayResult error and the journal entry it belongs to.
// Env: R9_ROOT (store tree, default /work/tnrun/v152-upg, the lab tree), R9_DIAG_STORE (scenario directory
// below R9_ROOT, default v150/uor/stores/asyncdocs-snap-clean; the test adds data/), R9_DIAG_AREA (scratch
// directory below R9_ROOT; default a fresh r9diag-* directory), R9_DIAG_N (iterations, default 12).
// Fail-closed: a missing store fails the test; the test fails when any start is refused.

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


var r9RecSource string

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
	if err := r9CopyDir(r9RecSource, dst); err != nil {
		t.Fatalf("copy: %v", err)
	}
}

func TestR9RecoverErrorsV150AsyncdocsSnapClean(t *testing.T) {
	storeRoot := os.Getenv("R9_ROOT")
	if storeRoot == "" {
		storeRoot = "/work/tnrun/v152-upg"
	}
	store := os.Getenv("R9_DIAG_STORE")
	if store == "" {
		store = "v150/uor/stores/asyncdocs-snap-clean"
	}
	r9RecSource = filepath.Join(storeRoot, store, "data")
	if st, err := os.Stat(filepath.Join(r9RecSource, "MANIFEST")); err != nil || st.IsDir() {
		t.Fatalf("selected store is missing or incomplete (no data/MANIFEST): %s (%v)", r9RecSource, err)
	}
	n := 12
	if v, err := strconv.Atoi(os.Getenv("R9_DIAG_N")); err == nil && v > 0 {
		n = v
	}
	fmt.Printf("R9REC store=%s\n", r9RecSource)
	var root string
	if area := os.Getenv("R9_DIAG_AREA"); area != "" {
		root = filepath.Join(storeRoot, area)
	} else {
		var err error
		if root, err = os.MkdirTemp(storeRoot, "r9diag-"); err != nil {
			t.Fatalf("scratch area below %s: %v", storeRoot, err)
		}
	}
	realFail, replicaFail, wboffFail := 0, 0, 0
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
			t.Errorf("iteration %d: the store cannot be opened with storage upgrade allowed: %v", i, err)
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
		wboff := filepath.Join(work, "wboff")
		for _, d := range []string{real, replica, wboff} {
			if err := r9CopyDir(base, d); err != nil {
				t.Fatalf("copy damaged: %v", err)
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

		// REPLICA: recoverBadgerFromSnapshotAndWAL's steps with Open's options, printing the errors;
		// REPLICA-WBOFF: the same with write-behind disabled (everything else equal).
		runReplica := func(replica, label string, writeBehind bool) int {
		cfg := r9RecConfig(true)
		opts, _ := resolveDurabilityOptions(replica, cfg)
		if !writeBehind {
			opts.WriteBehind = false
		}
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
		defer fresh.Close()
		res, st, rerr2 := storage.RecoverIntoEngine(context.Background(), fresh, filepath.Join(backup, "wal"), snap)
		fmt.Printf("R9REC iter=%d %s applied=%d skipped=%d failed=%d err=%v snapshotNodes=%d snapshotEdges=%d walEntries=%d writeBehind=%v\n",
			i, label, res.Applied, res.Skipped, res.Failed, rerr2, st.SnapshotNodes, st.SnapshotEdges, st.WALEntries, opts.WriteBehind)
		for _, e := range res.Errors {
			fmt.Printf("R9REC iter=%d %s-ERROR seq=%d op=%v err=%v\n", i, label, e.Sequence, e.Operation, e.Error)
			_ = storage.VisitWALEntriesAfterFromDir(context.Background(), filepath.Join(backup, "wal"), e.Sequence-1, func(w storage.WALEntry) error {
				if w.Sequence == e.Sequence {
					data := string(w.Data)
					if len(data) > 300 {
						data = data[:300] + "...(" + strconv.Itoa(len(w.Data)) + " bytes)"
					}
					fmt.Printf("R9REC iter=%d %s-ENTRY seq=%d op=%v db=%q data=%s\n", i, label, w.Sequence, w.Operation, w.Database, strings.ReplaceAll(data, "\n", " "))
				}
				return nil
			})
		}
		return res.Failed
		}
		if runReplica(replica, "REPLICA", true) > 0 {
			replicaFail++
		}
		if runReplica(wboff, "REPLICA-WBOFF", false) > 0 {
			wboffFail++
		}
		if err := os.RemoveAll(work); err != nil {
			t.Logf("cleanup %s: %v", work, err)
		}
	}
	fmt.Printf("R9REC SUMMARY iterations=%d real_failed=%d replica_failed=%d replica_wboff_failed=%d\n", n, realFail, replicaFail, wboffFail)
	_ = os.RemoveAll(root)
	if realFail > 0 {
		t.Errorf("the damaged store was refused at start in %d of %d starts", realFail, n)
	}
}

// r9CopyDir copies the directory src to dst (dst must not exist). Method: UPR_COPY=gnu (cp -a --sparse=always),
// gcp (gcp -a --sparse=always, GNU cp as installed on macOS/BSD), or plain (cp -R -p, no sparse files: the copy
// takes the store's apparent size on disk). Unset, the first method present that succeeds is used.
func r9CopyDir(src, dst string) error {
	methods := map[string][]string{
		"gnu":   {"cp", "-a", "--sparse=always", src, dst},
		"gcp":   {"gcp", "-a", "--sparse=always", src, dst},
		"plain": {"cp", "-R", "-p", src, dst},
	}
	order := []string{"gnu", "gcp", "plain"}
	if m := os.Getenv("UPR_COPY"); m != "" {
		if _, ok := methods[m]; !ok {
			return fmt.Errorf("UPR_COPY=%q: want gnu, gcp or plain", m)
		}
		order = []string{m}
	}
	var last error
	for _, m := range order {
		args := methods[m]
		if _, err := exec.LookPath(args[0]); err != nil {
			last = fmt.Errorf("copy method %s: %v", m, err)
			continue
		}
		_ = os.RemoveAll(dst)
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err == nil {
			return nil
		}
		last = fmt.Errorf("copy method %s (%s): %v: %s", m, strings.Join(args[:len(args)-2], " "), err, out)
	}
	return last
}
