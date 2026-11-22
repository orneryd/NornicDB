//go:build closeout_l2_e31_b

package nornicdb_test

// L2 close-out, report entry 31, the parts other than the admin import
// (fixture A of the entry, steps 1, 2, 5 and 6):
//   (a) a server opened in low-memory mode (Storage.Mode = "low", 32 MiB
//       values) cannot update a node whose property the default server
//       accepted;
//   (b) a snapshot the server wrote holding that node does not load into a
//       store opened with the default engine profile (128 MiB values);
//   (c) nor into one opened with the low-memory profile (32 MiB).
// The high-performance profile (256 MiB, the server's default store) is the
// in-run reference: the same snapshot must load there, or the run is a
// harness failure.
//
// Public paths at the pin: nornicdb.Open with DefaultConfig (the server opens
// its store HighPerformance unless Storage.Mode is "low", db.go:1506-1511);
// the snapshot is the server's own automatic compaction snapshot
// (WALEngine.createSnapshotAndCompact, storage/wal_engine.go:419, written to
// <data>/snapshots/snapshot-*.json every WALSnapshotInterval, db.go:971-975),
// polled for with a deadline; the load is storage.RecoverIntoEngine
// (storage/wal_recovery_stream.go:30) from that snapshot and <data>/wal.
//
// Scaling: the value must be above the default profile's 128 MiB limit, so
// 130 MiB of repeated ASCII replaces the entry's 136 MiB base64 text. Peak
// memory a few GiB; a 6 GiB soft limit and explicit GCs between phases keep
// it under 8 GiB.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	l2E31bBigBytes       = 130 << 20
	l2E31bSmallBytes     = 1 << 20
	l2E31bSnapshotWait   = 120 * time.Second
	l2E31bSnapshotPeriod = time.Second
)

func l2E31bConfig(lowMemory, compaction bool) *nornicdb.Config {
	config := nornicdb.DefaultConfig()
	config.Memory.AutoLinksEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.WALAutoCompactionEnabled = compaction
	config.Database.WALSnapshotInterval = l2E31bSnapshotPeriod
	if lowMemory {
		config.Storage.Mode = "low"
	}
	return config
}

func l2E31bPayload(size int) string {
	block := strings.Repeat("l2-e31b-large-value-0123456789abcde", (64<<10)/35+1)
	var builder strings.Builder
	builder.Grow(size)
	for builder.Len() < size {
		builder.WriteString(block[:min(len(block), size-builder.Len())])
	}
	return builder.String()
}

func l2E31bRead(db *nornicdb.DB) (string, error) {
	result, err := db.ExecuteCypher(context.Background(), "MATCH (n:Big) RETURN n.id, size(n.blob)", nil)
	if err != nil {
		return "", err
	}
	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, fmt.Sprint(row...))
	}
	return strings.Join(rows, ","), nil
}

// l2E31bSnapshotAfter waits (bounded) for a snapshot file written after since.
func l2E31bSnapshotAfter(dir string, since time.Time) (string, time.Duration) {
	start := time.Now()
	for time.Since(start) < l2E31bSnapshotWait {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
				continue
			}
			if info, err := entry.Info(); err == nil && info.ModTime().After(since) {
				return filepath.Join(dir, name), time.Since(start)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", time.Since(start)
}

type l2E31bProfile struct {
	name    string
	options storage.BadgerOptions
}

type l2E31bJourney struct {
	source       string
	snapshot     string
	snapshotWait time.Duration
	recovered    map[string]string // profile -> "<Big count>" or the error
	lowRead      string
	lowUpdateErr error
	sys          uint64
}

func (j l2E31bJourney) String() string {
	return fmt.Sprintf("source=%q snapshot=%q (after %v) recovered=%v lowRead=%q lowUpdateErr=%v runtimeSys=%dMiB",
		j.source, filepath.Base(j.snapshot), j.snapshotWait.Round(time.Millisecond), j.recovered, j.lowRead, j.lowUpdateErr, j.sys>>20)
}

func l2E31bRun(t *testing.T, size int) l2E31bJourney {
	t.Helper()
	previous := debug.SetMemoryLimit(6 << 30)
	defer debug.SetMemoryLimit(previous)
	ctx := context.Background()
	journey := l2E31bJourney{recovered: map[string]string{}}
	dataDir := filepath.Join(t.TempDir(), "store")

	// Steps 1-2: the default server accepts the value and writes a snapshot.
	db, err := nornicdb.Open(dataDir, l2E31bConfig(false, true))
	if err != nil {
		t.Fatalf("open default server: %v", err)
	}
	if _, err := db.ExecuteCypher(ctx, "CREATE (:Small {id:'s1'})", nil); err != nil {
		_ = db.Close()
		t.Fatalf("create Small: %v", err)
	}
	payload := l2E31bPayload(size)
	_, err = db.ExecuteCypher(ctx, "CREATE (:Big {id:'big', blob:$b})", map[string]interface{}{"b": payload})
	payload = ""
	if err != nil {
		_ = db.Close()
		t.Fatalf("the default server refused the value of %d bytes: %v", size, err)
	}
	created := time.Now()
	journey.source, err = l2E31bRead(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("read source: %v", err)
	}
	journey.snapshot, journey.snapshotWait = l2E31bSnapshotAfter(filepath.Join(dataDir, "snapshots"), created)
	if err := db.Close(); err != nil {
		t.Fatalf("close default server: %v", err)
	}
	runtime.GC()

	// Step 6: load the server's snapshot (and its WAL) into fresh stores.
	if journey.snapshot != "" {
		for _, profile := range []l2E31bProfile{
			{"high-performance", storage.BadgerOptions{HighPerformance: true}},
			{"default", storage.BadgerOptions{}},
			{"low-memory", storage.BadgerOptions{LowMemory: true}},
		} {
			options := profile.options
			options.DataDir = filepath.Join(t.TempDir(), profile.name)
			engine, err := storage.NewBadgerEngineWithOptions(options)
			if err != nil {
				t.Fatalf("open %s engine: %v", profile.name, err)
			}
			_, _, recoverErr := storage.RecoverIntoEngine(ctx, engine, filepath.Join(dataDir, "wal"), journey.snapshot)
			if recoverErr != nil {
				journey.recovered[profile.name] = "error: " + recoverErr.Error()
			} else if count, countErr := engine.NodeCountByLabel(ctx, "Big"); countErr != nil {
				journey.recovered[profile.name] = "count error: " + countErr.Error()
			} else {
				journey.recovered[profile.name] = fmt.Sprint(count)
			}
			_ = engine.Close()
			runtime.GC()
		}
	}

	// Step 5: reopen the store as a low-memory server; read, then update.
	low, err := nornicdb.Open(dataDir, l2E31bConfig(true, false))
	if err != nil {
		t.Fatalf("open low-memory server: %v", err)
	}
	journey.lowRead, err = l2E31bRead(low)
	if err != nil {
		journey.lowRead = "error: " + err.Error()
	}
	_, journey.lowUpdateErr = low.ExecuteCypher(ctx, "MATCH (n:Big {id:'big'}) SET n.touched = 1", nil)
	_ = low.Close()
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	journey.sys = stats.Sys
	return journey
}

func TestL2_e31_b_Repro(t *testing.T) {
	journey := l2E31bRun(t, l2E31bBigBytes)
	want := fmt.Sprint("big", l2E31bBigBytes)
	figures := journey.String()
	// Harness preconditions: the server held the value, wrote a snapshot of
	// it, and the snapshot loads into the server's own profile.
	if journey.source != want {
		t.Fatalf("harness: the source does not hold the accepted value (want %q): %s", want, figures)
	}
	if journey.snapshot == "" {
		// The server wrote no snapshot within the deadline: the entry's
		// b6d8e762a outcome, where no snapshot holding the node was written.
		t.Logf("L2RESULT E31-b DEFECT no server snapshot within %v; %s", l2E31bSnapshotWait, figures)
		t.Fatalf("the server wrote no snapshot holding the node within %v: %s", l2E31bSnapshotWait, figures)
	}
	if journey.recovered["high-performance"] != "1" {
		t.Logf("L2RESULT E31-b DEFECT snapshot does not load even into the server's profile; %s", figures)
		t.Fatalf("the server's snapshot does not load into the high-performance profile: %s", figures)
	}
	var defects []string
	if journey.lowUpdateErr != nil {
		defects = append(defects, "(a) low-memory update refused")
	}
	if journey.recovered["default"] != "1" {
		defects = append(defects, "(b) default-profile load: "+journey.recovered["default"])
	}
	if journey.recovered["low-memory"] != "1" {
		defects = append(defects, "(c) low-memory-profile load: "+journey.recovered["low-memory"])
	}
	if len(defects) > 0 {
		t.Logf("L2RESULT E31-b DEFECT %s; %s", strings.Join(defects, "; "), figures)
		t.Errorf("a store the server accepted cannot be used by the other profiles: %s; %s", strings.Join(defects, "; "), figures)
		return
	}
	t.Logf("L2RESULT E31-b OK %s", figures)
}

// The control: the same journey with a 1 MiB value, inside every profile's
// limit: the server writes a snapshot, it loads into all three profiles, and
// the low-memory server updates the node.
func TestL2_e31_b_Control(t *testing.T) {
	journey := l2E31bRun(t, l2E31bSmallBytes)
	figures := journey.String()
	if journey.source != fmt.Sprint("big", l2E31bSmallBytes) || journey.snapshot == "" || journey.lowUpdateErr != nil ||
		journey.recovered["high-performance"] != "1" || journey.recovered["default"] != "1" || journey.recovered["low-memory"] != "1" {
		t.Fatalf("control journey failed: %s", figures)
	}
	t.Logf("L2RESULT E31-b CONTROL OK %s", figures)
}
