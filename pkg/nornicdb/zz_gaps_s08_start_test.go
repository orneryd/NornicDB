package nornicdb

// Report gap S08 (v1.5.2 verification campaign, test-side only): the start of
// a damaged v1.5.0 asynchronous store, on the build under test. Identical file
// on the pin (e8cd436e5) and on v1.5.0 (c99e77131), so the v1.5.0 run shows
// whether v1.5.0 refuses its own damaged store too.
// Store: v1.5.0 "asyncdocs-snap-clean" (upgrade lane copy under
// /work/tnrun/v152-upg). Per iteration: copy, Open (storage upgrade allowed,
// asynchronous writes on), Close, write "damaged" over MANIFEST, Open again
// (the start-up auto-recovery from snapshot + journal): pass or refused.
// Env: GAPS08S_AREA (scratch dir under /work/tnrun/v152-upg), GAPS08S_N
// (iterations, default 12). Diagnostic: fails only on harness errors.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const gapS08StartSource = "/work/tnrun/v152-upg/v150/uor/stores/asyncdocs-snap-clean/data"

func gapS08StartConfig() *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = time.Hour
	cfg.Database.AllowStorageUpgrade = true
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.StrictDurability = false
	return cfg
}

func TestGapS08DamagedV150AsyncStart(t *testing.T) {
	area := os.Getenv("GAPS08S_AREA")
	if area == "" {
		t.Skip("GAPS08S_AREA not set")
	}
	n := 12
	if v, err := strconv.Atoi(os.Getenv("GAPS08S_N")); err == nil && v > 0 {
		n = v
	}
	root := filepath.Join("/work/tnrun/v152-upg", area)
	_ = os.RemoveAll(root)
	defer os.RemoveAll(root)
	refused, firstOpenFailed := 0, 0
	for i := 0; i < n; i++ {
		work := filepath.Join(root, strconv.Itoa(i))
		if err := os.MkdirAll(work, 0o755); err != nil {
			t.Fatal(err)
		}
		data := filepath.Join(work, "data")
		if out, err := exec.Command("cp", "-a", "--sparse=always", gapS08StartSource, data).CombinedOutput(); err != nil {
			t.Fatalf("copy: %v %s", err, out)
		}
		db, err := Open(data, gapS08StartConfig())
		if err != nil {
			firstOpenFailed++
			fmt.Printf("GAPS08S iter=%d first-open FAILED err=%v\n", i, err)
			_ = os.RemoveAll(work)
			continue
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if err := os.WriteFile(filepath.Join(data, "MANIFEST"), []byte("damaged"), 0o644); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		db2, err := Open(data, gapS08StartConfig())
		if err != nil {
			refused++
			fmt.Printf("GAPS08S iter=%d damaged-start REFUSED after %v err=%v\n", i, time.Since(start), err)
		} else {
			fmt.Printf("GAPS08S iter=%d damaged-start ok after %v\n", i, time.Since(start))
			_ = db2.Close()
		}
		_ = os.RemoveAll(work)
	}
	fmt.Printf("GAPS08S SUMMARY iterations=%d refused=%d first_open_failed=%d\n", n, refused, firstOpenFailed)
}
