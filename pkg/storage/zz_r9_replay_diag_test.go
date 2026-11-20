package storage

// R9 delta diagnostic (campaign scratch test, not product code): replay the snapshot and journal of the
// v1.5.0 store asyncdocs-snap-clean, written by the upgrade lane, into a fresh Badger engine with
// RecoverIntoEngine (the call nornicdb's start-up recovery makes) and print every replay error.
// On the pin the rebuild of this store succeeded; on 89b9c363d it is refused with applied=55 skipped=1 failed=1.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

const r9DiagSource = "/work/tnrun/v152-upg/v150/uor/stores/asyncdocs-snap-clean/data"

func TestR9ReplayDiagV150AsyncdocsSnapClean(t *testing.T) {
	area := os.Getenv("R9_DIAG_AREA")
	if area == "" {
		t.Skip("R9_DIAG_AREA not set")
	}
	if _, err := os.Stat(r9DiagSource); err != nil {
		t.Fatalf("source store missing: %v", err)
	}
	work := filepath.Join("/work/tnrun/v152-upg", area)
	if err := os.RemoveAll(work); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(work)
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(work, "data")
	if out, err := exec.Command("cp", "-a", "--sparse=always", r9DiagSource, data).CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, out)
	}
	snaps, _ := filepath.Glob(filepath.Join(data, "snapshots", "snapshot-*"))
	sort.Strings(snaps)
	snap := ""
	if len(snaps) > 0 {
		snap = snaps[len(snaps)-1]
	}
	fmt.Printf("R9DIAG snapshots=%d using=%s\n", len(snaps), filepath.Base(snap))
	fresh, err := NewBadgerEngine(filepath.Join(work, "fresh"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	res, st, err := RecoverIntoEngine(context.Background(), fresh, filepath.Join(data, "wal"), snap)
	fmt.Printf("R9DIAG result applied=%d skipped=%d failed=%d err=%v snapshotNodes=%d snapshotEdges=%d walEntries=%d\n",
		res.Applied, res.Skipped, res.Failed, err, st.SnapshotNodes, st.SnapshotEdges, st.WALEntries)
	for _, e := range res.Errors {
		fmt.Printf("R9DIAG replay-error seq=%d op=%v err=%v\n", e.Sequence, e.Operation, e.Error)
	}
}
