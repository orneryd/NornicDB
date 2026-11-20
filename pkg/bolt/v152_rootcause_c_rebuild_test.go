package bolt

// Lane C root-cause diagnostic (research, not for merge) for the rebuild of an
// upgraded b6d8e762a store: which step drops the acknowledged post-snapshot
// writes. Uses the upgrade lane's reader helpers (zz_upg_read_test.go) and the
// stores under /work/tnrun/v152-upg/b6/uor/stores.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func v152RCCMissing(want, got uprState) (nodes, edges []string, changed []string) {
	gn := map[string]uprNode{}
	for _, n := range got.Nodes {
		gn[n.ID] = n
	}
	for _, w := range want.Nodes {
		g, ok := gn[w.ID]
		if !ok {
			nodes = append(nodes, w.ID)
		} else if g.PropsHash != w.PropsHash || strings.Join(g.Labels, ",") != strings.Join(w.Labels, ",") {
			changed = append(changed, w.ID)
		}
	}
	ge := map[string]bool{}
	for _, e := range got.Edges {
		ge[e.ID] = true
	}
	for _, w := range want.Edges {
		if !ge[w.ID] {
			edges = append(edges, w.ID)
		}
	}
	return
}

func v152RCCSnapshots(tag, data string) string {
	snaps, _ := filepath.Glob(filepath.Join(data, "snapshots", "snapshot-*"))
	sort.Strings(snaps)
	newest, newestMod := "", int64(0)
	for _, s := range snaps {
		info, err := os.Stat(s)
		if err != nil {
			continue
		}
		fmt.Printf("RCC_REBUILD_SNAPSHOT %s %s size=%d mod=%s\n", tag, filepath.Base(s), info.Size(), info.ModTime().UTC().Format("15:04:05.000"))
		if strings.HasSuffix(s, ".json") && info.ModTime().UnixNano() > newestMod {
			newest, newestMod = s, info.ModTime().UnixNano()
		}
	}
	return newest
}

type v152RCCEntry struct {
	seq     uint64
	op      storage.OperationType
	db      string
	targets []string
}

// v152RCCWAL lists every entry of walDir and which of the probe IDs (missing
// or changed entities, with and without the namespace prefix) its data names.
func v152RCCWAL(tag, walDir string, probes []string) ([]v152RCCEntry, error) {
	var out []v152RCCEntry
	ops := map[storage.OperationType]int{}
	err := storage.VisitWALEntriesAfterFromDir(context.Background(), walDir, 0, func(e storage.WALEntry) error {
		entry := v152RCCEntry{seq: e.Sequence, op: e.Operation, db: e.Database}
		for _, p := range probes {
			local := p[strings.Index(p, ":")+1:]
			if bytes.Contains(e.Data, []byte(p)) || bytes.Contains(e.Data, []byte(local)) {
				entry.targets = append(entry.targets, local)
			}
		}
		ops[e.Operation]++
		out = append(out, entry)
		return nil
	})
	first, last := uint64(0), uint64(0)
	if len(out) > 0 {
		first, last = out[0].seq, out[len(out)-1].seq
	}
	fmt.Printf("RCC_REBUILD_WAL %s entries=%d first_seq=%d last_seq=%d ops=%v err=%v\n", tag, len(out), first, last, ops, err)
	for _, e := range out {
		if len(e.targets) > 0 {
			fmt.Printf("RCC_REBUILD_WAL_TARGET %s seq=%d op=%s db=%q names=%v\n", tag, e.seq, e.op, e.db, e.targets)
		}
	}
	return out, err
}

func TestV152RCCRebuildReplayTrace(t *testing.T) {
	for _, name := range []string{"api-snap-clean", "api-snap-kill", "async-snap-clean", "async-snap-kill"} {
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(uprRoot, "b6", "uor", "stores", name)
			raw, err := os.ReadFile(filepath.Join(src, "expected.json"))
			require.NoError(t, err)
			var want uprState
			require.NoError(t, json.Unmarshal(raw, &want))
			work := filepath.Join(uprRoot, "read-rcc", name)
			require.NoError(t, os.RemoveAll(work))
			defer os.RemoveAll(work)
			data := filepath.Join(work, "data")
			uprCopy(t, filepath.Join(src, "data"), data)
			tag := "b6/" + name

			// 1. The old store as written: snapshot(s) and journal.
			v152RCCSnapshots(tag+" old", data)
			_, oldErr := v152RCCWAL(tag+" old", filepath.Join(data, "wal"), nil)
			fmt.Printf("RCC_REBUILD_OLD_WAL_READ %s err=%v\n", tag, oldErr)

			// 2. Upgrade; the upgraded live store before any damage.
			db, err := uprOpen(t, tag+" upgrade", name, data, true)
			require.NoError(t, err)
			gotUpgraded := uprCapture(t, db.GetBaseStorageForManager())
			require.NoError(t, db.Close())
			mn, me, mc := v152RCCMissing(want, gotUpgraded)
			fmt.Printf("RCC_REBUILD_UPGRADED_LIVE %s missing_nodes=%d missing_edges=%d changed=%d\n", tag, len(mn), len(me), len(mc))

			snapshot := v152RCCSnapshots(tag+" upgraded", data)
			walDir := filepath.Join(data, "wal")

			// 3. The exact recovery function on the upgraded store, offline.
			replayed, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			result, status, recErr := storage.RecoverIntoEngine(context.Background(), replayed, walDir, snapshot)
			rn, re, rc := v152RCCMissing(want, uprCapture(t, replayed))
			require.NoError(t, replayed.Close())
			probes := append(append(append([]string{}, rn...), re...), rc...)
			fmt.Printf("RCC_REBUILD_PROBES %s ids=%v\n", tag, probes)

			// 4. The upgraded journal: are the lost writes in it, and where?
			entries, walErr := v152RCCWAL(tag+" upgraded", walDir, probes)
			require.NoError(t, walErr)
			visitedFrom := uint64(0)
			if int(status.WALEntries) <= len(entries) && status.WALEntries > 0 {
				visitedFrom = entries[len(entries)-int(status.WALEntries)].seq
			}
			fmt.Printf("RCC_REBUILD_REPLAY %s err=%v snapshot=%s snapshot_nodes=%d snapshot_edges=%d wal_entries_visited=%d first_visited_seq=%d applied=%d skipped=%d failed=%d missing_nodes=%d missing_edges=%d changed=%d\n",
				tag, recErr, filepath.Base(snapshot), status.SnapshotNodes, status.SnapshotEdges, status.WALEntries, visitedFrom, result.Applied, result.Skipped, result.Failed, len(rn), len(re), len(rc))
			for _, e := range result.Errors {
				fmt.Printf("RCC_REBUILD_REPLAY_ERROR %s seq=%d op=%s err=%v\n", tag, e.Sequence, e.Operation, e.Error)
			}
			for _, e := range entries {
				if len(e.targets) > 0 {
					fmt.Printf("RCC_REBUILD_TARGET_VISITED %s seq=%d op=%s visited=%v\n", tag, e.seq, e.op, status.WALEntries > 0 && e.seq >= visitedFrom)
				}
			}

			// 5. Control: the real damaged-store start on the same upgraded copy.
			uprDamage(t, data)
			db, err = uprOpen(t, tag+" rebuild", name, data, true)
			if err != nil {
				fmt.Printf("RCC_REBUILD_REAL %s refused err=%v\n", tag, err)
				t.Errorf("%s: real rebuild refused; offline replay failed=%d", tag, result.Failed)
				return
			}
			bn, be, bc := v152RCCMissing(want, uprCapture(t, db.GetBaseStorageForManager()))
			require.NoError(t, db.Close())
			fmt.Printf("RCC_REBUILD_REAL %s missing_nodes=%d missing_edges=%d changed=%d\n", tag, len(bn), len(be), len(bc))
			if len(rn)+len(re)+len(rc) != 0 {
				t.Errorf("%s: offline RecoverIntoEngine on the upgraded store loses %d nodes, %d edges, changes %d", tag, len(rn), len(re), len(rc))
			}
		})
	}
}
