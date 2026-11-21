package bolt

// Lane L6 (closeout): the store cells the earlier traces left open.
//   G-wal-replay-rebuild    UPG-RB2 Qdrant cells (b6/qdrant-snap-clean, -kill)
//   G-snapshot-wal-recovery UPG-RB3 Bolt large-document cells (b6/asyncdocs-snap-clean, -kill)
// Same steps as TestV152RCCRebuildReplayTrace (v152_rootcause_c_rebuild_test.go),
// applied to these cells, plus the original-journal raw probe for the Qdrant
// cells with the v1.5.1 stores of the same workload as control.

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

func TestL6RebuildReplayTraceCells(t *testing.T) {
	cells := []string{"qdrant-snap-clean", "qdrant-snap-kill", "asyncdocs-snap-clean", "asyncdocs-snap-kill"}
	if v := os.Getenv("ZZL6_CELLS"); v != "" {
		cells = strings.Split(v, ",")
	}
	for _, name := range cells {
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(uprRoot, "b6", "uor", "stores", name)
			raw, err := os.ReadFile(filepath.Join(src, "expected.json"))
			require.NoError(t, err)
			var want uprState
			require.NoError(t, json.Unmarshal(raw, &want))
			work := filepath.Join(uprRoot, "read-l6", name)
			require.NoError(t, os.RemoveAll(work))
			defer os.RemoveAll(work)
			data := filepath.Join(work, "data")
			uprCopy(t, filepath.Join(src, "data"), data)
			tag := "b6/" + name

			v152RCCSnapshots(tag+" old", data)
			_, oldErr := v152RCCWAL(tag+" old", filepath.Join(data, "wal"), nil)
			fmt.Printf("L6_REBUILD_OLD_WAL_READ %s err=%v\n", tag, oldErr)

			db, err := uprOpen(t, tag+" upgrade", name, data, true)
			require.NoError(t, err)
			gotUpgraded := uprCapture(t, db.GetBaseStorageForManager())
			require.NoError(t, db.Close())
			mn, me, mc := v152RCCMissing(want, gotUpgraded)
			fmt.Printf("L6_REBUILD_UPGRADED_LIVE %s missing_nodes=%d missing_edges=%d changed=%d\n", tag, len(mn), len(me), len(mc))

			snapshot := v152RCCSnapshots(tag+" upgraded", data)
			walDir := filepath.Join(data, "wal")
			replayed, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			result, status, recErr := storage.RecoverIntoEngine(context.Background(), replayed, walDir, snapshot)
			rn, re, rc := v152RCCMissing(want, uprCapture(t, replayed))
			require.NoError(t, replayed.Close())
			probes := append(append(append([]string{}, rn...), re...), rc...)
			fmt.Printf("L6_REBUILD_PROBES %s ids=%v\n", tag, probes)
			entries, walErr := v152RCCWAL(tag+" upgraded", walDir, probes)
			require.NoError(t, walErr)
			visitedFrom := uint64(0)
			if int(status.WALEntries) <= len(entries) && status.WALEntries > 0 {
				visitedFrom = entries[len(entries)-int(status.WALEntries)].seq
			}
			fmt.Printf("L6_REBUILD_REPLAY %s err=%v snapshot=%s snapshot_nodes=%d snapshot_edges=%d wal_entries_visited=%d first_visited_seq=%d applied=%d skipped=%d failed=%d missing_nodes=%d missing_edges=%d changed=%d\n",
				tag, recErr, filepath.Base(snapshot), status.SnapshotNodes, status.SnapshotEdges, status.WALEntries, visitedFrom, result.Applied, result.Skipped, result.Failed, len(rn), len(re), len(rc))
			errClass := map[string]int{}
			for _, e := range result.Errors {
				msg := fmt.Sprint(e.Error)
				class := msg
				if i := strings.Index(msg, " token "); i > 0 {
					class = msg[:i]
				}
				errClass[fmt.Sprintf("%s|%s", e.Operation, class)]++
				fmt.Printf("L6_REBUILD_REPLAY_ERROR %s seq=%d op=%s err=%v\n", tag, e.Sequence, e.Operation, e.Error)
			}
			fmt.Printf("L6_REBUILD_REPLAY_ERROR_CLASSES %s %v\n", tag, errClass)
			for _, e := range entries {
				if len(e.targets) > 0 {
					fmt.Printf("L6_REBUILD_TARGET_VISITED %s seq=%d op=%s visited=%v\n", tag, e.seq, e.op, status.WALEntries > 0 && e.seq >= visitedFrom)
				}
			}
			uprDamage(t, data)
			db, err = uprOpen(t, tag+" rebuild", name, data, true)
			if err != nil {
				fmt.Printf("L6_REBUILD_REAL %s refused err=%v\n", tag, err)
				return
			}
			bn, be, bc := v152RCCMissing(want, uprCapture(t, db.GetBaseStorageForManager()))
			require.NoError(t, db.Close())
			fmt.Printf("L6_REBUILD_REAL %s missing_nodes=%d missing_edges=%d changed=%d\n", tag, len(bn), len(be), len(bc))
		})
	}
}

func TestL6OldJournalRawProbeQdrant(t *testing.T) {
	for _, build := range []string{"b6", "v151"} {
		for _, name := range []string{"qdrant-snap-clean", "qdrant-snap-kill"} {
			t.Run(build+"/"+name, func(t *testing.T) {
				src := filepath.Join(uprRoot, build, "uor", "stores", name)
				raw, err := os.ReadFile(filepath.Join(src, "expected.json"))
				if os.IsNotExist(err) {
					t.Skipf("no store %s", src)
				}
				require.NoError(t, err)
				var want uprState
				require.NoError(t, json.Unmarshal(raw, &want))
				var journal []byte
				files, _ := filepath.Glob(filepath.Join(src, "data", "wal", "*"))
				sort.Strings(files)
				for _, f := range files {
					if info, err := os.Stat(f); err != nil || info.IsDir() {
						continue
					}
					body, err := os.ReadFile(f)
					require.NoError(t, err)
					journal = append(journal, body...)
				}
				type tally struct{ found, total int }
				byKind := map[string]*tally{}
				count := func(kind, id string) {
					local := id[strings.Index(id, ":")+1:]
					tl := byKind[kind]
					if tl == nil {
						tl = &tally{}
						byKind[kind] = tl
					}
					tl.total++
					if bytes.Contains(journal, []byte(local)) {
						tl.found++
					}
				}
				for _, n := range want.Nodes {
					count("node:"+strings.Join(n.Labels, ","), n.ID)
				}
				for _, e := range want.Edges {
					count("edge:"+e.Type, e.ID)
				}
				kinds := make([]string, 0, len(byKind))
				for k := range byKind {
					kinds = append(kinds, k)
				}
				sort.Strings(kinds)
				for _, k := range kinds {
					fmt.Printf("L6_OLDWAL_KIND %s/%s %s in_journal=%d of=%d\n", build, name, k, byKind[k].found, byKind[k].total)
				}
				fmt.Printf("L6_OLDWAL_SUMMARY %s/%s journal_bytes=%d wale_frames=%d\n", build, name, len(journal), bytes.Count(journal, []byte("WALE")))
			})
		}
	}
}
