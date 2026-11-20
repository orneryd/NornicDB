package bolt

// v152 upgrade lane, part 2 (research reader, not for merge).
//  - TestV152TornRealRead: the b6d8e762a stores whose journal a real SIGKILL
//    left ending inside the oversized embedding-audit record (report entry 5,
//    the 20-kill case; writer zz_ei5_torn_real_test.go on b6d8e762a). One store
//    at a time: sparse copy, hashes, open with storage upgrade allowed, then a
//    retry or a second open; the copy is deleted before the next store.
//  - TestV152SnapshotRebuild: stores that hold an old-build snapshot followed
//    by journal records (the "-snap-" stores of every old build) rebuilt by
//    this build's automatic recovery (damaged store files at start: rebuild
//    from the newest snapshot plus the journal, pkg/nornicdb/db.go:809-819),
//    in two orders: A upgrade first, then damage and rebuild; B damage the old
//    store, then the first open with storage upgrade allowed. Every node and
//    relationship with every vector is compared with the old build's
//    acknowledged state, after the rebuild and at a second open.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestV152TornRealRead(t *testing.T) {
	dirs, _ := filepath.Glob(filepath.Join(uprRoot, "b6", "torn-real", "attempt-*", "data"))
	sort.Strings(dirs)
	fmt.Printf("UPR_TREAL stores=%d\n", len(dirs))
	if len(dirs) == 0 {
		t.Fatalf("no torn-real stores")
	}
	opened, refused := 0, 0
	for _, src := range dirs {
		name := filepath.Base(filepath.Dir(src))
		work := filepath.Join(uprRoot, uprReadDir, "torn-real", name, "data")
		require.NoError(t, os.RemoveAll(filepath.Dir(work)))
		uprCopy(t, src, work)
		before := uprHashes(work)
		ok := true
		for i, step := range []string{"first", "second"} {
			logPath := work + "." + step + ".log"
			logf, err := os.Create(logPath)
			require.NoError(t, err)
			start := time.Now()
			db, err := nornicdb.Open(work, uprTornConfig(logf))
			elapsed := time.Since(start)
			logf.Close()
			uprGrepLog("treal "+name+" "+step, logPath)
			fmt.Printf("UPR_TREAL_OPEN %s %s err=%v elapsed=%s\n", name, step, err, elapsed.Round(time.Millisecond))
			if err != nil {
				ok = false
				if i == 0 {
					fmt.Printf("UPR_TREAL_UNCHANGED_AFTER_REFUSAL %s %v\n", name, before == uprHashes(work))
				}
				continue
			}
			fmt.Printf("UPR_TREAL_STATE %s %s %s\n", name, step, uprTornState(db))
			require.NoError(t, db.Close())
		}
		if ok {
			opened++
		} else {
			refused++
			t.Errorf("%s: refused", name)
		}
		require.NoError(t, os.RemoveAll(filepath.Dir(work)))
	}
	fmt.Printf("UPR_TREAL_SUMMARY stores=%d opened=%d refused=%d\n", len(dirs), opened, refused)
}

func uprDamage(t testing.TB, data string) {
	require.NoError(t, os.WriteFile(filepath.Join(data, "MANIFEST"), []byte("damaged"), 0o644))
}

// uprRebuilt reports whether the start rebuilt the store (it keeps the damaged
// directory beside it) and prints the recovery manifest.
func uprRebuilt(tag, data string) bool {
	preserved, _ := filepath.Glob(data + ".corrupted-*")
	fmt.Printf("UPR_REBUILD_PRESERVED %s count=%d\n", tag, len(preserved))
	for _, p := range preserved {
		if raw, err := os.ReadFile(filepath.Join(p, "recovery-manifest.json")); err == nil {
			s := strings.Join(strings.Fields(string(raw)), " ")
			if len(s) > 800 {
				s = s[:800] + "..."
			}
			fmt.Printf("UPR_REBUILD_MANIFEST %s %s\n", tag, s)
		}
	}
	return len(preserved) > 0
}

func TestV152SnapshotRebuild(t *testing.T) {
	cases := uprSelectStores(t, func(name string) bool { return strings.Contains(name, "-snap-") })
	ran := map[string]bool{}
	var selected []string
	for _, c := range cases {
		build, storesDir, name := c.build, filepath.Dir(c.dir), c.name
		{
			for _, order := range []string{"A-upgrade-then-damage", "B-damage-then-upgrade"} {
				build, name, order := build, name, order
				selected = append(selected, build+"/"+name+"/"+order)
				t.Run(build+"/"+name+"/"+order, func(t *testing.T) {
					ran[build+"/"+name+"/"+order] = true
					src := filepath.Join(storesDir, name)
					raw, err := os.ReadFile(filepath.Join(src, "expected.json"))
					require.NoError(t, err)
					var want uprState
					require.NoError(t, json.Unmarshal(raw, &want))
					snaps, _ := filepath.Glob(filepath.Join(src, "data", "snapshots", "snapshot-*"))
					tag := build + "/" + name + " " + order
					fmt.Printf("UPR_REBUILD_SNAPSHOTS %s count=%d\n", tag, len(snaps))
					work := filepath.Join(uprRoot, uprReadDir, "rebuild", build, name+"-"+order[:1])
					require.NoError(t, os.RemoveAll(work))
					defer os.RemoveAll(work)
					data := filepath.Join(work, "data")
					uprCopy(t, filepath.Join(src, "data"), data)
					if _, err := os.Stat(filepath.Join(data, "MANIFEST")); err != nil {
						t.Fatalf("%s: no MANIFEST in the store directory: %v", tag, err)
					}
					if order[0] == 'A' {
						db, err := uprOpen(t, tag+" upgrade", name, data, true)
						if err != nil {
							fmt.Printf("UPR_REBUILD_RESULT %s step=upgrade REFUSED err=%v\n", tag, err)
							t.Errorf("%s: the upgrade itself is refused (entry 4), so this order does not reach the rebuild: %v", tag, err)
							return
						}
						require.NoError(t, db.Close())
					}
					uprDamage(t, data)
					db, err := uprOpen(t, tag+" rebuild", name, data, true)
					if err != nil {
						fmt.Printf("UPR_REBUILD_RESULT %s step=rebuild FAILED err=%v\n", tag, err)
						t.Errorf("%s: start on the damaged store failed: %v", tag, err)
						return
					}
					rebuilt := uprRebuilt(tag, data)
					if !rebuilt {
						t.Errorf("%s: the start did not rebuild the damaged store", tag)
					}
					m1, c1, e1 := uprCompare(tag+" rebuilt", want, uprCapture(t, db.GetBaseStorageForManager()))
					require.NoError(t, db.Close())
					db, err = uprOpen(t, tag+" second", name, data, true)
					if err != nil {
						fmt.Printf("UPR_REBUILD_RESULT %s step=second FAILED err=%v\n", tag, err)
						t.Errorf("%s: second open failed: %v", tag, err)
						return
					}
					m2, c2, e2 := uprCompare(tag+" second", want, uprCapture(t, db.GetBaseStorageForManager()))
					require.NoError(t, db.Close())
					fmt.Printf("UPR_REBUILD_RESULT %s rebuilt=%v missing=%d changed=%d extra=%d second_missing=%d second_changed=%d second_extra=%d\n", tag, rebuilt, m1, c1, e1, m2, c2, e2)
					if m1+c1+e1+m2+c2+e2 != 0 {
						t.Errorf("%s: acknowledged state differs after the rebuild", tag)
					}
				})
			}
		}
	}
	uprRequireRan(t, selected, ran)
}
