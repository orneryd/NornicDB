package bolt

// Lane C root-cause diagnostic (research, not for merge): are the entities the
// rebuild of an upgraded b6d8e762a store loses present in the store's ORIGINAL
// journal at all? The original files are only read, never opened as a store.
// A v1.5.1-written store of the same workload is the control.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV152RCCOldJournalRawProbe(t *testing.T) {
	for _, build := range []string{"b6", "v151"} {
		for _, name := range []string{"api-snap-clean", "api-snap-kill", "async-snap-clean"} {
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
					info, err := os.Stat(f)
					if err != nil || info.IsDir() {
						continue
					}
					body, err := os.ReadFile(f)
					require.NoError(t, err)
					head := body
					if len(head) > 16 {
						head = head[:16]
					}
					fmt.Printf("RCC_OLDWAL_FILE %s/%s %s size=%d head=%s\n", build, name, filepath.Base(f), len(body), hex.EncodeToString(head))
					journal = append(journal, body...)
				}
				type tally struct{ found, total int }
				byKind := map[string]*tally{}
				var missingIDs []string
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
					} else {
						missingIDs = append(missingIDs, kind+"="+local)
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
					fmt.Printf("RCC_OLDWAL_KIND %s/%s %s in_journal=%d of=%d\n", build, name, k, byKind[k].found, byKind[k].total)
				}
				fmt.Printf("RCC_OLDWAL_SUMMARY %s/%s journal_bytes=%d label_After_bytes=%d not_in_journal=%d\n", build, name, len(journal), bytes.Count(journal, []byte("After")), len(missingIDs))
			})
		}
	}
}
