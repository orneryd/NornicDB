package bolt

// Lane C root-cause diagnostic (research, not for merge) for the V4 upgrade
// refusal of b6d8e762a async-Bolt stores.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCHistoricalJSONKeyCollapse is a minimal reproduction of the old
// writer's encoding step (b6d8e762a pkg/storage/wal.go:711-736: Writes, a
// map[string][]byte of physical keys, marshalled as JSON): two label-index
// keys whose big-endian numeric IDs differ only in a byte >= 0x80 decode as
// one key. Control: IDs whose bytes are all < 0x80 survive.
func TestV152RCCHistoricalJSONKeyCollapse(t *testing.T) {
	key := func(id uint64) string {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], id)
		return string(append([]byte{0x03, 'L', 0x00}, b[:]...))
	}
	roundTrip := func(ids ...uint64) map[string][]byte {
		in := map[string][]byte{}
		for _, id := range ids {
			in[key(id)] = []byte(fmt.Sprint(id))
		}
		raw, err := json.Marshal(struct{ Writes map[string][]byte }{in})
		require.NoError(t, err)
		var out struct{ Writes map[string][]byte }
		require.NoError(t, json.Unmarshal(raw, &out))
		return out.Writes
	}
	control := roundTrip(0x01, 0x02)
	t.Logf("RCC_V4_JSON_CONTROL ids=0x01,0x02 keys_in=2 keys_out=%d exact=%v", len(control), control[key(1)] != nil && control[key(2)] != nil)
	require.Len(t, control, 2)
	trigger := roundTrip(0x80, 0x81)
	var outKeys []string
	for k, v := range trigger {
		outKeys = append(outKeys, fmt.Sprintf("%x=>%s", k, v))
	}
	sort.Strings(outKeys)
	t.Logf("RCC_V4_JSON_TRIGGER ids=0x80,0x81 keys_in=2 keys_out=%d decoded=%v", len(trigger), outKeys)
	assert.Len(t, trigger, 2, "distinct physical keys must survive the historical encoding")
}

var v152RCCNoBody = regexp.MustCompile(`node "([^"]+)" has no body`)

// TestV152RCCAsyncBoltNoBodyProbe opens copies of the b6 async-Bolt stores
// with storage upgrade allowed, reads the refusal at
// wal_v4_buffered_key_repair.go:290-293, and for every node it names reports
// how often its ID occurs in the original journal, the snapshot and the
// acknowledged state.
func TestV152RCCAsyncBoltNoBodyProbe(t *testing.T) {
	storesDir := filepath.Join(uprRoot, "b6", "uor", "stores")
	entries, err := os.ReadDir(storesDir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "asyncbolt") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(storesDir, name)
			read := func(glob string) []byte {
				var all []byte
				files, _ := filepath.Glob(glob)
				for _, f := range files {
					if info, err := os.Stat(f); err == nil && !info.IsDir() {
						b, err := os.ReadFile(f)
						require.NoError(t, err)
						all = append(all, b...)
					}
				}
				return all
			}
			journal := read(filepath.Join(src, "data", "wal", "*"))
			snapshot := read(filepath.Join(src, "data", "snapshots", "*"))
			expected, _ := os.ReadFile(filepath.Join(src, "expected.json"))
			work := filepath.Join(uprRoot, "read-rcc", "v4", name)
			require.NoError(t, os.RemoveAll(work))
			defer os.RemoveAll(work)
			data := filepath.Join(work, "data")
			uprCopy(t, filepath.Join(src, "data"), data)
			db, openErr := uprOpen(t, "b6/"+name+" upgrade", name, data, true)
			if openErr == nil {
				require.NoError(t, db.Close())
				fmt.Printf("RCC_V4_NOBODY b6/%s upgrade_opened=true\n", name)
				return
			}
			ids := map[string]bool{}
			for _, m := range v152RCCNoBody.FindAllStringSubmatch(openErr.Error(), -1) {
				ids[m[1]] = true
			}
			fmt.Printf("RCC_V4_NOBODY b6/%s upgrade_refused=true named_nodes=%d journal_bytes=%d\n", name, len(ids), len(journal))
			for id := range ids {
				local := id[strings.Index(id, ":")+1:]
				fmt.Printf("RCC_V4_NOBODY_NODE b6/%s id=%s journal_occurrences=%d snapshot_occurrences=%d in_expected=%v\n",
					name, id, bytes.Count(journal, []byte(local)), bytes.Count(snapshot, []byte(local)), bytes.Contains(expected, []byte(local)))
			}
		})
	}
}
