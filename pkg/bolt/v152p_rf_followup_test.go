package bolt

// Research only (refusal observations, follow-up).
// TestZZRFHAStored: in HA primary mode the Bolt server built here answers
// MATCH with "not implemented", so what is stored is read through the
// embedded statement API (db.ExecuteCypher) and directly from the base
// storage (storage.CollectStoredNodes), before and after a clean reopen.
// TestZZRFRecoverProfiles: a 142,606,336-character value written with
// defaults; the snapshot (the server's own if one appears within 300 s,
// otherwise one written by the probe with storage.SaveStreamingSnapshot at the
// journal's current number) plus the journal are recovered into a store of
// each profile.
import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func rffStored(tag string, db *nornicdb.DB) {
	res, err := db.ExecuteCypher(context.Background(), "MATCH (n) RETURN labels(n), n.id, size(n.blob) ORDER BY n.id", nil)
	if err != nil {
		fmt.Printf("RFF %s ExecuteCypher err=%v\n", tag, err)
	} else {
		fmt.Printf("RFF %s ExecuteCypher rows=%v\n", tag, res.Rows)
	}
	nodes, err := storage.CollectStoredNodes(context.Background(), db.GetBaseStorageForManager())
	var found []string
	for _, n := range nodes {
		size := 0
		if s, ok := n.Properties["blob"].(string); ok {
			size = len(s)
		}
		found = append(found, fmt.Sprintf("%s%v:%v:%d", n.ID, n.Labels, n.Properties["id"], size))
	}
	sort.Strings(found)
	fmt.Printf("RFF %s base storage %T stored nodes=%d err=%v %s\n", tag, db.GetBaseStorageForManager(), len(nodes), err, strings.Join(found, " "))
}

func TestZZRFHAStored(t *testing.T) {
	t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
	t.Setenv("NORNICDB_CLUSTER_HA_ROLE", "primary")
	t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "127.0.0.1:1")
	over := rfbBlob(1, 16<<20)
	under := rfbBlob(2, 6<<20)
	root := t.TempDir()
	cfg := rfbConfig(false)
	l, err := rfbOpen(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.run("CREATE (:Small {id:'s1'})", nil, false)
	fmt.Printf("RFF ha small err=%v\n", err)
	_, err = l.run("CREATE (:Under {id:'u1', blob:$b})", map[string]any{"b": under}, false)
	fmt.Printf("RFF ha under-frame %d chars err=%v\n", len(under), err)
	_, err = l.run("CREATE (:Big {id:'b-implicit', blob:$b})", map[string]any{"b": over}, false)
	fmt.Printf("RFF ha over-frame %d chars implicit err=%v\n", len(over), err)
	_, err = l.run("CREATE (:Big {id:'b-explicit', blob:$b})", map[string]any{"b": over}, true)
	fmt.Printf("RFF ha over-frame %d chars explicit err=%v\n", len(over), err)
	_, err = l.run("CREATE (:Small {id:'s2'})", nil, false)
	fmt.Printf("RFF ha small-after err=%v\n", err)
	time.Sleep(2 * time.Second)
	rffStored("ha before-close", l.db)
	l.close()
	db, err := nornicdb.Open(root, cfg)
	if err != nil {
		fmt.Printf("RFF ha reopen error: %v\n", err)
		return
	}
	rffStored("ha after-reopen", db)
	if err := db.Close(); err != nil {
		fmt.Printf("RFF ha second close err=%v\n", err)
	}
	fmt.Printf("RFF HA END\n")
}

func TestZZRFRecoverProfiles(t *testing.T) {
	big := rfbBlob(3, 106954752)
	root := t.TempDir()
	cfg := rfbConfig(false)
	cfg.Database.WALSnapshotInterval = 2 * time.Second
	l, err := rfbOpen(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.run("CREATE (:Small {id:'s1'})", nil, false)
	_, err = l.run("CREATE (:Big {id:'big', blob:$b})", map[string]any{"b": big}, false)
	fmt.Printf("RFF recover write %d chars err=%v\n", len(big), err)
	big = ""
	_, _ = l.run("CREATE (:Small {id:'s2'})", nil, false)
	had := len(rfbSnapshots(root))
	start := time.Now()
	for len(rfbSnapshots(root)) <= had && time.Since(start) < 300*time.Second {
		time.Sleep(time.Second)
	}
	snaps := rfbSnapshots(root)
	snap, source := "", ""
	if len(snaps) > had {
		snap, source = snaps[len(snaps)-1], fmt.Sprintf("server's own snapshot after %.0f s", time.Since(start).Seconds())
	} else if w, ok := l.db.GetBaseStorageForManager().(*storage.WALEngine); ok {
		snap = filepath.Join(t.TempDir(), "probe-snapshot.json")
		err := storage.SaveStreamingSnapshot(context.Background(), w.GetInnerEngine(), snap, storage.SnapshotOptions{Sequence: w.GetWAL().Sequence()})
		source = fmt.Sprintf("probe-written snapshot (no server snapshot within 300 s) err=%v", err)
	}
	fmt.Printf("RFF recover snapshot source: %s\n", source)
	l.close()
	if snap == "" {
		return
	}
	if st, e := os.Stat(snap); e == nil {
		fmt.Printf("RFF recover snapshot bytes=%d\n", st.Size())
	}
	for _, p := range []struct {
		name string
		opts storage.BadgerOptions
	}{
		{"server-profile(HighPerformance)", storage.BadgerOptions{HighPerformance: true}},
		{"storage-default(NewBadgerEngine)", storage.BadgerOptions{}},
		{"low-memory", storage.BadgerOptions{LowMemory: true}},
	} {
		p.opts.DataDir = t.TempDir()
		fresh, err := storage.NewBadgerEngineWithOptions(p.opts)
		if err != nil {
			fmt.Printf("RFF recover %s open error %v\n", p.name, err)
			continue
		}
		res, err := rfRecover(fresh, filepath.Join(root, "wal"), snap)
		nodes, _ := storage.CollectStoredNodes(context.Background(), fresh)
		n := 0
		for _, x := range nodes {
			for _, lb := range x.Labels {
				if lb == "Big" {
					n++
				}
			}
		}
		fmt.Printf("RFF recover into %s store err=%s applied=%d failed=%d nodes=%d bigNodes=%d\n", p.name, rfbShort(err), res.Applied, res.Failed, len(nodes), n)
		fresh.Close()
	}
	fmt.Printf("RFF RECOVER END\n")
}
