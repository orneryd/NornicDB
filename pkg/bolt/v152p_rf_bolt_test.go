package bolt

// Research only (refusal observations 1 and 2), through Bolt.
// 1: a CREATE whose journal record exceeds one 16 MiB frame, standalone
//    defaults vs HA primary (NORNICDB_CLUSTER_MODE=ha_standby, role primary,
//    peer unreachable); implicit and explicit; what the client is told, what is
//    visible, what is visible after a clean reopen.
// 2: a 142,606,336-character value written with defaults (server profile), then
//    the server's own backup (db.Backup, as POST /admin/backup) restored into a
//    fresh default server and a fresh low-memory server (db.Restore, as POST
//    /admin/restore); the original store reopened in low-memory mode; and the
//    snapshot+journal recovery into a store of each profile.
import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type rfbLab struct {
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func rfbConfig(low bool) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	if low {
		c.Storage.Mode = "low"
	}
	return c
}

func rfbOpen(root string, cfg *nornicdb.Config) (*rfbLab, error) {
	db, err := nornicdb.Open(root, cfg)
	if err != nil {
		return nil, err
	}
	m, err := rfNewManager(db.GetBaseStorageForManager())
	if err != nil {
		db.Close()
		return nil, err
	}
	s := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	d, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	if err != nil {
		return nil, err
	}
	return &rfbLab{db: db, d: d, s: s, done: done}, nil
}

func (l *rfbLab) close() {
	if l == nil || l.db == nil {
		return
	}
	_ = l.d.Close(context.Background())
	_ = l.s.Close()
	<-l.done
	if err := l.db.Close(); err != nil {
		fmt.Printf("RFB close error: %v\n", err)
	}
	l.db = nil
}

func (l *rfbLab) run(q string, p map[string]any, explicit bool) ([][]any, error) {
	ctx := context.Background()
	s := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	var res neo4j.ResultWithContext
	var err error
	var tx neo4j.ExplicitTransaction
	if explicit {
		if tx, err = s.BeginTransaction(ctx); err != nil {
			return nil, fmt.Errorf("BEGIN: %w", err)
		}
		defer tx.Close(ctx)
		res, err = tx.Run(ctx, q, p)
	} else {
		res, err = s.Run(ctx, q, p)
	}
	if err != nil {
		return nil, fmt.Errorf("RUN: %w", err)
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("PULL: %w", err)
	}
	rows := [][]any{}
	for _, r := range recs {
		rows = append(rows, r.Values)
	}
	if explicit {
		if err = tx.Commit(ctx); err != nil {
			return rows, fmt.Errorf("COMMIT: %w", err)
		}
	}
	return rows, nil
}

func rfbBlob(seed int64, raw int) string {
	b := make([]byte, raw)
	rand.New(rand.NewSource(seed)).Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func rfbShort(v any) string {
	s := fmt.Sprint(v)
	if len(s) > 600 {
		s = s[:600] + "…"
	}
	return s
}

func rfbVisible(l *rfbLab, tag, label string) {
	rows, err := l.run("MATCH (n:"+label+") RETURN n.id, size(n.blob) ORDER BY n.id", nil, false)
	fmt.Printf("RFB %s visible %s rows=%v err=%v\n", tag, label, rows, err)
}

func TestZZRFBoltFrame(t *testing.T) {
	over := rfbBlob(1, 16<<20) // 22,369,624 characters
	under := rfbBlob(2, 6<<20) // 8,388,608 characters
	for _, c := range []struct {
		name string
		env  map[string]string
	}{
		{"standalone-defaults", nil},
		{"ha-primary", map[string]string{"NORNICDB_CLUSTER_MODE": "ha_standby", "NORNICDB_CLUSTER_HA_ROLE": "primary", "NORNICDB_CLUSTER_HA_PEER_ADDR": "127.0.0.1:1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			root := t.TempDir()
			cfg := rfbConfig(false)
			l, err := rfbOpen(root, cfg)
			if err != nil {
				fmt.Printf("RFB %s open error: %v\n", c.name, err)
				return
			}
			fmt.Printf("RFB %s env mode=%q role=%q base=%T\n", c.name, os.Getenv("NORNICDB_CLUSTER_MODE"), os.Getenv("NORNICDB_CLUSTER_HA_ROLE"), l.db.GetBaseStorageForManager())
			_, err = l.run("CREATE (:Small {id:'s1'})", nil, false)
			fmt.Printf("RFB %s small err=%v\n", c.name, err)
			_, err = l.run("CREATE (:Under {id:'u1', blob:$b})", map[string]any{"b": under}, false)
			fmt.Printf("RFB %s under-frame %d chars implicit err=%v\n", c.name, len(under), err)
			_, err = l.run("CREATE (:Big {id:'b-implicit', blob:$b})", map[string]any{"b": over}, false)
			fmt.Printf("RFB %s over-frame %d chars implicit err=%v\n", c.name, len(over), err)
			_, err = l.run("CREATE (:Big {id:'b-explicit', blob:$b})", map[string]any{"b": over}, true)
			fmt.Printf("RFB %s over-frame %d chars explicit err=%v\n", c.name, len(over), err)
			_, err = l.run("CREATE (:Small {id:'s2'})", nil, false)
			fmt.Printf("RFB %s small-after err=%v\n", c.name, err)
			rfbVisible(l, c.name+" before-reopen", "Big")
			rfbVisible(l, c.name+" before-reopen", "Under")
			rfbVisible(l, c.name+" before-reopen", "Small")
			l.close()
			l, err = rfbOpen(root, cfg)
			if err != nil {
				fmt.Printf("RFB %s reopen error: %v\n", c.name, err)
				return
			}
			rfbVisible(l, c.name+" after-reopen", "Big")
			rfbVisible(l, c.name+" after-reopen", "Under")
			rfbVisible(l, c.name+" after-reopen", "Small")
			l.close()
		})
	}
	fmt.Printf("RFB FRAME END\n")
}

func rfbSnapshots(root string) []string {
	m, _ := filepath.Glob(filepath.Join(root, "snapshots", "snapshot-*.json"))
	sort.Strings(m)
	return m
}

func TestZZRFBoltProfile(t *testing.T) {
	big := rfbBlob(3, 106954752) // 142,606,336 characters
	root := t.TempDir()
	cfg := rfbConfig(false)
	cfg.Database.WALSnapshotInterval = 2 * time.Second
	l, err := rfbOpen(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.run("CREATE (:Small {id:'s1'})", nil, false)
	fmt.Printf("RFB profile small err=%v\n", err)
	_, err = l.run("CREATE (:Big {id:'big', blob:$b})", map[string]any{"b": big}, false)
	fmt.Printf("RFB profile write %d chars with defaults (storage mode %q) err=%v\n", len(big), cfg.Storage.Mode, err)
	big = ""
	rfbVisible(l, "profile original", "Big")
	had := len(rfbSnapshots(root))
	dl := time.Now().Add(60 * time.Second)
	for len(rfbSnapshots(root)) <= had && time.Now().Before(dl) {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second)
	snaps := rfbSnapshots(root)
	fmt.Printf("RFB profile snapshots=%d\n", len(snaps))
	bdir := t.TempDir()
	bpath := filepath.Join(bdir, "backup.json")
	err = l.db.Backup(context.Background(), bpath)
	var bsize int64
	if st, e := os.Stat(bpath); e == nil {
		bsize = st.Size()
	}
	head := make([]byte, 16)
	if f, e := os.Open(bpath); e == nil {
		f.Read(head)
		f.Close()
	}
	fmt.Printf("RFB profile backup (db.Backup, as POST /admin/backup) err=%v bytes=%d starts=%q\n", err, bsize, strings.TrimSpace(string(head)))
	l.close()
	for _, low := range []bool{false, true} {
		name := "default"
		if low {
			name = "low-memory"
		}
		r, err := rfbOpen(t.TempDir(), rfbConfig(low))
		if err != nil {
			fmt.Printf("RFB profile restore-target %s open error: %v\n", name, err)
			continue
		}
		err = r.db.Restore(context.Background(), bpath)
		fmt.Printf("RFB profile restore (db.Restore, as POST /admin/restore) into fresh %s server err=%s\n", name, rfbShort(err))
		rfbVisible(r, "profile restored-into-"+name, "Big")
		r.close()
	}
	lowCfg := rfbConfig(true)
	lowCfg.Database.WALAutoCompactionEnabled = false
	r, err := rfbOpen(root, lowCfg)
	if err != nil {
		fmt.Printf("RFB profile reopen original in low-memory mode error: %s\n", rfbShort(err))
	} else {
		fmt.Printf("RFB profile reopen original in low-memory mode ok\n")
		rfbVisible(r, "profile reopened-low", "Big")
		_, err = r.run("MATCH (n:Big {id:'big'}) SET n.touched = 1", nil, false)
		fmt.Printf("RFB profile low-memory reopen: update a property of the big node err=%s\n", rfbShort(err))
		r.close()
	}
	if len(snaps) == 0 {
		fmt.Printf("RFB profile no snapshot: recovery not run\n")
		return
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
			fmt.Printf("RFB profile recover %s open error %v\n", p.name, err)
			continue
		}
		res, err := rfRecover(fresh, filepath.Join(root, "wal"), snaps[len(snaps)-1])
		nodes, _ := storage.CollectStoredNodes(context.Background(), fresh)
		n := 0
		for _, x := range nodes {
			for _, lb := range x.Labels {
				if lb == "Big" {
					n++
				}
			}
		}
		fmt.Printf("RFB profile recover snapshot+journal into %s store err=%s applied=%d failed=%d bigNodes=%d\n", p.name, rfbShort(err), res.Applied, res.Failed, n)
		fresh.Close()
	}
	fmt.Printf("RFB PROFILE END\n")
}
