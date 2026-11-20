package main

// Lost-acknowledgement probe over Bolt (untracked; never committed). Sixteen
// clients write through a real Bolt listener (half auto-commit RUN, half an
// explicit transaction BEGIN, RUN, COMMIT) while the server is stopped
// cleanly the way the binary stops it (Bolt stop, then database close). Every
// write is recorded by name with the answer the client received; after the
// stop the journal is read and the database is reopened, and every name is
// looked up in the store.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

type boltStopRecord struct {
	name     string
	explicit bool
	outcome  string // SUCCESS, FAILURE:<code>, NOANSWER:<type>, OTHER:<type>
	step     string // auto: run/consume; explicit: begin/run/consume/commit
	detail   string
	sent     time.Time
	answered time.Time
}

func boltStopConfig() *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	return c
}

func boltStopClassify(err error) (string, string) {
	if err == nil {
		return "SUCCESS", ""
	}
	var n4j *neo4j.Neo4jError
	if errors.As(err, &n4j) {
		return "FAILURE:" + n4j.Code, boltStopCut(n4j.Msg)
	}
	var conn *neo4j.ConnectivityError
	if errors.As(err, &conn) {
		return "NOANSWER:connectivity", boltStopCut(err.Error())
	}
	return fmt.Sprintf("OTHER:%T", err), boltStopCut(err.Error())
}

func boltStopCut(s string) string {
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func boltStopRounds() int {
	if v := os.Getenv("BOLTSTOP_ROUNDS"); v != "" {
		var n int
		fmt.Sscan(v, &n)
		if n > 0 {
			return n
		}
	}
	return 20
}

func TestBoltStopInFlightWrites(t *testing.T) {
	rounds := boltStopRounds()
	totals := map[string]int{}
	for round := 0; round < rounds; round++ {
		dir := filepath.Join(t.TempDir(), "data")
		db, err := nornicdb.Open(dir, boltStopConfig())
		require.NoError(t, err)
		server := bolt.New(&bolt.Config{Port: 0, MaxConnections: 64, ReadBufferSize: 8192, WriteBufferSize: 8192}, NewDBQueryExecutor(db))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		served := make(chan error, 1)
		go func() { served <- server.Serve(listener) }()
		driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://%s", listener.Addr()), neo4j.NoAuth(), func(c *neo4j.Config) {
			c.MaxConnectionPoolSize = 32
			c.ConnectionAcquisitionTimeout = 5 * time.Second
			c.SocketConnectTimeout = 2 * time.Second
		})
		require.NoError(t, err)

		var mu sync.Mutex
		var records []boltStopRecord
		var stop atomic.Bool
		var stopAt atomic.Int64
		var wg sync.WaitGroup
		for writer := 0; writer < 16; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				explicit := writer%2 == 1
				ctx := context.Background()
				session := driver.NewSession(ctx, neo4j.SessionConfig{})
				defer func() { _ = session.Close(ctx) }()
				for index := 0; ; index++ {
					if at := stopAt.Load(); at != 0 && time.Since(time.Unix(0, at)) > 5*time.Second {
						return
					}
					name := fmt.Sprintf("r%02d-w%02d-%05d", round, writer, index)
					params := map[string]any{"name": name}
					rec := boltStopRecord{name: name, explicit: explicit, sent: time.Now()}
					var err error
					if !explicit {
						rec.step = "run"
						var result neo4j.ResultWithContext
						result, err = session.Run(ctx, "CREATE (:BL {name: $name})", params)
						if err == nil {
							rec.step = "consume"
							_, err = result.Consume(ctx)
						}
					} else {
						rec.step = "begin"
						var tx neo4j.ExplicitTransaction
						tx, err = session.BeginTransaction(ctx)
						if err == nil {
							rec.step = "run"
							var result neo4j.ResultWithContext
							result, err = tx.Run(ctx, "CREATE (:BL {name: $name})", params)
							if err == nil {
								rec.step = "consume"
								_, err = result.Consume(ctx)
							}
							if err != nil {
								_ = tx.Close(ctx)
							} else {
								rec.step = "commit"
								err = tx.Commit(ctx)
							}
						}
					}
					rec.answered = time.Now()
					rec.outcome, rec.detail = boltStopClassify(err)
					mu.Lock()
					records = append(records, rec)
					mu.Unlock()
					if err != nil {
						if stop.Load() {
							return
						}
						_ = session.Close(ctx)
						session = driver.NewSession(ctx, neo4j.SessionConfig{})
						time.Sleep(time.Millisecond)
					}
				}
			}(writer)
		}
		time.Sleep(1500 * time.Millisecond)
		stopStart := time.Now()
		stopAt.Store(stopStart.UnixNano())
		stopErr := boltStopServer(server)
		boltStopped := time.Now()
		closeErr := db.Close()
		stopEnd := time.Now()
		stop.Store(true)
		// Writers whose connection still works keep writing until they fail;
		// the database is closed, so their next write fails.
		wg.Wait()
		_ = driver.Close(context.Background())
		select {
		case <-served:
		case <-time.After(10 * time.Second):
			t.Logf("BOLTSTOP round=%d Serve did not return within 10s", round)
		}

		inJournal := boltStopJournalIndex(t, filepath.Join(dir, "wal"))

		db2, err := nornicdb.Open(dir, boltStopConfig())
		require.NoError(t, err)
		res, err := db2.ExecuteCypher(context.Background(), "MATCH (x:BL) RETURN x.name", nil)
		require.NoError(t, err)
		stored := map[string]int{}
		for _, row := range res.Rows {
			stored[fmt.Sprint(row[0])]++
		}
		require.NoError(t, db2.Close())

		counts := map[string]int{}
		var details []string
		for _, rec := range records {
			mode := map[bool]string{false: "auto", true: "explicit"}[rec.explicit]
			isStored := stored[rec.name] > 0
			key := fmt.Sprintf("%s %s stored=%v", mode, rec.outcome, isStored)
			counts[key]++
			totals[key]++
			if (rec.outcome == "SUCCESS" && !isStored) || (rec.outcome != "SUCCESS" && isStored) {
				details = append(details, fmt.Sprintf("MISMATCH %s %s outcome=%s step=%s stored=%v detail=%q sent=%+dms answered=%+dms (bolt stop %dms, db close %dms) journal=%s",
					rec.name, mode, rec.outcome, rec.step, isStored, rec.detail,
					rec.sent.Sub(stopStart).Milliseconds(), rec.answered.Sub(stopStart).Milliseconds(),
					boltStopped.Sub(stopStart).Milliseconds(), stopEnd.Sub(stopStart).Milliseconds(), inJournal(rec.name)))
				totals["mismatch"]++
			}
			if rec.outcome != "SUCCESS" && rec.answered.After(stopStart.Add(-50*time.Millisecond)) {
				counts["failures-near-stop "+mode+" "+rec.outcome+" step="+rec.step]++
			}
		}
		for name, c := range stored {
			if c > 1 {
				details = append(details, fmt.Sprintf("DUPLICATE %s x%d", name, c))
			}
		}
		keys := make([]string, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("[%s]=%d", k, counts[k]))
		}
		t.Logf("BOLTSTOP round=%d requests=%d stored=%d stopErr=%v closeErr=%v boltStop=%dms dbClose=%dms %s",
			round, len(records), len(stored), stopErr, closeErr, boltStopped.Sub(stopStart).Milliseconds(), stopEnd.Sub(boltStopped).Milliseconds(), strings.Join(parts, " "))
		sort.Strings(details)
		for _, d := range details {
			t.Logf("BOLTSTOP round=%d %s", round, d)
		}
	}
	keys := make([]string, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("BOLTSTOP TOTAL [%s]=%d", k, totals[k])
	}
}

func boltStopJournalIndex(t *testing.T, walDir string) func(string) string {
	entries, err := boltStopReadJournal(walDir)
	return func(name string) string {
		if err != nil {
			return "unreadable: " + err.Error()
		}
		var ops []string
		for _, e := range entries {
			if strings.Contains(string(e.Data), name) {
				ops = append(ops, fmt.Sprintf("%s#%d", e.Operation, e.Sequence))
			}
		}
		if len(ops) == 0 {
			return "absent"
		}
		return strings.Join(ops, ",")
	}
}
