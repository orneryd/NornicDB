package server

// Hunt ground H5: concurrency invariants. N Bolt clients (16) run mixed
// statements on shared data for a fixed time (20 minutes); afterwards
// invariants that hold whatever the interleaving are checked. Run with the
// race detector and on both parsers (tnrun --parser both; go test -race).
//
// Scenarios (HUNT_H5_SCENARIO):
//
//	C1 counter      explicit transactions increment one counter; counter = acknowledged increments
//	C2 unique       MERGE on a uniquely constrained key from all clients; one node per key, no error
//	C3 relmerge     MERGE of a relationship between two MERGEd endpoints; at most one relationship per pair
//	C4 transfer     transfers between 50 accounts in explicit transactions; the total never changes
//	C5 ownwrites    each client creates and deletes its own nodes; acknowledged creates present once, acknowledged deletes absent
//	C6 readback     each client writes then reads its write by an indexed property while another client drops and re-creates that index
//
// Oracle: the scenario's invariant; any error that is not a transient
// (Neo.TransientError.*) one is a lead (a valid request refused); a statement
// that does not answer within 60 s is a lead (hang). The race detector's
// report fails the test binary by itself.
//
// Run: HUNT_H5_SCENARIO=C1 [HUNT_H5_SECONDS=1200] [HUNT_H5_CLIENTS=16]
//      go test -race ./pkg/server -run '^TestHuntH5Concurrency$' -count=1 -timeout=40m -v

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

const huntH5Seed = 20261120

type huntH5Errors struct {
	mu     sync.Mutex
	counts map[string]int
	first  map[string]string
}

func (e *huntH5Errors) add(code, query, message string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.counts == nil {
		e.counts, e.first = map[string]int{}, map[string]string{}
	}
	key := code + " | " + query
	e.counts[key]++
	if _, ok := e.first[key]; !ok {
		e.first[key] = message
	}
}

func huntTransient(err error) bool {
	var status *neo4jdriver.Neo4jError
	if ok := asNeo4jError(err, &status); ok {
		return strings.HasPrefix(status.Code, "Neo.TransientError.")
	}
	return false
}

func asNeo4jError(err error, target **neo4jdriver.Neo4jError) bool {
	for err != nil {
		if typed, ok := err.(*neo4jdriver.Neo4jError); ok {
			*target = typed
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func huntCode(err error) string {
	var status *neo4jdriver.Neo4jError
	if asNeo4jError(err, &status) {
		return status.Code
	}
	if strings.Contains(err.Error(), "deadline") || strings.Contains(err.Error(), "timeout") {
		return "client:timeout"
	}
	return "client:" + huntFirstLine(err.Error())
}

// huntTx runs fn in one explicit transaction, retrying transient errors up to
// 20 times; it returns nil when committed (acknowledged).
func huntTx(h *huntServer, errs *huntH5Errors, label string, fn func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error) error {
	var last error
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		tx, err := session.BeginTransaction(ctx)
		if err == nil {
			err = fn(ctx, tx)
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.Background())
			}
		}
		_ = session.Close(context.Background())
		cancel()
		if err == nil {
			return nil
		}
		last = err
		if !huntTransient(err) {
			errs.add(huntCode(err), label, err.Error())
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	errs.add("transient retries exhausted", label, last.Error())
	return last
}

func huntRun(ctx context.Context, tx neo4jdriver.ExplicitTransaction, query string, params map[string]any) ([]*neo4jdriver.Record, error) {
	result, err := tx.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return result.Collect(ctx)
}

func TestHuntH5Concurrency(t *testing.T) {
	scenario := os.Getenv("HUNT_H5_SCENARIO")
	if scenario == "" {
		t.Skip("set HUNT_H5_SCENARIO to C1..C6")
	}
	seconds := huntEnvInt("HUNT_H5_SECONDS", 1200)
	clients := huntEnvInt("HUNT_H5_CLIENTS", 16)
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h5-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	h := huntOpen(t, filepath.Join(base, "store"), huntOptions{QueryCache: true, AsyncWrites: os.Getenv("HUNT_H5_ASYNC") == "1"})
	defer h.Close(t)
	setup := func(q string) {
		if outcome := h.Run(routeBoltAuto, q, nil, time.Minute); outcome.Failed() {
			t.Fatalf("setup %s: %s %s", q, outcome.Code, outcome.Message)
		}
	}
	switch scenario {
	case "C1":
		setup("CREATE (:H5Counter {k: 1, n: 0})")
	case "C2":
		setup("CREATE CONSTRAINT h5_unique IF NOT EXISTS FOR (n:H5U) REQUIRE n.id IS UNIQUE")
	case "C3":
		setup("CREATE CONSTRAINT h5_p IF NOT EXISTS FOR (n:H5P) REQUIRE n.id IS UNIQUE")
	case "C4":
		setup("UNWIND range(0, 49) AS i CREATE (:H5Account {id: i, balance: 1000})")
	case "C6":
		setup("CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)")
	}
	errs := &huntH5Errors{}
	var acked, attempted int64
	var ownMu sync.Mutex
	ownCreated, ownDeleted := map[string]bool{}, map[string]bool{}
	var readbackMisses int64
	var readbackFirst atomic.Value
	stop := time.Now().Add(time.Duration(seconds) * time.Second)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(huntH5Seed + int64(client)))
			for i := 0; time.Now().Before(stop); i++ {
				atomic.AddInt64(&attempted, 1)
				var err error
				switch scenario {
				case "C1":
					err = huntTx(h, errs, "increment", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "MATCH (c:H5Counter {k: 1}) SET c.n = c.n + 1", nil)
						return err
					})
				case "C2":
					err = huntTx(h, errs, "merge unique", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "MERGE (n:H5U {id: $id}) ON CREATE SET n.by = $c ON MATCH SET n.hits = coalesce(n.hits, 0) + 1", map[string]any{"id": rnd.Intn(200), "c": client})
						return err
					})
				case "C3":
					a, b := rnd.Intn(40), rnd.Intn(40)
					err = huntTx(h, errs, "merge relationship", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "MERGE (a:H5P {id: $a}) MERGE (b:H5P {id: $b}) MERGE (a)-[r:H5R]->(b) ON CREATE SET r.by = $c", map[string]any{"a": a, "b": b, "c": client})
						return err
					})
				case "C4":
					from, to, amount := rnd.Intn(50), rnd.Intn(50), rnd.Intn(100)+1
					if from == to {
						continue
					}
					err = huntTx(h, errs, "transfer", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						if _, err := huntRun(ctx, tx, "MATCH (a:H5Account {id: $from}) SET a.balance = a.balance - $amount", map[string]any{"from": from, "amount": amount}); err != nil {
							return err
						}
						_, err := huntRun(ctx, tx, "MATCH (b:H5Account {id: $to}) SET b.balance = b.balance + $amount", map[string]any{"to": to, "amount": amount})
						return err
					})
				case "C5":
					id := fmt.Sprintf("c%d-%d", client, i)
					err = huntTx(h, errs, "own create", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "CREATE (:H5Own {id: $id, client: $c})", map[string]any{"id": id, "c": client})
						return err
					})
					if err == nil {
						ownMu.Lock()
						ownCreated[id] = true
						ownMu.Unlock()
					}
					if i%3 == 2 {
						old := fmt.Sprintf("c%d-%d", client, i-2)
						if derr := huntTx(h, errs, "own delete", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
							_, err := huntRun(ctx, tx, "MATCH (n:H5Own {id: $id}) DETACH DELETE n", map[string]any{"id": old})
							return err
						}); derr == nil {
							ownMu.Lock()
							ownDeleted[old] = true
							ownMu.Unlock()
						}
					}
				case "C6":
					if client == 0 {
						for _, q := range []string{"DROP INDEX h5_rb IF EXISTS", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)"} {
							if outcome := h.Run(routeBoltAuto, q, nil, 60*time.Second); outcome.Failed() {
								errs.add(outcome.Code, q, outcome.Message)
							}
						}
						time.Sleep(50 * time.Millisecond)
						continue
					}
					key := fmt.Sprintf("k%d-%d", client, i)
					err = huntTx(h, errs, "readback write", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "CREATE (:H5RB {key: $k})", map[string]any{"k": key})
						return err
					})
					if err == nil {
						outcome := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", map[string]any{"k": key}, 60*time.Second)
						if outcome.Failed() {
							errs.add(outcome.Code, "readback read", outcome.Message)
						} else if len(outcome.Rows) != 1 || fmt.Sprint(outcome.Rows[0][0]) != fmt.Sprint(map[string]string{"Integer": "1"}) {
							atomic.AddInt64(&readbackMisses, 1)
							readbackFirst.CompareAndSwap(nil, key+" -> "+fmt.Sprint(outcome.Rows))
						}
					}
				}
				if err == nil {
					atomic.AddInt64(&acked, 1)
				}
			}
		}(c)
	}
	wg.Wait()

	lead := func(verdict string, detail map[string]any) {
		entry := map[string]any{"ground": "H5", "scenario": scenario, "parser": nornicConfig.GetParserType(), "clients": clients, "seconds": seconds, "seed": huntH5Seed, "verdict": verdict}
		for k, v := range detail {
			entry[k] = v
		}
		huntLead(t, entry)
	}
	leads := 0
	check := func(q string) differential.Outcome { return h.Run(routeBoltAuto, q, nil, 5*time.Minute) }
	integer := func(outcome differential.Outcome) int64 {
		if outcome.Failed() || len(outcome.Rows) != 1 {
			return -1
		}
		if typed, ok := outcome.Rows[0][0].(map[string]string); ok {
			v, _ := strconv.ParseInt(typed["Integer"], 10, 64)
			return v
		}
		return -1
	}
	switch scenario {
	case "C1":
		if got := integer(check("MATCH (c:H5Counter {k: 1}) RETURN c.n")); got != acked {
			lead("counter differs from acknowledged increments", map[string]any{"counter": got, "acknowledged": acked})
			leads++
		}
	case "C2":
		dup := check("MATCH (n:H5U) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c")
		if len(dup.Rows) > 0 || dup.Failed() {
			lead("unique key duplicated", map[string]any{"duplicates": huntTrim(dup)})
			leads++
		}
	case "C3":
		dup := check("MATCH (a:H5P)-[r:H5R]->(b:H5P) WITH a.id AS a, b.id AS b, count(r) AS c WHERE c > 1 RETURN a, b, c")
		if len(dup.Rows) > 0 || dup.Failed() {
			lead("relationship MERGE created duplicates", map[string]any{"duplicates": huntTrim(dup)})
			leads++
		}
		nodes := check("MATCH (n:H5P) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c")
		if len(nodes.Rows) > 0 {
			lead("endpoint MERGE created duplicates", map[string]any{"duplicates": huntTrim(nodes)})
			leads++
		}
	case "C4":
		if got := integer(check("MATCH (a:H5Account) RETURN sum(a.balance)")); got != 50000 {
			lead("total balance changed", map[string]any{"total": got, "want": 50000})
			leads++
		}
	case "C5":
		present := huntH5Strings(check("MATCH (n:H5Own) RETURN n.id"))
		var missing, resurrected, duplicated []string
		for id := range ownCreated {
			if ownDeleted[id] {
				if present[id] > 0 {
					resurrected = append(resurrected, id)
				}
				continue
			}
			if present[id] == 0 {
				missing = append(missing, id)
			}
		}
		for id, n := range present {
			if n > 1 {
				duplicated = append(duplicated, id)
			}
		}
		if len(missing)+len(resurrected)+len(duplicated) > 0 {
			lead("own writes not as acknowledged", map[string]any{"missing": huntFirstN(missing, 20), "deletedButPresent": huntFirstN(resurrected, 20), "duplicated": huntFirstN(duplicated, 20),
				"counts": []int{len(missing), len(resurrected), len(duplicated)}})
			leads++
		}
	case "C6":
		if readbackMisses > 0 {
			first, _ := readbackFirst.Load().(string)
			lead("acknowledged write not visible to its own client's next read", map[string]any{"misses": readbackMisses, "first": first})
			leads++
		}
	}
	errs.mu.Lock()
	for key, n := range errs.counts {
		lead("non-transient error on a valid statement", map[string]any{"error": key, "count": n, "firstMessage": errs.first[key]})
		leads++
	}
	errs.mu.Unlock()
	huntCoverage(t, map[string]any{"ground": "H5", "scenario": scenario, "parser": nornicConfig.GetParserType(), "clients": clients, "seconds": seconds,
		"attempted": attempted, "acknowledged": acked, "leads": leads})
}

func huntH5Strings(outcome differential.Outcome) map[string]int {
	out := map[string]int{}
	for _, row := range outcome.Rows {
		if s, ok := row[0].(string); ok {
			out[s]++
		}
	}
	return out
}

func huntFirstN(values []string, n int) []string {
	if len(values) > n {
		return values[:n]
	}
	return values
}
