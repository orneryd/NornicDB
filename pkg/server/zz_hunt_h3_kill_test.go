package server

// Hunt ground H3, part 1: kill and reopen.
//
// A child process (this test binary, TestHuntH3Child) runs one write workload
// against a whole server over a directory through Bolt, and logs every
// transaction it begins ("B id") and every one the server acknowledged
// ("A id unixnano"), each line fsynced before the next step. The parent kills
// the child with SIGKILL at a seeded random moment, reopens the directory in
// its own process, checks the model below, closes cleanly, and starts the next
// cycle on the same directory.
//
// Oracle (a model of acknowledged writes):
//   - the store opens;
//   - every acknowledged transaction is present exactly once (W1-W3, durable
//     writes, the default); for W4 (asynchronous writes, documented as losing
//     at most one flush interval, clamped to 1 s, on a crash) every
//     transaction acknowledged more than 1 s before the kill is present;
//   - nothing that was never begun is present; a transaction is all or nothing;
//   - the workload's invariant holds (W2 counter = transactions present;
//     W3 deleted batches absent; W4 merge key unique).
//
// Workloads (HUNT_H3_WORKLOAD):
//
//	W1 auto-commit CREATE of one node per transaction
//	W2 explicit transactions: CREATE node, link to the previous one, increment a counter
//	W3 UNWIND batches of 100 nodes with 1 KiB properties; every 5th transaction deletes the batch 3 back
//	W4 asynchronous writes; MERGE on a uniquely constrained key plus an event node per transaction
//
// Run: HUNT_H3_WORKLOAD=W1 [HUNT_H3_CYCLES=200] [HUNT_H3_SEED=20261120]
//      go test ./pkg/server -run '^TestHuntH3KillReopen$' -count=1 -timeout=6h -v

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

const huntH3Seed = 20261120

func huntH3Options(workload string) huntOptions {
	return huntOptions{QueryCache: true, AsyncWrites: workload == "W4"}
}

// TestHuntH3Child is the child process; it does nothing unless started by
// TestHuntH3KillReopen.
func TestHuntH3Child(t *testing.T) {
	dir := os.Getenv("HUNT_H3_CHILD_DIR")
	if dir == "" {
		t.Skip("child of TestHuntH3KillReopen only")
	}
	workload := os.Getenv("HUNT_H3_WORKLOAD")
	start, _ := strconv.Atoi(os.Getenv("HUNT_H3_CHILD_START"))
	log, err := os.OpenFile(os.Getenv("HUNT_H3_CHILD_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	note := func(line string) {
		_, _ = log.WriteString(line + "\n")
		_ = log.Sync()
	}
	h := huntOpen(t, filepath.Join(dir, "store"), huntH3Options(workload))
	note("OPEN")
	ctx := context.Background()
	if workload == "W4" {
		_ = h.Run(routeBoltAuto, "CREATE CONSTRAINT h3w4_key IF NOT EXISTS FOR (n:H3W4) REQUIRE n.k IS UNIQUE", nil, time.Minute)
	}
	if workload == "W2" {
		_ = h.Run(routeBoltAuto, "MERGE (:H3W2Counter {k: 1})", nil, time.Minute)
	}
	pad := strings.Repeat("p", 1024)
	for id := start; ; id++ {
		note("B " + strconv.Itoa(id))
		var err error
		session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		switch workload {
		case "W1":
			var result neo4jdriver.ResultWithContext
			if result, err = session.Run(ctx, "CREATE (:H3W1 {id: $id})", map[string]any{"id": id}); err == nil {
				_, err = result.Consume(ctx)
			}
		case "W2":
			_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				for _, q := range []string{
					"CREATE (:H3W2 {id: $id})",
					"MATCH (p:H3W2 {id: $id - 1}), (n:H3W2 {id: $id}) CREATE (p)-[:NEXT {id: $id}]->(n)",
					"MATCH (c:H3W2Counter {k: 1}) SET c.n = coalesce(c.n, 0) + 1",
				} {
					result, err := tx.Run(ctx, q, map[string]any{"id": id})
					if err != nil {
						return nil, err
					}
					if _, err := result.Consume(ctx); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
		case "W3":
			_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "UNWIND range(0, 99) AS i CREATE (:H3W3 {id: $id * 100 + i, tx: $id, pad: $pad})", map[string]any{"id": id, "pad": pad})
				if err != nil {
					return nil, err
				}
				if _, err := result.Consume(ctx); err != nil {
					return nil, err
				}
				if id%5 == 0 && id >= 3 {
					result, err = tx.Run(ctx, "MATCH (n:H3W3 {tx: $old}) DETACH DELETE n", map[string]any{"old": id - 3})
					if err != nil {
						return nil, err
					}
					_, err = result.Consume(ctx)
				}
				return nil, err
			})
		case "W4":
			_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "MERGE (n:H3W4 {k: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.c = n.c + 1 CREATE (:H3W4Event {id: $id, k: $k})", map[string]any{"id": id, "k": id % 500})
				if err != nil {
					return nil, err
				}
				_, err = result.Consume(ctx)
				return nil, err
			})
		default:
			t.Fatalf("unknown workload %q", workload)
		}
		_ = session.Close(ctx)
		if err != nil {
			note("E " + strconv.Itoa(id) + " " + strings.ReplaceAll(err.Error(), "\n", " "))
			continue
		}
		note("A " + strconv.Itoa(id) + " " + strconv.FormatInt(time.Now().UnixNano(), 10))
	}
}

type huntH3Log struct {
	begun   map[int]bool
	acked   map[int]int64
	failed  map[int]bool
	maxSeen int
	// errors: the first few distinct error texts the child reported
	errors  []string
}

func readHuntH3Log(t *testing.T, path string) huntH3Log {
	l := huntH3Log{begun: map[int]bool{}, acked: map[int]int64{}, failed: map[int]bool{}, maxSeen: -1}
	file, err := os.Open(path)
	if err != nil {
		return l
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		id, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		switch fields[0] {
		case "B":
			l.begun[id] = true
			if id > l.maxSeen {
				l.maxSeen = id
			}
		case "A":
			at, _ := strconv.ParseInt(fields[2], 10, 64)
			l.acked[id] = at
		case "E":
			l.failed[id] = true
			if text := strings.Join(fields[2:], " "); len(l.errors) < 5 && !huntContains(l.errors, text) {
				l.errors = append(l.errors, text)
			}
		}
	}
	return l
}

func huntInts(outcome differential.Outcome) map[int]int {
	counts := map[int]int{}
	for _, row := range outcome.Rows {
		if typed, ok := row[0].(map[string]string); ok {
			if v, err := strconv.Atoi(typed["Integer"]); err == nil {
				counts[v]++
			}
		}
	}
	return counts
}

func TestHuntH3KillReopen(t *testing.T) {
	workload := os.Getenv("HUNT_H3_WORKLOAD")
	if workload == "" {
		t.Skip("set HUNT_H3_WORKLOAD to W1, W2, W3 or W4")
	}
	cycles := huntEnvInt("HUNT_H3_CYCLES", 200)
	seed := int64(huntEnvInt("HUNT_H3_SEED", huntH3Seed))
	random := rand.New(rand.NewSource(seed + int64(workload[1])))
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h3-"+workload+"-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	logPath := filepath.Join(base, "acks.log")
	started := time.Now()
	leads, completed := 0, 0
	lead := func(cycle int, verdict string, detail map[string]any) {
		leads++
		entry := map[string]any{"ground": "H3", "part": "kill", "workload": workload, "cycle": cycle, "seed": seed, "verdict": verdict}
		for k, v := range detail {
			entry[k] = v
		}
		huntLead(t, entry)
	}
	next := 0
	lastBegun, lastAcked, lastFailed := 0, 0, 0
	var lastErrors []string
	for cycle := 1; cycle <= cycles; cycle++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHuntH3Child$", "-test.count=1", "-test.timeout=30m")
		cmd.Env = append(os.Environ(), "HUNT_H3_CHILD_DIR="+base, "HUNT_H3_WORKLOAD="+workload,
			"HUNT_H3_CHILD_START="+strconv.Itoa(next), "HUNT_H3_CHILD_LOG="+logPath)
		childOut, _ := os.Create(filepath.Join(base, "child.out"))
		cmd.Stdout, cmd.Stderr = childOut, childOut
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child: %v", err)
		}
		// wait for the child to open the store, then a seeded 0-2000 ms of writes
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			if data, _ := os.ReadFile(logPath); strings.Count(string(data), "OPEN") >= cycle {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		killDelay := time.Duration(random.Intn(2000)) * time.Millisecond
		time.Sleep(killDelay)
		killedAt := time.Now()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = childOut.Close()

		state := readHuntH3Log(t, logPath)
		next = state.maxSeen + 1
		lastBegun, lastAcked, lastFailed, lastErrors = len(state.begun), len(state.acked), len(state.failed), state.errors
		h, err := huntOpenErr(t, filepath.Join(base, "store"), huntH3Options(workload))
		if err != nil {
			out, _ := os.ReadFile(filepath.Join(base, "child.out"))
			lead(cycle, "store does not open after kill", map[string]any{"error": err.Error(), "childTail": huntTail(string(out), 2000)})
			break
		}
		// A check statement that fails is reported as such (and not as every
		// acknowledged write missing); the cycle's other checks still run.
		checkFailed := false
		check := func(query string) differential.Outcome {
			outcome := h.Run(routeBoltAuto, query, nil, 5*time.Minute)
			if outcome.Code != "" {
				checkFailed = true
				lead(cycle, "check statement failed after reopen", map[string]any{"query": query, "code": outcome.Code, "message": outcome.Message})
			}
			return outcome
		}
		var present map[int]int
		switch workload {
		case "W1":
			present = huntInts(check("MATCH (n:H3W1) RETURN n.id"))
		case "W2":
			present = huntInts(check("MATCH (n:H3W2) RETURN n.id"))
			// The counter node is created (MERGE) when the child starts and has no
			// value until the first W2 transaction commits; a kill before either is
			// counter = 0, not a disagreement.
			counter := check("OPTIONAL MATCH (c:H3W2Counter) RETURN count(c) AS nodes, coalesce(max(c.n), 0) AS n")
			total := 0
			for _, c := range present {
				total += c
			}
			counterOK := len(counter.Rows) == 1 && len(counter.Rows[0]) == 2 &&
				fmt.Sprint(counter.Rows[0][1]) == fmt.Sprint(map[string]string{"Integer": strconv.Itoa(total)}) &&
				(fmt.Sprint(counter.Rows[0][0]) == fmt.Sprint(map[string]string{"Integer": "1"}) ||
					(total == 0 && fmt.Sprint(counter.Rows[0][0]) == fmt.Sprint(map[string]string{"Integer": "0"})))
			if counter.Code == "" && !counterOK {
				lead(cycle, "W2 counter differs from transactions present", map[string]any{"counter": huntTrim(counter), "present": total})
			}
			orphan := check("MATCH ()-[r:NEXT]->(n:H3W2) WHERE r.id <> n.id RETURN count(r)")
			if len(orphan.Rows) == 1 && fmt.Sprint(orphan.Rows[0][0]) != fmt.Sprint(map[string]string{"Integer": "0"}) {
				lead(cycle, "W2 relationship id does not match its end node", map[string]any{"count": huntTrim(orphan)})
			}
		case "W3":
			perTx := huntInts(check("MATCH (n:H3W3) RETURN n.tx"))
			present = map[int]int{}
			for tx, n := range perTx {
				if n != 100 {
					lead(cycle, "W3 batch partially present", map[string]any{"tx": tx, "nodes": n})
				}
				present[tx] = 1
			}
			for id := range state.begun {
				if id%5 == 0 && id >= 3 {
					if _, acked := state.acked[id]; acked {
						if _, ok := perTx[id-3]; ok {
							lead(cycle, "W3 acknowledged delete not applied", map[string]any{"deleteTx": id, "batch": id - 3})
						}
					}
					// a begun delete may have removed its batch: that batch is not required
					delete(state.acked, id-3)
				}
			}
		case "W4":
			present = huntInts(check("MATCH (e:H3W4Event) RETURN e.id"))
			dup := check("MATCH (n:H3W4) WITH n.k AS k, count(*) AS c WHERE c > 1 RETURN k, c")
			if len(dup.Rows) > 0 {
				lead(cycle, "W4 unique key duplicated", map[string]any{"duplicates": huntTrim(dup)})
			}
		}
		for id, count := range present {
			if count > 1 {
				lead(cycle, "transaction present more than once", map[string]any{"id": id, "count": count})
			}
			if !state.begun[id] {
				lead(cycle, "write present that was never begun", map[string]any{"id": id})
			}
			if state.failed[id] {
				lead(cycle, "write reported as failed is present", map[string]any{"id": id})
			}
		}
		missing := []int{}
		for id, at := range state.acked {
			if _, ok := present[id]; ok {
				continue
			}
			if workload == "W4" && killedAt.Sub(time.Unix(0, at)) <= time.Second {
				continue
			}
			missing = append(missing, id)
		}
		totalPresent := 0
		for _, c := range present {
			totalPresent += c
		}
		t.Logf("HUNT_H3_CYCLE workload=%s cycle=%d begun=%d acked=%d failed=%d present=%d missing=%d checkFailed=%v killAfterOpenMs=%d",
			workload, cycle, lastBegun, lastAcked, lastFailed, totalPresent, len(missing), checkFailed, killDelay.Milliseconds())
		if checkFailed {
			missing = nil // already reported as a failed check
		}
		if len(missing) > 0 {
			if len(missing) > 20 {
				missing = missing[:20]
			}
			lead(cycle, "acknowledged write missing after kill", map[string]any{"missingFirst20": missing, "killedAt": killedAt.UnixNano()})
		}
		h.Close(t)
		completed++
		if leads >= 50 {
			break
		}
	}
	huntCoverage(t, map[string]any{"ground": "H3", "part": "kill", "workload": workload, "seed": seed, "cyclesPlanned": cycles,
		"cyclesCompleted": completed, "leads": leads, "seconds": int(time.Since(started).Seconds()),
		"begun": lastBegun, "acknowledged": lastAcked, "failed": lastFailed, "childErrorsFirst5": lastErrors})
}

func huntTail(text string, n int) string {
	if len(text) <= n {
		return text
	}
	return text[len(text)-n:]
}
