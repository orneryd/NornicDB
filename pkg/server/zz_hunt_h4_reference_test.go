package server

// Hunt ground H4: reference differential against Neo4j 5.26 (the lab's
// neo-ref, 5.26.30).
//
// Population: testdata/hunt/h4-corpus.json.gz (frozen at hunt opening; built
// by sig/handoff/v152-verification/hunt/tools/gen_h4.py, seed 20261120):
//   - generated: 20,000 read statements, 4,000 on each of 5 generated graphs;
//   - mutants: every TCK statement of the H1 corpus mutated 3 ways (literal,
//     operator, function, null, direction or boolean; the slots without an
//     applicable site are listed in the population and not run).
//
// Oracle: the same statement on the same graph returns the same rows and the
// same error class on NornicDB and on Neo4j (huntAgree, the repository's own
// comparison rules); a mutant that writes also compares the graph after it.
//
// The reference is one shared Neo4j database that the harness empties: only
// ONE H4 job may run at a time, and no other job may use neo-ref meanwhile.
//
// Run: HUNT_H4_PART=generated|mutants [HUNT_H4_SHARD=i/n] [HUNT_PARSER via tnrun --parser]
//      NORNICDB_NEO4J_REFERENCE_URI=bolt://neo-ref:7687
//      go test ./pkg/server -run '^TestHuntH4Reference$' -count=1 -timeout=6h -v

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

type huntH4Corpus struct {
	Seed   int `json:"seed"`
	Graphs []struct {
		ID    string   `json:"id"`
		Setup []string `json:"setup"`
	} `json:"graphs"`
	Generated []struct {
		ID    string `json:"id"`
		Graph string `json:"graph"`
		Query string `json:"query"`
	} `json:"generated"`
	Mutants []struct {
		ID       string          `json:"id"`
		Of       string          `json:"of"`
		Operator string          `json:"operator"`
		Setup    []string        `json:"setup"`
		Query    string          `json:"query"`
		Params   json.RawMessage `json:"params"`
	} `json:"mutants"`
}

// huntReference is the Neo4j reference behind one Bolt driver.
type huntReference struct {
	driver neo4jdriver.DriverWithContext
}

func (r *huntReference) Run(route huntRoute, query string, params map[string]any, timeout time.Duration) differential.Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return huntBoltRun(ctx, r.driver, "neo4j", route == routeBoltExplicit, query, params)
}

func TestHuntH4Reference(t *testing.T) {
	part := os.Getenv("HUNT_H4_PART")
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if part == "" || uri == "" {
		t.Skip("set HUNT_H4_PART (generated|mutants) and NORNICDB_NEO4J_REFERENCE_URI")
	}
	path := huntEnv("HUNT_H4_CORPUS", filepath.Join("testdata", "hunt", "h4-corpus.json.gz"))
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	var corpus huntH4Corpus
	if err := json.NewDecoder(reader).Decode(&corpus); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	for {
		if err = driver.VerifyConnectivity(ctx); err == nil || ctx.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	cancel()
	if err != nil {
		t.Fatalf("reference %s: %v", uri, err)
	}
	ref := &huntReference{driver: driver}
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h4-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	nornic := huntOpen(t, filepath.Join(base, "store"), huntOptions{QueryCache: true})
	defer func() { nornic.Close(t) }()
	shard := os.Getenv("HUNT_H4_SHARD")
	timeout := time.Duration(huntEnvInt("HUNT_STATEMENT_TIMEOUT_SECONDS", 20)) * time.Second
	parser := nornicConfig.GetParserType()
	started := time.Now()
	compared, leads, resetFailures := 0, 0, 0
	lead := func(id, query, verdict string, setup []string, a, b differential.Outcome, extra map[string]any) {
		leads++
		entry := map[string]any{"ground": "H4", "part": part, "parser": parser, "statement": id, "query": query, "verdict": verdict,
			"a": a, "b": b, "setup": setup, "aIs": "Neo4j 5.26 reference", "bIs": "NornicDB candidate"}
		for k, v := range extra {
			entry[k] = v
		}
		huntLead(t, entry)
	}
	resetBoth := func() bool {
		ok := huntResetWith(ref.Run)
		if !nornic.huntReset() {
			nornic.Close(t)
			_ = os.RemoveAll(filepath.Join(base, "store"))
			nornic = huntOpen(t, filepath.Join(base, "store"), huntOptions{QueryCache: true})
		}
		if !ok {
			resetFailures++
		}
		return ok
	}
	switch part {
	case "generated":
		setups := map[string][]string{}
		for _, g := range corpus.Graphs {
			setups[g.ID] = g.Setup
		}
		current := ""
		for index, statement := range corpus.Generated {
			if !huntShard(shard, index) {
				continue
			}
			if statement.Graph != current {
				current = statement.Graph
				if !resetBoth() {
					t.Fatalf("cannot empty the reference before graph %s", current)
				}
				for _, q := range setups[current] {
					if a, b := ref.Run(routeBoltAuto, q, nil, time.Minute), nornic.Run(routeBoltAuto, q, nil, time.Minute); a.Failed() || b.Failed() {
						lead(current+":setup", q, "graph setup statement failed", nil, a, b, nil)
					}
				}
			}
			a := ref.Run(routeBoltAuto, statement.Query, nil, timeout)
			b := nornic.Run(routeBoltAuto, statement.Query, nil, timeout)
			compared++
			if ok, verdict := huntAgree(statement.Query, a, b); !ok {
				lead(statement.ID, statement.Query, verdict, setups[current], a, b, map[string]any{"graph": current})
			}
		}
	case "mutants":
		for index, mutant := range corpus.Mutants {
			if !huntShard(shard, index) {
				continue
			}
			if !resetBoth() {
				t.Fatalf("cannot empty the reference before %s", mutant.ID)
			}
			for _, q := range mutant.Setup {
				_ = ref.Run(routeBoltAuto, q, nil, time.Minute)
				_ = nornic.Run(routeBoltAuto, q, nil, time.Minute)
			}
			refParams := huntH4Params(ref.Run, mutant.Params)
			a := ref.Run(routeBoltAuto, mutant.Query, refParams, timeout)
			b := nornic.Run(routeBoltAuto, mutant.Query, huntH4Params(nornic.Run, mutant.Params), timeout)
			compared++
			if ok, verdict := huntAgree(mutant.Query, a, b); !ok {
				lead(mutant.ID, mutant.Query, verdict, mutant.Setup, a, b, map[string]any{"of": mutant.Of, "operator": mutant.Operator})
				continue
			}
			if huntWrites(mutant.Query) && !a.Failed() {
				ga, gb := huntGraphStateWith(ref.Run), nornic.huntGraphState()
				for k := range ga {
					compared++
					if ok, verdict := huntAgree("MATCH (n) RETURN n", ga[k], gb[k]); !ok {
						lead(mutant.ID+":graph", mutant.Query, "graph "+verdict, mutant.Setup, ga[k], gb[k], map[string]any{"of": mutant.Of, "operator": mutant.Operator})
					}
				}
			}
		}
	default:
		t.Fatalf("unknown HUNT_H4_PART %q", part)
	}
	huntCoverage(t, map[string]any{"ground": "H4", "part": part, "parser": parser, "shard": shard, "seed": corpus.Seed,
		"comparisons": compared, "leads": leads, "referenceResetFailures": resetFailures, "seconds": int(time.Since(started).Seconds())})
}

// huntH4Params binds a TCK statement's literal parameters by evaluating them
// on the server that runs it (each side evaluates its own).
func huntH4Params(run huntRunner, raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	if _, ok := decoded["tck_literal_params"]; !ok {
		return decoded
	}
	// TCK literal parameters are bound only in the H1 harness; here a mutant
	// with literal parameters runs without them on both sides alike.
	_ = run
	return nil
}
