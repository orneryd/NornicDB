package cypher

// Hunt ground H10 (decoders and parsers), targets 1-3 of 8:
//
//	FuzzHuntH10NornicParse  the default parser (Parser.Parse)
//	FuzzHuntH10ANTLRParse   the ANTLR parser (antlr.Parse)
//	FuzzHuntH10Execute      a whole statement through StorageExecutor.Execute on an
//	                        empty in-memory database (run under --parser both)
//
// Oracle: no panic; no hang (each input finishes within 10 s; Execute runs with
// a 2 s deadline and must return within 10 s). Errors are expected and fine.
// Seeds: every runnable statement of the H1 corpus (pkg/server/testdata/hunt).
//
// Run (one target per job, 30 CPU-minutes each):
//   go test ./pkg/cypher -run '^$' -fuzz '^FuzzHuntH10NornicParse$' -fuzztime 30m -parallel 1

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// huntH10LeadSeeds are seed indexes left out because they are already leads
// (DECISIONS 15): seed#1320 = H1-tck-00390, the TCK movie-graph CREATE script
// (L-H10-002). Only targets 1 and 3 leave them out.
var huntH10LeadSeeds = map[int]bool{1320: true}

func huntH10Seeds(f *testing.F) { huntH10SeedsSkipping(f, nil) }

func huntH10SeedsSkipping(f *testing.F, skip map[int]bool) {
	f.Helper()
	file, err := os.Open(filepath.Join("..", "server", "testdata", "hunt", "h1-corpus.json.gz"))
	if err != nil {
		f.Logf("no H1 corpus seeds: %v", err)
		f.Add("RETURN 1")
		return
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		f.Fatal(err)
	}
	var corpus struct {
		Groups []struct {
			Setup      []string `json:"setup"`
			Statements []struct {
				Query string   `json:"query"`
				Flags []string `json:"flags"`
			} `json:"statements"`
		} `json:"groups"`
	}
	if err := json.NewDecoder(reader).Decode(&corpus); err != nil {
		f.Fatal(err)
	}
	index := 0
	add := func(q string) {
		if skip[index] {
			f.Logf("H10 seed left out (already a lead): seed#%d, %d bytes, %.60q", index, len(q), q)
		} else {
			f.Add(q)
		}
		index++
	}
	for _, g := range corpus.Groups {
		for _, s := range g.Setup {
			add(s)
		}
		for _, s := range g.Statements {
			add(s.Query)
		}
	}
}

func huntH10Bounded(t *testing.T, limit time.Duration, input string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("hang: no answer within %s for input %q", limit, input)
	}
}

func FuzzHuntH10NornicParse(f *testing.F) {
	huntH10SeedsSkipping(f, huntH10LeadSeeds)
	parser := NewParser()
	f.Fuzz(func(t *testing.T, query string) {
		huntH10Remember("FuzzHuntH10NornicParse", []byte(query))
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, query)
			}
		}()
		huntH10Bounded(t, 10*time.Second, query, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = parser.Parse(ctx, query)
		})
	})
}

func FuzzHuntH10ANTLRParse(f *testing.F) {
	huntH10Seeds(f)
	f.Fuzz(func(t *testing.T, query string) {
		huntH10Remember("FuzzHuntH10ANTLRParse", []byte(query))
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, query)
			}
		}()
		huntH10Bounded(t, 10*time.Second, query, func() {
			_, _ = antlr.Parse(query)
		})
	})
}

func FuzzHuntH10Execute(f *testing.F) {
	huntH10SeedsSkipping(f, huntH10LeadSeeds)
	f.Fuzz(func(t *testing.T, query string) {
		huntH10Remember("FuzzHuntH10Execute", []byte(query))
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, query)
			}
		}()
		engine := storage.NewMemoryEngine()
		defer engine.Close()
		executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
		huntH10Bounded(t, 10*time.Second, query, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = executor.Execute(ctx, query, nil)
		})
	})
}

// huntH10Remember keeps the input being run in $HUNT_H10_LAST_DIR/<target>.last
// (the lab's shared /work/tnrun dataset), so that an input that crashes the
// process outside the fuzz goroutine survives the job.
func huntH10Remember(target string, data []byte) {
	dir := os.Getenv("HUNT_H10_LAST_DIR")
	if dir == "" {
		return
	}
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, target+".last"), data, 0o644)
}
