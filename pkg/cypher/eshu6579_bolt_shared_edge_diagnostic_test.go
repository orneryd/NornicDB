// SPDX-License-Identifier: MIT
// Diagnostic scheduling experiment for Eshu #6579; no root-cause claim.
package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// TestEshu6579BoltSharedEdgeDiagnostic keeps independent scope generations
// contending on one generation-independent edge, without per-round barriers.
func TestEshu6579BoltSharedEdgeDiagnostic(t *testing.T) {
	if os.Getenv("ESHU6579_BOLT_DIAGNOSTIC") != "1" {
		t.Skip("set ESHU6579_BOLT_DIAGNOSTIC=1 for disposable backend")
	}
	dsn := strings.TrimSpace(os.Getenv("ESHU6579_BOLT_DSN"))
	if dsn == "" {
		t.Fatal("ESHU6579_BOLT_DSN required")
	}
	auth := neo4j.NoAuth()
	if user := os.Getenv("ESHU6579_BOLT_USER"); user != "" {
		auth = neo4j.BasicAuth(user, os.Getenv("ESHU6579_BOLT_PASSWORD"), "")
	}
	driver, err := neo4j.NewDriverWithContext(dsn, auth)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := driver.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const workers, rounds = 4, 24
	prefix := fmt.Sprintf("eshu6579-shared-%d", time.Now().UnixNano())
	source, sink := prefix+"-source", prefix+"-sink"
	call := func(phase, q string, p map[string]any) []*neo4j.Record {
		t.Helper()
		r, err := eshu6579BoltCall(ctx, t, driver, phase, q, p)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, err := eshu6579BoltCall(c, t, driver, "cleanup", `UNWIND $uids AS uid MATCH (f:Function {uid:uid}) DETACH DELETE f`, map[string]any{"uids": []string{source, sink}})
		if err != nil {
			t.Errorf("owned cleanup: %v", err)
		}
	}()
	call("index", "CREATE INDEX eshu6579_function_uid IF NOT EXISTS FOR (f:Function) ON (f.uid)", nil)
	call("endpoints", `MERGE (s:Function {uid:$source}) MERGE (t:Function {uid:$sink})`, map[string]any{"source": source, "sink": sink})
	makeRow := func(worker, round int) map[string]any {
		return map[string]any{"uid": prefix + "-edge", "source_function_uid": source, "sink_function_uid": sink, "sink_kind": "sink", "source_kind": "source", "confidence": 0.5, "cloud": false, "relative_path": "fixture.go", "why_trail_json": "[]", "why_trail_truncated": false, "scope_id": fmt.Sprintf("scope-%d", worker), "generation_id": fmt.Sprintf("generation-%d-%d", worker, round), "evidence_source": prefix}
	}
	control := makeRow(-1, -1)
	control["uid"] = prefix + "-control"
	control["evidence_source"] = prefix + "-other"
	call("control_seed", eshu6579Upsert, map[string]any{"rows": []map[string]any{control}})
	tuples := func(evidence string) []string {
		rs := call("truth", `MATCH (s:Function)-[rel:TAINT_FLOWS_TO]->(t:Function) WHERE rel.evidence_source=$source RETURN s.uid AS source,t.uid AS sink,properties(rel) AS properties`, map[string]any{"source": evidence})
		out := make([]string, len(rs))
		for i, r := range rs {
			b, err := json.Marshal(r.AsMap())
			if err != nil {
				t.Fatal(err)
			}
			out[i] = string(b)
		}
		slices.Sort(out)
		return out
	}
	before := tuples(prefix + "-other")
	if len(before) != 1 {
		t.Fatal("control seed must contain exactly one tuple")
	}
	// Derive the complete expected observable tuple, including any backend-added
	// properties, from an actual seed; replace only the submitted scope/generation.
	call("target_seed", eshu6579Upsert, map[string]any{"rows": []map[string]any{makeRow(0, 0)}})
	seed := tuples(prefix)
	if len(seed) != 1 {
		t.Fatal("target seed must contain exactly one tuple")
	}
	var expected map[string]any
	if err := json.Unmarshal([]byte(seed[0]), &expected); err != nil {
		t.Fatal(err)
	}
	props, ok := expected["properties"].(map[string]any)
	if !ok {
		t.Fatal("target properties missing")
	}
	allowed := make(map[string]bool, workers*rounds)
	for w := 0; w < workers; w++ {
		for r := 0; r < rounds; r++ {
			row := makeRow(w, r)
			props["scope_id"] = row["scope_id"]
			props["generation_id"] = row["generation_id"]
			b, err := json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
			allowed[string(b)] = true
		}
	}
	params := map[string]any{"source_uids": []string{source}, "evidence_source": prefix}
	var ready sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	results := make(chan int, workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			failed := 0
			defer func() { results <- failed }()
			ready.Done()
			<-start
			for round := 0; round < rounds; round++ {
				// Tiny deterministic pacing changes arrival order, not database knobs.
				select {
				case <-ctx.Done():
					t.Error(ctx.Err())
					failed++
					return
				case <-time.After(time.Duration(1+(worker+round)%4) * time.Millisecond):
				}
				phase := fmt.Sprintf("shared/worker%d/round%d", worker, round)
				_, err := eshu6579BoltCall(ctx, t, driver, phase+"/retract", eshu6579Retract, params)
				t.Logf("caller_phase=%s/retract error=%v", phase, err)
				if err != nil {
					t.Error(err)
					failed++
					continue
				}
				_, err = eshu6579BoltCall(ctx, t, driver, phase+"/upsert", eshu6579Upsert, map[string]any{"rows": []map[string]any{makeRow(worker, round)}})
				t.Logf("caller_phase=%s/upsert error=%v", phase, err)
				if err != nil {
					t.Error(err)
					failed++
				}
			}
		}(worker)
	}
	ready.Wait()
	close(start)
	failures := 0
	for w := 0; w < workers; w++ {
		failures += <-results
	}
	t.Logf("workers_joined=%d rounds_per_worker=%d failed_rounds=%d", workers, rounds, failures)
	final := tuples(prefix)
	if len(final) != 1 || !allowed[final[0]] {
		t.Fatalf("final target must be one complete submitted generation: %v", final)
	}
	if !slices.Equal(before, tuples(prefix+"-other")) {
		t.Fatal("unaffected tuple changed")
	}
	endpoints := call("endpoint_count", `UNWIND $uids AS uid MATCH (f:Function {uid:uid}) RETURN f.uid AS uid`, map[string]any{"uids": []string{source, sink}})
	got := make([]string, len(endpoints))
	for i, r := range endpoints {
		v, ok := r.Get("uid")
		if !ok {
			t.Fatal("uid missing")
		}
		s, ok := v.(string)
		if !ok {
			t.Fatal("uid not string")
		}
		got[i] = s
	}
	want := []string{source, sink}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("endpoint multiset=%v want%v", got, want)
	}
	row := makeRow(0, rounds-1)
	call("designated_replay", eshu6579Upsert, map[string]any{"rows": []map[string]any{row}})
	replay := tuples(prefix)
	call("duplicate_replay", eshu6579Upsert, map[string]any{"rows": []map[string]any{row}})
	if len(replay) != 1 || !allowed[replay[0]] || !slices.Equal(replay, tuples(prefix)) {
		t.Fatal("duplicate replay altered tuple")
	}
	call("final_retract", eshu6579Retract, params)
	call("empty_replay", eshu6579Retract, params)
	call("empty_input", eshu6579Retract, map[string]any{"source_uids": []string{}, "evidence_source": prefix})
	if len(tuples(prefix)) != 0 {
		t.Fatal("empty replay retained target")
	}
	if !slices.Equal(before, tuples(prefix+"-other")) {
		t.Fatal("replay changed unaffected tuple")
	}
}
