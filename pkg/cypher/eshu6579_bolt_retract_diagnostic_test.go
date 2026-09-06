// SPDX-License-Identifier: MIT
// Diagnostic-only reproduction of Eshu #6579, not a production fix or causal proof.
package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const eshu6579Upsert = `UNWIND $rows AS row
MATCH (s:Function {uid: row.source_function_uid})
MATCH (t:Function {uid: row.sink_function_uid})
MERGE (s)-[rel:TAINT_FLOWS_TO {evidence_uid: row.uid}]->(t)
SET rel.sink_kind = row.sink_kind,
    rel.source_kind = row.source_kind,
    rel.confidence = row.confidence,
    rel.cloud = row.cloud,
    rel.relative_path = row.relative_path,
    rel.why_trail_json = row.why_trail_json,
    rel.why_trail_truncated = row.why_trail_truncated,
    rel.scope_id = row.scope_id,
    rel.generation_id = row.generation_id,
    rel.evidence_source = row.evidence_source`

const eshu6579Retract = `UNWIND $source_uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->(:Function)
WHERE rel.evidence_source = $evidence_source
DELETE rel`

// Requires an explicitly opted-in disposable backend. Ordinary suites skip it;
// an opted-in run with a missing DSN fails instead of silently skipping.
func TestEshu6579BoltRetractDiagnostic(t *testing.T) {
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
	for _, mode := range []string{"delete", "rewrite", "endpoint"} {
		t.Run(mode, func(t *testing.T) { eshu6579BoltArm(t, driver, mode) })
	}
}

// Each operation and each retry opens a new independent autocommit session.
// Only Eshu's exact isNornicDBWriteConflict text predicate is copied here.
// This intentionally does not emulate its other retry categories or retry not-found.
func eshu6579BoltCall(ctx context.Context, t *testing.T, driver neo4j.DriverWithContext, phase, query string, params map[string]any) ([]*neo4j.Record, error) {
	for attempt := 0; ; attempt++ {
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: os.Getenv("ESHU6579_BOLT_DATABASE"), AccessMode: neo4j.AccessModeWrite})
		result, err := session.Run(ctx, query, params)
		var records []*neo4j.Record
		if err == nil {
			records, err = result.Collect(ctx)
		}
		closeErr := session.Close(ctx)
		if err == nil {
			err = closeErr
		}
		if err == nil {
			return records, nil
		}
		msg := err.Error()
		conflict := strings.Contains(msg, "conflict:") && strings.Contains(msg, "changed after transaction start")
		t.Logf("caller_phase=%s attempt=%d recognized_conflict=%t error=%v", phase, attempt, conflict, err)
		if !conflict || attempt == 3 {
			return nil, fmt.Errorf("caller_phase=%s attempt=%d: %w", phase, attempt, err)
		}
		delay := 50 * time.Millisecond * time.Duration(1<<uint(attempt))
		jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(jitter):
		}
	}
}

func eshu6579BoltArm(t *testing.T, driver neo4j.DriverWithContext, mode string) {
	const anchors, trials, workers = 64, 12, 4
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("eshu6579-%s-%d", mode, time.Now().UnixNano())
	call := func(phase, query string, params map[string]any) []*neo4j.Record {
		t.Helper()
		records, err := eshu6579BoltCall(ctx, t, driver, phase, query, params)
		if err != nil {
			t.Fatal(err)
		}
		return records
	}
	call("index", "CREATE INDEX eshu6579_function_uid IF NOT EXISTS FOR (f:Function) ON (f.uid)", nil)
	rows, controls := make([]map[string]any, anchors), make([]map[string]any, anchors)
	uids := make([]string, anchors)
	allUIDs := make([]string, 0, anchors*4)
	for i := range rows {
		source, sink := fmt.Sprintf("%s-src-%d", prefix, i), fmt.Sprintf("%s-sink-%d", prefix, i)
		uids[i] = source
		rows[i] = map[string]any{"source_function_uid": source, "sink_function_uid": sink, "uid": fmt.Sprintf("%s-edge-%d", prefix, i), "scope_id": prefix, "generation_id": "seed", "evidence_source": prefix, "sink_kind": "sink", "source_kind": "source", "confidence": 0.5, "cloud": "", "relative_path": "fixture.go", "why_trail_json": "[]", "why_trail_truncated": false}
		allUIDs = append(allUIDs, source, sink)
		if mode == "endpoint" {
			source += "-control"
			sink += "-control"
			allUIDs = append(allUIDs, source, sink)
		}
		controls[i] = map[string]any{}
		for k, v := range rows[i] {
			controls[i][k] = v
		}
		controls[i]["source_function_uid"], controls[i]["sink_function_uid"] = source, sink
		controls[i]["uid"], controls[i]["evidence_source"] = fmt.Sprintf("%s-other-%d", prefix, i), prefix+"-other"
	}
	// Fixture cleanup is restricted to the generated UIDs, never the whole database.
	defer func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, err := eshu6579BoltCall(cleanupCtx, t, driver, "cleanup", `UNWIND $uids AS uid MATCH (f:Function {uid:uid}) DETACH DELETE f`, map[string]any{"uids": allUIDs})
		if err != nil {
			t.Errorf("owned fixture cleanup: %v", err)
		}
	}()
	const endpoints = `UNWIND $rows AS row MERGE (s:Function {uid:row.source_function_uid}) MERGE (t:Function {uid:row.sink_function_uid})`
	call("seed_endpoints", endpoints, map[string]any{"rows": rows})
	call("seed_control_endpoints", endpoints, map[string]any{"rows": controls})
	call("seed_controls", eshu6579Upsert, map[string]any{"rows": controls})
	tuples := func(source string) []string {
		records := call("truth", `MATCH (s:Function)-[rel:TAINT_FLOWS_TO]->(t:Function) WHERE rel.evidence_source=$source RETURN s.uid AS source, t.uid AS sink, properties(rel) AS properties`, map[string]any{"source": source})
		out := make([]string, len(records))
		for i, r := range records {
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
	if len(before) != anchors {
		t.Fatalf("control seed=%d want%d", len(before), anchors)
	}
	retractParams := map[string]any{"source_uids": uids, "evidence_source": prefix}
	for trial := 0; trial < trials; trial++ {
		call("seed_target", eshu6579Upsert, map[string]any{"rows": rows})
		if n := len(tuples(prefix)); n != anchors {
			t.Fatalf("seed target=%d", n)
		}
		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(workers)
		results := make(chan error, workers)
		for worker := 0; worker < workers; worker++ {
			go func(worker int) {
				ready.Done()
				<-start
				run := func(phase, q string, p map[string]any) error {
					_, err := eshu6579BoltCall(ctx, t, driver, fmt.Sprintf("%s/trial%d/worker%d/%s", mode, trial, worker, phase), q, p)
					t.Logf("mode=%s trial=%d worker=%d caller_phase=%s completed=true error=%v", mode, trial, worker, phase, err)
					return err
				}
				var err error
				if mode == "endpoint" && worker == workers-1 {
					err = run("endpoint_delete", `UNWIND $source_uids AS suid MATCH (s:Function {uid:suid}) DETACH DELETE s`, retractParams)
					if err == nil {
						err = run("endpoint_recreate", endpoints, map[string]any{"rows": rows})
					}
				} else {
					err = run("retract", eshu6579Retract, retractParams)
					if err == nil && mode == "rewrite" {
						err = run("upsert", eshu6579Upsert, map[string]any{"rows": rows})
					}
				}
				results <- err
			}(worker)
		}
		ready.Wait()
		close(start)
		for worker := 0; worker < workers; worker++ {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
		want := 0
		if mode == "rewrite" {
			want = anchors
		}
		if n := len(tuples(prefix)); n != want {
			t.Errorf("trial%d target=%d want%d", trial, n, want)
		}
		if after := tuples(prefix + "-other"); !slices.Equal(before, after) {
			t.Fatalf("trial%d unaffected tuples changed: before=%v after=%v", trial, before, after)
		}
		// Exact endpoint pair multiset also detects missing or duplicate recreated nodes.
		pairs := call("endpoint_truth", `UNWIND $rows AS row MATCH (s:Function {uid:row.source_function_uid}) MATCH (t:Function {uid:row.sink_function_uid}) RETURN s.uid AS source,t.uid AS sink`, map[string]any{"rows": rows})
		actual, expected := make([]string, len(pairs)), make([]string, len(rows))
		for i, r := range pairs {
			b, err := json.Marshal(r.AsMap())
			if err != nil {
				t.Fatal(err)
			}
			actual[i] = string(b)
		}
		for i, r := range rows {
			b, err := json.Marshal(map[string]any{"source": r["source_function_uid"], "sink": r["sink_function_uid"]})
			if err != nil {
				t.Fatal(err)
			}
			expected[i] = string(b)
		}
		slices.Sort(actual)
		slices.Sort(expected)
		if !slices.Equal(actual, expected) {
			t.Fatal("endpoint pair multiset changed")
		}
		call("replay_retract", eshu6579Retract, retractParams)
		call("empty_replay_retract", eshu6579Retract, retractParams)
		call("empty_input", eshu6579Retract, map[string]any{"source_uids": []string{}, "evidence_source": prefix})
		if n := len(tuples(prefix)); n != 0 {
			t.Fatalf("empty replay target=%d", n)
		}
		call("replay_upsert", eshu6579Upsert, map[string]any{"rows": rows})
		expectedTarget := tuples(prefix)
		call("duplicate_upsert", eshu6579Upsert, map[string]any{"rows": rows})
		if after := tuples(prefix); len(after) != anchors || !slices.Equal(expectedTarget, after) {
			t.Fatal("duplicate replay changed target multiset")
		}
		if !slices.Equal(before, tuples(prefix+"-other")) {
			t.Fatal("replay changed unaffected evidence")
		}
		t.Logf("mode=%s trial=%d workers=%d anchors=%d truth_checked=true", mode, trial, workers, anchors)
	}
}
