// Command projected-query compares real Bolt projections on a combined corpus.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/experiments/property-storage/corpus"
)

const documentProjection = `MATCH (d:ProjectionDocument {key: $key}) RETURN d.key AS key, d.path AS path, d.status AS status`
const documentFull = `MATCH (d:ProjectionDocument {key: $key}) RETURN d.key AS key, d.path AS path, d.body AS body, d.noise AS noise, d.metrics AS metrics`
const productProjection = `MATCH (p:Product {productID: $id}) RETURN p.productID AS id, p.productName AS name, p.unitPrice AS price, p.unitsInStock AS stock`
const batchProjection = `UNWIND $keys AS key MATCH (d:ProjectionDocument {key: key}) RETURN d.key AS key, d.path AS path, d.status AS status`

func query(ctx context.Context, session neo4j.SessionWithContext, text string, params map[string]any) ([][]any, error) {
	result, err := session.Run(ctx, text, params)
	if err != nil {
		return nil, err
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = result.Consume(ctx); err != nil {
		return nil, err
	}
	rows := make([][]any, len(records))
	for i, record := range records {
		rows[i] = record.Values
	}
	return rows, nil
}

func rss(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("server RSS not found")
}

func run() error {
	uri := flag.String("uri", "bolt://127.0.0.1:17687", "isolated laboratory server")
	docs := flag.String("docs", "/corpus", "frozen documentation corpus")
	out := flag.String("out", "", "receipt path")
	verify := flag.Bool("verify", false, "verify all stored document fields against the source corpus")
	seed := flag.Bool("seed", false, "append only the document fixture to an existing Northwind store")
	pid := flag.Int("pid", 0, "database server PID for Linux RSS sampling")
	count := flag.Int("queries", 4000, "queries per phase")
	workers := flag.Int("workers", 8, "concurrent Bolt clients")
	scanOnly := flag.Bool("scan-only", false, "measure the label-streaming projection path")
	warmup := flag.Int("warmup", 64, "warm-up calls per phase")
	flag.Parse()
	ctx := context.Background()
	base, digest, err := corpus.Load(*docs)
	if err != nil {
		return err
	}
	records := corpus.Replicate(base, 16)
	driver, err := neo4j.NewDriverWithContext(*uri, neo4j.NoAuth(), func(c *neo4j.Config) { c.MaxConnectionPoolSize = *workers + 2 })
	if err != nil {
		return err
	}
	defer driver.Close(ctx)
	if err = driver.VerifyConnectivity(ctx); err != nil {
		return err
	}
	makeSession := func() neo4j.SessionWithContext {
		return driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	}
	session := makeSession()
	defer session.Close(ctx)
	if *seed {
		if _, err = query(ctx, session, `CREATE CONSTRAINT projection_document_key IF NOT EXISTS FOR (d:ProjectionDocument) REQUIRE d.key IS UNIQUE`, nil); err != nil {
			return err
		}
		for start := 0; start < len(records); start += 128 {
			var rows []map[string]any
			for _, record := range records[start:min(start+128, len(records))] {
				row := make(map[string]any, len(record.Properties)+1)
				for k, v := range record.Properties {
					row[k] = v
				}
				row["key"] = record.ID
				rows = append(rows, row)
			}
			if _, err = query(ctx, session, `UNWIND $rows AS row CREATE (:ProjectionDocument {key: row.key, path: row.path, body: row.body, noise: row.noise, metrics: row.metrics, status: row.status})`, map[string]any{"rows": rows}); err != nil {
				return fmt.Errorf("seed %d: %w", start, err)
			}
		}
		rows, err := query(ctx, session, `MATCH (d:ProjectionDocument) RETURN count(d)`, nil)
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0][0] != int64(len(records)) {
			return fmt.Errorf("document count: %v", rows)
		}
		fmt.Println("seeded", len(records), "documents", digest)
		return nil
	}
	if *verify {
		rows, err := query(ctx, session, `MATCH (d:ProjectionDocument) RETURN d.key, d.path, d.body, d.noise, d.metrics, d.status`, nil)
		if err != nil {
			return err
		}
		expected := make(map[string]string, len(records))
		for _, record := range records {
			encoded, err := json.Marshal([]any{record.ID, record.Properties["path"], record.Properties["body"], record.Properties["noise"], record.Properties["metrics"], record.Properties["status"]})
			if err != nil {
				return err
			}
			expected[record.ID] = string(encoded)
		}
		if len(rows) != len(expected) {
			return fmt.Errorf("verification count %d != %d", len(rows), len(expected))
		}
		for _, row := range rows {
			key, ok := row[0].(string)
			if !ok {
				return fmt.Errorf("non-string document key")
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				return err
			}
			want, exists := expected[key]
			if !exists || string(encoded) != want {
				return fmt.Errorf("source corpus mismatch for %s", key)
			}
			delete(expected, key)
		}
		fmt.Println("verified all fields against source corpus:", len(rows), digest)
		return nil
	}
	if *out == "" || *pid < 1 || *count < 100 || *workers < 1 {
		return fmt.Errorf("out, server pid, >=100 queries and positive workers required")
	}
	startRSS, err := rss(*pid)
	if err != nil {
		return err
	}
	var phases []map[string]any
	kinds := []string{"document_projection", "northwind_projection", "mixed_projection", "document_batch_projection", "document_full_control"}
	if *scanOnly {
		kinds = []string{"document_label_projection"}
	}
	for _, kind := range kinds {
		operation := func(s neo4j.SessionWithContext, i int) ([][]any, error) {
			if kind == "document_label_projection" {
				return query(ctx, s, `MATCH (d:ProjectionDocument) RETURN d.key AS key, d.path AS path, d.status AS status`, nil)
			}
			index := (i * 997) % len(records)
			if kind == "northwind_projection" || (kind == "mixed_projection" && i%2 == 0) {
				return query(ctx, s, productProjection, map[string]any{"id": int64((i*7919)%48000 + 1)})
			}
			if kind == "document_batch_projection" {
				keys := make([]string, 16)
				for n := range keys {
					keys[n] = records[(index+n)%len(records)].ID
				}
				return query(ctx, s, batchProjection, map[string]any{"keys": keys})
			}
			text := documentProjection
			if kind == "document_full_control" {
				text = documentFull
			}
			return query(ctx, s, text, map[string]any{"key": records[index].ID})
		}
		for i := 0; i < *warmup; i++ {
			if _, err = operation(session, i); err != nil {
				return fmt.Errorf("warmup %s: %w", kind, err)
			}
		}
		beforeRSS, err := rss(*pid)
		if err != nil {
			return err
		}
		peak := beforeRSS
		stop := make(chan struct{})
		sampleDone := make(chan error, 1)
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					value, e := rss(*pid)
					if e != nil {
						sampleDone <- e
						return
					}
					peak = max(peak, value)
				case <-stop:
					sampleDone <- nil
					return
				}
			}
		}()
		samples := make([]int64, *count)
		hashes := make([][32]byte, *count)
		var wg sync.WaitGroup
		failures := make(chan error, *workers)
		barrier := make(chan struct{})
		for worker := 0; worker < *workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				s := makeSession()
				defer s.Close(ctx)
				<-barrier
				for i := worker; i < *count; i += *workers {
					start := time.Now()
					rows, e := operation(s, i)
					samples[i] = time.Since(start).Nanoseconds()
					if e != nil {
						failures <- e
						return
					}
					expected := 1
					if kind == "document_label_projection" {
						expected = len(records)
					}
					if kind == "document_batch_projection" {
						expected = 16
					}
					if len(rows) != expected {
						failures <- fmt.Errorf("%s expected %d rows, got %d", kind, expected, len(rows))
						return
					}
					// Result order is not a Cypher guarantee without ORDER BY.
					sort.Slice(rows, func(a, b int) bool { return fmt.Sprint(rows[a][0]) < fmt.Sprint(rows[b][0]) })
					encoded, e := json.Marshal(rows)
					if e != nil {
						failures <- e
						return
					}
					hashes[i] = sha256.Sum256(encoded)
				}
			}(worker)
		}
		begin := time.Now()
		close(barrier)
		wg.Wait()
		elapsed := time.Since(begin)
		close(stop)
		sampleErr := <-sampleDone
		close(failures)
		for e := range failures {
			return fmt.Errorf("%s: %w", kind, e)
		}
		if sampleErr != nil {
			return sampleErr
		}
		afterRSS, err := rss(*pid)
		if err != nil {
			return err
		}
		peak = max(peak, afterRSS)
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		var total int64
		for _, v := range samples {
			total += v
		}
		combined := sha256.New()
		for _, h := range hashes {
			combined.Write(h[:])
		}
		phases = append(phases, map[string]any{"name": kind, "queries": *count, "workers": *workers, "mean_ms": float64(total) / float64(*count) / 1e6, "p50_ms": float64(samples[len(samples)/2]) / 1e6, "p95_ms": float64(samples[len(samples)*95/100]) / 1e6, "p99_ms": float64(samples[len(samples)*99/100]) / 1e6, "queries_second": float64(*count) / elapsed.Seconds(), "server_rss_before_kib": beforeRSS, "server_rss_after_kib": afterRSS, "sampled_server_peak_rss_kib": peak, "result_sha256": hex.EncodeToString(combined.Sum(nil))})
		fmt.Println(kind, "queries", *count, "p95 ms", float64(samples[len(samples)*95/100])/1e6, "RSS KiB", peak)
	}
	result := map[string]any{"corpus_sha256": digest, "documents": len(records), "northwind_nodes": 97440, "northwind_relationships": 312050, "query_cache_enabled": false, "server_start_rss_kib": startRSS, "phases": phases, "sampling_interval_ms": 10}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*out, data, 0600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
