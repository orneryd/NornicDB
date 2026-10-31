package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
)

// Copy this file unchanged to the release base for paired measurements. Only
// public APIs are used, including the listening HTTP server. One benchmark op
// is 64 committed writes (192 requests for explicit transactions). Each batch
// gets a fresh tmpfs database; startup, warmup, validation, shutdown and removal
// are untimed. Unlike the older request benchmarks, neither nodes nor WAL files
// accumulate with b.N. These are end-to-end HTTP costs, including transport.
const jsonPublicHTTPBatch = 64

const jsonPublicHTTPCreate = `{"statement":"CREATE (:JSONPublicWrite {integer: $integer, whole: $whole, nested: $nested})","parameters":{"integer":9007199254740993,"whole":2.0,"nested":[9007199254740993,0.125,2.0]}}`

func BenchmarkJSONPublicImplicitWriteBounded(b *testing.B) {
	for _, name := range []string{"single_write", "three_statements"} {
		b.Run(name, func(b *testing.B) {
			body := `{"statements":[` + jsonPublicHTTPCreate + `]}`
			if name == "three_statements" {
				body = `{"statements":[` + jsonPublicHTTPCreate + `,{"statement":"MATCH (n:JSONPublicWrite) RETURN count(n) AS c"},{"statement":"RETURN 1 AS x"}]}`
			}
			jsonPublicHTTPBench(b, body, false)
		})
	}
}

func BenchmarkJSONPublicExplicitWriteBounded(b *testing.B) {
	jsonPublicHTTPBench(b, `{"statements":[`+jsonPublicHTTPCreate+`]}`, true)
}

func jsonPublicHTTPBench(b *testing.B, body string, explicit bool) {
	b.StopTimer()
	b.Cleanup(config.WithWALEnabled())
	root := jsonPublicHTTPTmpfs(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jsonPublicHTTPRunBatch(b, filepath.Join(root, "batch"), body, explicit)
	}
	b.ReportMetric(jsonPublicHTTPBatch, "writes/op")
	requests := jsonPublicHTTPBatch
	if explicit {
		requests *= 3
	}
	b.ReportMetric(float64(requests), "requests/op")
}

func jsonPublicHTTPTmpfs(b *testing.B) string {
	b.Helper()
	mounts, err := os.ReadFile("/proc/mounts")
	jsonPublicHTTPCheck(b, err)
	found := false
	for _, line := range strings.Split(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[1] == "/dev/shm" && fields[2] == "tmpfs" {
			found = true
		}
	}
	if !found {
		b.Fatal("public HTTP benchmarks require /dev/shm tmpfs; no disk fallback")
	}
	root, err := os.MkdirTemp("/dev/shm", "nornic-json-http-public-")
	jsonPublicHTTPCheck(b, err)
	b.Cleanup(func() { jsonPublicHTTPCheck(b, os.RemoveAll(root)) })
	return root
}

func jsonPublicHTTPCheck(b *testing.B, err error) {
	if err != nil {
		b.Helper()
		b.Fatal(err)
	}
}

func jsonPublicHTTPRunBatch(b *testing.B, dir, body string, explicit bool) {
	b.Helper()
	defer func() {
		b.StopTimer()
		jsonPublicHTTPCheck(b, os.RemoveAll(dir))
	}()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.WALSyncMode = "none"
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(dir, cfg)
	jsonPublicHTTPCheck(b, err)
	defer func() { jsonPublicHTTPCheck(b, db.Close()) }()
	sc := server.DefaultConfig()
	sc.Address, sc.Port = "127.0.0.1", 0
	sc.EmbeddingEnabled = false
	sc.MCPEnabled = false
	sc.RateLimitEnabled = false
	sc.ProcessConfig = cfg
	s, err := server.New(db, nil, sc)
	jsonPublicHTTPCheck(b, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		jsonPublicHTTPCheck(b, s.Stop(ctx))
	}()
	jsonPublicHTTPCheck(b, s.Start())
	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	endpoint := "http://" + s.Addr() + "/db/nornic/tx"
	jsonPublicHTTPPost(b, client, endpoint+"/commit", `{"statements":[{"statement":"RETURN 1"}]}`, http.StatusOK)
	b.StartTimer()
	for i := 0; i < jsonPublicHTTPBatch; i++ {
		if !explicit {
			jsonPublicHTTPPost(b, client, endpoint+"/commit", body, http.StatusOK)
			continue
		}
		commit := jsonPublicHTTPPost(b, client, endpoint, body, http.StatusCreated)
		if !strings.HasPrefix(commit, endpoint+"/") || !strings.HasSuffix(commit, "/commit") {
			b.Fatalf("unexpected transaction commit URI: %q", commit)
		}
		jsonPublicHTTPPost(b, client, strings.TrimSuffix(commit, "/commit"), `{"statements":[{"statement":"MATCH (n:JSONPublicWrite) RETURN count(n) AS c"}]}`, http.StatusOK)
		jsonPublicHTTPPost(b, client, commit, `{"statements":[]}`, http.StatusOK)
	}
	b.StopTimer()
	nodes, err := db.GetStorage().GetNodesByLabel("JSONPublicWrite")
	jsonPublicHTTPCheck(b, err)
	if len(nodes) != jsonPublicHTTPBatch {
		b.Fatalf("batch committed %d nodes, want %d", len(nodes), jsonPublicHTTPBatch)
	}
}

func jsonPublicHTTPPost(b *testing.B, client *http.Client, endpoint, body string, status int) string {
	b.Helper()
	resp, err := client.Post(endpoint, "application/json", strings.NewReader(body))
	jsonPublicHTTPCheck(b, err)
	raw, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	jsonPublicHTTPCheck(b, readErr)
	jsonPublicHTTPCheck(b, closeErr)
	var envelope struct {
		Commit string `json:"commit"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	jsonPublicHTTPCheck(b, json.Unmarshal(raw, &envelope))
	if resp.StatusCode != status || len(envelope.Errors) != 0 {
		b.Fatalf("POST %s: status=%d, body=%s", endpoint, resp.StatusCode, raw)
	}
	return envelope.Commit
}
