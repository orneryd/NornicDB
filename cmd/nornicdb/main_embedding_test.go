package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Run the real CLI in a separate process: configuration must survive runServe,
// nornicdb.Open and Server.New, not just Cobra parsing or a test-only resolver.
func TestServeEmbeddingPrecedence(t *testing.T) {
	if os.Getenv("NORNICDB_TEST_EMBEDDING_CLI") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
				main()
				os.Exit(0)
			}
		}
		os.Exit(2)
	}
	const unrelated = "databases:\n  nornic:\n    db.nornic.search.vector.warming: lazy\n"
	const yamlEmbedding = "embedding:\n  provider: ollama\n  model: yaml-model\n  url: http://127.0.0.1:11435\n  dimensions: 384\n"
	cases := []struct {
		name, yaml                  string
		env, flags                  []string
		provider, model, dimensions string
	}{
		{name: "defaults", provider: "local", model: "bge-m3", dimensions: "1024"},
		{name: "unrelated YAML preserves defaults", yaml: unrelated, provider: "local", model: "bge-m3", dimensions: "1024"},
		{name: "explicit flags with unrelated YAML", yaml: unrelated, flags: []string{"--embedding-provider=ollama", "--embedding-model=cli-model", "--embedding-dim=256"}, provider: "ollama", model: "cli-model", dimensions: "256"},
		{name: "explicit flags without YAML", flags: []string{"--embedding-provider=ollama", "--embedding-model=cli-model", "--embedding-dim=256"}, provider: "ollama", model: "cli-model", dimensions: "256"},
		{name: "YAML when flags omitted", yaml: yamlEmbedding, provider: "ollama", model: "yaml-model", dimensions: "384"},
		{name: "environment overrides YAML", yaml: yamlEmbedding, env: []string{"NORNICDB_EMBEDDING_PROVIDER=openai", "NORNICDB_EMBEDDING_MODEL=env-model", "NORNICDB_EMBEDDING_DIMENSIONS=512"}, provider: "openai", model: "env-model", dimensions: "512"},
		{name: "explicit flags override environment and YAML", yaml: yamlEmbedding, env: []string{"NORNICDB_EMBEDDING_PROVIDER=openai", "NORNICDB_EMBEDDING_MODEL=env-model", "NORNICDB_EMBEDDING_DIMENSIONS=512"}, flags: []string{"--embedding-provider=ollama", "--embedding-model=cli-model", "--embedding-dim=256"}, provider: "ollama", model: "cli-model", dimensions: "256"},
		{name: "explicit default values override YAML", yaml: yamlEmbedding, flags: []string{"--embedding-provider=local", "--embedding-model=bge-m3", "--embedding-dim=1024"}, provider: "local", model: "bge-m3", dimensions: "1024"},
		{name: "one explicit flag preserves remaining YAML", yaml: yamlEmbedding, flags: []string{"--embedding-model=cli-model"}, provider: "ollama", model: "cli-model", dimensions: "384"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			// Avoid auto-discovery: only cases with YAML pass this file explicitly.
			configPath := filepath.Join(dir, "cli-settings.yaml")
			if err := os.WriteFile(configPath, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"-test.run=^TestServeEmbeddingPrecedence$", "--", "serve", "--no-auth", "--headless", "--data-dir=" + filepath.Join(dir, "data"), "--http-port=" + fmt.Sprint(port), "--bolt-port=0", "--embedding-enabled=false", "--search-vector-enabled=false", "--search-bm25-enabled=false", "--stdio-log-max-kb=0"}
			if tc.yaml != "" {
				args = append(args, "--config="+configPath)
			}
			args = append(args, tc.flags...)
			proc := exec.Command(os.Args[0], args...)
			proc.Dir = dir
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "NORNICDB_") && !strings.HasPrefix(value, "NEO4J_") {
					proc.Env = append(proc.Env, value)
				}
			}
			// HOME must be isolated: config.FindConfigFile checks
			// ~/.nornicdb/config.yaml before the current directory, so a
			// developer's real home-directory config would otherwise leak
			// into "defaults" and other cases that don't pass --config.
			proc.Env = append(proc.Env, "HOME="+dir)
			proc.Env = append(proc.Env,
				"NORNICDB_TEST_EMBEDDING_CLI=1",
				"NORNICDB_LANGUAGE=en",
				"NORNICDB_BOLT_ENABLED=false",
				"NORNICDB_EMBEDDING_ENABLED=false",
				"NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0",
			)
			proc.Env = append(proc.Env, tc.env...)
			logPath := filepath.Join(dir, "serve.log")
			logFile, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			proc.Stdout, proc.Stderr = logFile, logFile
			if err := proc.Start(); err != nil {
				logFile.Close()
				t.Fatal(err)
			}
			procDone := make(chan error, 1)
			go func() { procDone <- proc.Wait() }()
			processExited := false
			defer func() {
				if !processExited {
					_ = proc.Process.Kill()
					select {
					case <-procDone:
					case <-time.After(5 * time.Second):
					}
				}
				_ = logFile.Close()
			}()
			client := &http.Client{Timeout: time.Second}
			deadline := time.Now().Add(30 * time.Second)
			var effective map[string]string
			for time.Now().Before(deadline) {
				select {
				case waitErr := <-procDone:
					processExited = true
					_ = logFile.Close()
					logs, _ := os.ReadFile(logPath)
					t.Fatalf("real CLI exited before exposing effective config: %v: %s", waitErr, logs)
				default:
				}
				resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/admin/databases/nornic/config", port))
				if err == nil {
					var result struct {
						Effective map[string]string `json:"effective"`
					}
					decodeErr := json.NewDecoder(resp.Body).Decode(&result)
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK && decodeErr == nil && result.Effective != nil {
						effective = result.Effective
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
			if effective == nil {
				logs, _ := os.ReadFile(logPath)
				t.Fatalf("real CLI did not expose effective config: %s", logs)
			}
			for key, want := range map[string]string{"provider": tc.provider, "model": tc.model, "dimensions": tc.dimensions} {
				if got := effective["db.nornic.embedding."+key]; got != want {
					t.Errorf("effective embedding %s = %q, want %q", key, got, want)
				}
			}
			t.Logf("real CLI effective provider=%s model=%s dimensions=%s", effective["db.nornic.embedding.provider"], effective["db.nornic.embedding.model"], effective["db.nornic.embedding.dimensions"])
		})
	}
}
