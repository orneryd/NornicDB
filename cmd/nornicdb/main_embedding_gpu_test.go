//go:build localllm

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServeEmbeddingGPULayersReachModelLoader(t *testing.T) {
	for _, tc := range []struct {
		name       string
		flags, env []string
		want       int
	}{
		{name: "automatic default", want: -1},
		{name: "explicit layers", flags: []string{"--embedding-gpu-layers=4"}, want: 4},
		{name: "CPU only overrides environment", flags: []string{"--embedding-gpu-layers=0"}, env: []string{"NORNICDB_EMBEDDING_GPU_LAYERS=6"}, want: 0},
		{name: "omitted flag preserves environment", env: []string{"NORNICDB_EMBEDDING_GPU_LAYERS=6"}, want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// An invalid local file reaches LoadModel without loading a real model.
			// The constructor prints the actual localllm.Options immediately before
			// passing them to LoadModel; wait for its returned error as well.
			if err := os.WriteFile(filepath.Join(dir, "gpu-options.gguf"), []byte("invalid GGUF fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"-test.run=^TestServeEmbeddingPrecedence$", "--", "serve", "--no-auth", "--headless", "--data-dir=" + filepath.Join(dir, "data"), "--http-port=0", "--bolt-port=0", "--embedding-enabled=true", "--embedding-provider=local", "--embedding-model=gpu-options", "--search-vector-enabled=false", "--search-bm25-enabled=false", "--stdio-log-max-kb=0"}
			args = append(args, tc.flags...)
			proc := exec.Command(os.Args[0], args...)
			proc.Dir = dir
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "NORNICDB_") && !strings.HasPrefix(value, "NEO4J_") {
					proc.Env = append(proc.Env, value)
				}
			}
			proc.Env = append(proc.Env, "NORNICDB_TEST_EMBEDDING_CLI=1", "NORNICDB_LANGUAGE=en", "NORNICDB_MODELS_DIR="+dir, "NORNICDB_BOLT_ENABLED=false", "NORNICDB_HTTP_PORT=0")
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
			defer func() { _ = proc.Process.Kill(); _ = proc.Wait(); _ = logFile.Close() }()
			deadline := time.Now().Add(30 * time.Second)
			var logs string
			for time.Now().Before(deadline) {
				content, _ := os.ReadFile(logPath)
				logs = string(content)
				if strings.Contains(logs, "failed to load model ") {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !strings.Contains(logs, "failed to load model ") {
				t.Fatalf("model-loader return not observed: %s", logs)
			}
			want := fmt.Sprintf("GPU layers: %d (-1 = auto/all)", tc.want)
			if !strings.Contains(logs, want) {
				t.Fatalf("missing constructor option %q: %s", want, logs)
			}
			t.Logf("real CLI -> server -> NewLocalGGUF -> LoadModel: %s; invalid fixture rejected", want)
		})
	}
}
