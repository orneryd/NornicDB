package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLISecretCredentialPrecedence(t *testing.T) {
	for _, source := range []string{"env", "yaml", "flag", "env-over-yaml", "flag-over-env-and-yaml"} {
		t.Run(source, func(t *testing.T) {
			observed := make(chan string, 8)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case observed <- r.Header.Get("Authorization"):
				default:
				}
				var body struct {
					Input json.RawMessage `json:"input"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				inputs := []string{string(body.Input)}
				_ = json.Unmarshal(body.Input, &inputs)
				data := make([]map[string]any, len(inputs))
				for i := range data {
					data[i] = map[string]any{"index": i, "embedding": []float32{1, 0, 0}}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer upstream.Close()
			dir := t.TempDir()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := ln.Addr().(*net.TCPAddr).Port
			ln.Close()
			args := []string{"serve", "--no-auth", "--headless", "--data-dir=" + filepath.Join(dir, "data"), fmt.Sprintf("--http-port=%d", port), "--bolt-port=0", "--embedding-enabled=true", "--embedding-provider=openai", "--embedding-url=" + upstream.URL, "--embedding-dim=3", "--embedding-model=probe", "--search-vector-enabled=false", "--search-bm25-enabled=false", "--stdio-log-max-kb=0"}
			env := []string{}
			want := cliSecretProbeEnv
			if strings.Contains(source, "yaml") || source == "yaml" {
				path := filepath.Join(dir, "probe.yaml")
				if err := os.WriteFile(path, []byte("embedding:\n  api_key: "+cliSecretProbeOverride+"\nembedding_worker:\n  batch_delay: 10ms\n  trigger_debounce: 10ms\n"), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--config="+path)
				want = cliSecretProbeOverride
			}
			if strings.Contains(source, "env") {
				env = append(env, "NORNICDB_EMBEDDING_API_KEY="+cliSecretProbeEnv)
				want = cliSecretProbeEnv
			}
			if strings.Contains(source, "flag") {
				args = append(args, "--embedding-key="+cliSecretProbeFlag)
				want = cliSecretProbeFlag
			}
			cmd := cliSecretProbeCommand(t, dir, env, args)
			file, err := os.Create(filepath.Join(dir, "output.log"))
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stdout, cmd.Stderr = file, file
			if err := cmd.Start(); err != nil {
				file.Close()
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			defer func() {
				_ = cmd.Process.Kill()
				<-done
				file.Close()
				output, _ := os.ReadFile(file.Name())
				assertCLISecretAbsent(t, "credential-consumption startup", output)
			}()
			client := &http.Client{Timeout: 15 * time.Second}
			base := fmt.Sprintf("http://127.0.0.1:%d", port)
			deadline := time.Now().Add(25 * time.Second)
			for {
				resp, err := client.Get(base + "/status")
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode == 200 {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("CLI did not become ready (output withheld)")
				}
				time.Sleep(50 * time.Millisecond)
			}
			body := `{"statements":[{"statement":"CREATE (:Document {content: 'synthetic embedding input'})"}]}`
			resp, err := client.Post(base+"/db/nornic/tx/commit", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal("create request failed (details withheld)")
			}
			output, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || bytes.Contains(output, []byte(`"errors":[{`)) {
				t.Fatal("create did not execute (output withheld)")
			}
			assertCLISecretAbsent(t, "create output", output)
			resp, err = client.Post(base+"/nornicdb/embed/trigger", "application/json", nil)
			if err != nil {
				t.Fatal("embedding trigger failed (details withheld)")
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 && resp.StatusCode != 202 {
				t.Fatal("embedding trigger was rejected")
			}
			select {
			case got := <-observed:
				if got != "Bearer "+want {
					t.Error("runtime credential or precedence changed (values withheld)")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("real CLI did not reach the loopback embedding provider")
			}
		})
	}
}
