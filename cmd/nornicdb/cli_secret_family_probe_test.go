package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/replication"
)

// These regressions exercise the real CLI entry point in disposable subprocesses.
// They never inspect the caller's environment or print captured CLI output.
const cliSecretProbeEnv = "synthetic-cli-family-env-credential-314159"
const cliSecretProbeFlag = "synthetic-cli-family-flag-credential-271828"
const cliSecretProbeOverride = "synthetic-cli-family-override-credential-161803"

func TestCLISecretFamilyProcess(t *testing.T) {
	if os.Getenv("NORNICDB_TEST_SECRET_FAMILY_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	os.Exit(2)
}

func cliSecretProbeCommand(t *testing.T, dir string, extraEnv, args []string) *exec.Cmd {
	t.Helper()
	cliBinary := solRealBinary
	if cliBinary == "" {
		cliBinary = solBuild(t, "./cmd/nornicdb")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, cliBinary, args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"HOME=" + dir,
		"PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin",
		"NORNICDB_TEST_SECRET_FAMILY_PROCESS=1",
		"NORNICDB_LANGUAGE=en",
		"NORNICDB_BOLT_ENABLED=false",
		"NORNICDB_EMBEDDING_ENABLED=false",
		"NORNICDB_TELEMETRY_ENABLED=false",
		"NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0",
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	return cmd
}

func assertCLISecretAbsent(t *testing.T, surface string, output []byte) {
	t.Helper()
	for _, marker := range []string{cliSecretProbeEnv, cliSecretProbeFlag, cliSecretProbeOverride} {
		if bytes.Contains(output, []byte(marker)) {
			t.Errorf("%s disclosed a synthetic credential (captured output withheld)", surface)
			return
		}
	}
}

func TestCLISecretFamilyPublicOutput(t *testing.T) {
	for _, tc := range []struct {
		name      string
		env       []string
		args      []string
		wantError bool
	}{
		{"root-help-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"--help"}, false},
		{"completion-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"completion", "bash"}, false},
		{"serve-help-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"serve", "--help"}, false},
		{"serve-help-malformed-url", []string{"NORNICDB_EMBEDDING_API_URL=http://probe:" + cliSecretProbeFlag + `"@127.0.0.1:1`}, []string{"serve", "--help"}, false},
		{"help-subcommand-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"help", "serve"}, false},
		{"parse-error-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"serve", "--http-port=invalid"}, true},
		{"unknown-flag-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"serve", "--not-a-real-flag"}, true},
		{"startup-error-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv}, []string{"serve", "--config=does-not-exist.yaml"}, true},
		{"incident-security-validation-env", []string{"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv, "NORNICDB_EMBEDDING_PROVIDER=voyage", "NORNICDB_EMBEDDING_ENABLED=true"}, []string{"serve", "--address=0.0.0.0"}, true},
		{"serve-help-flag", nil, []string{"serve", "--embedding-key=" + cliSecretProbeFlag, "--admin-password=" + cliSecretProbeFlag, "--help"}, false},
		{"parse-error-flag", nil, []string{"serve", "--embedding-key=" + cliSecretProbeFlag, "--admin-password=" + cliSecretProbeFlag, "--http-port=invalid"}, true},
		{"startup-error-flag", nil, []string{"serve", "--embedding-key=" + cliSecretProbeFlag, "--admin-password=" + cliSecretProbeFlag, "--config=does-not-exist.yaml"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := cliSecretProbeCommand(t, t.TempDir(), tc.env, tc.args)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected CLI exit state (output withheld)")
			}
			if len(output) == 0 {
				t.Fatal("CLI produced no output")
			}
			assertCLISecretAbsent(t, tc.name, output)
			if !tc.wantError && tc.name != "completion-env" && !bytes.Contains(output, []byte("Usage:")) {
				t.Error("CLI help was not reached")
			}
		})
	}
	t.Run("startup-url-banner", func(t *testing.T) {
		dir := t.TempDir()
		blocked := filepath.Join(dir, "blocked")
		if err := os.WriteFile(blocked, []byte("not a directory"), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := cliSecretProbeCommand(t, dir, nil, []string{"serve", "--no-auth", "--headless", "--data-dir=" + blocked, "--embedding-provider=ollama", "--embedding-enabled=true", "--embedding-url=http://user:" + cliSecretProbeFlag + "@127.0.0.1:1", "--stdio-log-max-kb=0"})
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("blocked data directory did not fail startup")
		}
		if !bytes.Contains(output, []byte("Embedding URL:")) {
			t.Fatal("startup banner was not reached (output withheld)")
		}
		assertCLISecretAbsent(t, "startup URL banner", output)
	})
	t.Run("yaml-error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "probe.yaml")
		if err := os.WriteFile(path, []byte("embedding:\n  api_key: \""+cliSecretProbeFlag+"\"\n  enabled: false\nserver:\n  bolt_port: invalid\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := cliSecretProbeCommand(t, dir, nil, []string{"serve", "--config=" + path})
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("invalid YAML field did not fail configuration")
		}
		assertCLISecretAbsent(t, "YAML decode error", output)
	})
}

func TestCLISecretFamilyConfigAndDebug(t *testing.T) {
	dir := t.TempDir()
	port := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		value := fmt.Sprint(ln.Addr().(*net.TCPAddr).Port)
		ln.Close()
		return value
	}
	httpPort, debugPort := port(), port()
	cmd := cliSecretProbeCommand(t, dir, []string{
		"NORNICDB_EMBEDDING_API_KEY=" + cliSecretProbeEnv,
		"NORNICDB_HEIMDALL_API_KEY=" + cliSecretProbeEnv,
		"NORNICDB_HEIMDALL_ENABLED=true",
		"NORNICDB_HEIMDALL_PROVIDER=openai",
		"NORNICDB_HEIMDALL_API_URL=http://127.0.0.1:1",
		"NORNICDB_AUTH_PROVIDER=oauth",
		"NORNICDB_OAUTH_ISSUER=http://127.0.0.1:1",
		"NORNICDB_OAUTH_CLIENT_ID=probe",
		"NORNICDB_OAUTH_CLIENT_SECRET=" + cliSecretProbeEnv,
		"NORNICDB_SEARCH_RERANK_API_KEY=" + cliSecretProbeEnv,
		"NORNICDB_AUTH_JWT_SECRET=" + cliSecretProbeEnv,
		"NORNICDB_PPROF_ENABLED=true",
		"NORNICDB_PPROF_LISTEN=127.0.0.1:" + debugPort,
	}, []string{"serve", "--no-auth", "--data-dir=" + filepath.Join(dir, "data"), "--http-port=" + httpPort, "--bolt-port=0", "--embedding-enabled=false", "--search-vector-enabled=false", "--search-bm25-enabled=false", "--embedding-key=" + cliSecretProbeFlag, "--admin-password=" + cliSecretProbeFlag, "--stdio-log-max-kb=0"})
	logPath := filepath.Join(dir, "probe.log")
	file, err := os.Create(logPath)
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
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-done
		_ = file.Close()
		output, err := os.ReadFile(logPath)
		if err != nil {
			t.Error(err)
			return
		}
		assertCLISecretAbsent(t, "successful startup logs", output)
	})
	client := &http.Client{Timeout: time.Second}
	base := "http://127.0.0.1:" + httpPort
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/admin/databases/nornic/config")
		if err == nil {
			output, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				assertCLISecretAbsent(t, "GET effective config", output)
				if !bytes.Contains(output, []byte("<REDACTED>")) && !bytes.Contains(output, []byte("\\u003cREDACTED\\u003e")) {
					t.Error("config did not show a redaction placeholder")
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("disposable CLI server did not become ready (output withheld)")
		}
		time.Sleep(100 * time.Millisecond)
	}
	request := func(t *testing.T, method, url, body string) []byte {
		t.Helper()
		req, err := http.NewRequest(method, url, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("probe request failed (details withheld)")
		}
		defer resp.Body.Close()
		output, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatal("probe request was not successful (output withheld)")
		}
		return output
	}
	for _, path := range []string{"/admin/config", "/auth/config", "/status", "/nornicdb/embed/status", "/api/bifrost/status", "/admin/databases/config/keys"} {
		t.Run("safe-route"+path, func(t *testing.T) {
			resp, err := client.Get(base + path)
			if err != nil {
				t.Fatal("safe-route request failed (details withheld)")
			}
			defer resp.Body.Close()
			output, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if path != "/nornicdb/embed/status" && path != "/auth/config" && resp.StatusCode != http.StatusOK {
				t.Errorf("enabled endpoint status %d", resp.StatusCode)
			}
			assertCLISecretAbsent(t, path, output)
		})
	}
	t.Run("put-config", func(t *testing.T) {
		body := `{"overrides":{"db.nornic.embedding.api.key":"` + cliSecretProbeOverride + `","db.nornic.search.rerank.api.key":"` + cliSecretProbeOverride + `"}}`
		assertCLISecretAbsent(t, "PUT config", request(t, http.MethodPut, base+"/admin/databases/nornic/config", body))
	})
	t.Run("put-url-config", func(t *testing.T) {
		body := `{"overrides":{"db.nornic.embedding.api.key":"` + cliSecretProbeOverride + `","db.nornic.embedding.api.url":"http://probe:` + cliSecretProbeFlag + `@127.0.0.1:1?api_key=` + cliSecretProbeEnv + `","db.nornic.search.rerank.api.url":"http://probe:` + cliSecretProbeFlag + `@127.0.0.1:1?api_key=` + cliSecretProbeEnv + `"}}`
		assertCLISecretAbsent(t, "PUT URL config", request(t, http.MethodPut, base+"/admin/databases/nornic/config", body))
		assertCLISecretAbsent(t, "GET URL config", request(t, http.MethodGet, base+"/admin/databases/nornic/config", ""))
	})
	for _, query := range []string{"SHOW SETTINGS", "CALL dbms.listConfig()"} {
		t.Run(query, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"statements": []map[string]string{{"statement": query}}})
			output := request(t, http.MethodPost, base+"/db/nornic/tx/commit", string(body))
			if bytes.Contains(output, []byte(`"errors":[{`)) {
				t.Fatal("config query did not execute (output withheld)")
			}
			assertCLISecretAbsent(t, query, output)
		})
	}
	t.Run("remote-metadata", func(t *testing.T) {
		query := func(statement string) []byte {
			body, _ := json.Marshal(map[string]any{"statements": []map[string]string{{"statement": statement}}})
			output := request(t, http.MethodPost, base+"/db/system/tx/commit", string(body))
			if bytes.Contains(output, []byte(`"errors":[{`)) {
				t.Fatal("remote metadata query did not execute (output withheld)")
			}
			return output
		}
		assertCLISecretAbsent(t, "CREATE remote metadata", query("CREATE COMPOSITE DATABASE family_probe ALIAS remote FOR DATABASE nornic AT 'http://probe:"+cliSecretProbeFlag+"@127.0.0.1:1' USER 'probe' PASSWORD '"+cliSecretProbeEnv+"' TYPE remote ACCESS read"))
		assertCLISecretAbsent(t, "SHOW CONSTITUENTS", query("SHOW CONSTITUENTS FOR COMPOSITE DATABASE family_probe"))
		assertCLISecretAbsent(t, "HTTP remote metadata", request(t, http.MethodGet, base+"/db/family_probe", ""))
	})
	t.Run("debug-cmdline", func(t *testing.T) {
		assertCLISecretAbsent(t, "pprof cmdline", request(t, http.MethodGet, "http://127.0.0.1:"+debugPort+"/debug/pprof/cmdline", ""))
	})
}

func TestCLISecretFamilyOwners(t *testing.T) {
	for _, name := range []string{"embedding-key", "admin-password", "EmbeddingAPIKey", "HeimdallAPIKey", "SearchRerankAPIKey", "db.nornic.embedding.api.key", "api_key", "JWTSecret", "EncryptionMasterKey", "EncryptionAWSAccessKey", "EncryptionAWSSecretKey", "EncryptionAWSSessionToken", "EncryptionAzureClientSecret", "private_key", "BoltTLSKey", "HTTPTLSKey", "QdrantGRPCTLSKey", "ReplicationSecret", "client-secret", "credentials"} {
		t.Run(name, func(t *testing.T) {
			if !observability.IsSensitiveFieldName(name) {
				t.Error("canonical field classifier did not recognize this secret-bearing field")
			}
			path := filepath.Join(t.TempDir(), "structured.json")
			logger, writer, err := observability.NewLogger(observability.LoggerConfig{Output: path}, observability.ServiceInfo{})
			if err != nil {
				t.Fatal(err)
			}
			logger.Info("secret-family probe", name, cliSecretProbeEnv)
			if closer, ok := writer.(io.Closer); ok {
				_ = closer.Close()
			}
			output, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			assertCLISecretAbsent(t, "structured attribute", output)
		})
	}
	t.Run("safe-config-string", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Memory.EmbeddingAPIKey, cfg.Features.HeimdallAPIKey, cfg.Features.SearchRerankAPIKey = cliSecretProbeEnv, cliSecretProbeEnv, cliSecretProbeEnv
		cfg.Auth.InitialPassword, cfg.Auth.JWTSecret = cliSecretProbeEnv, cliSecretProbeEnv
		cfg.Database.EncryptionMasterKey, cfg.Database.EncryptionPassword = cliSecretProbeEnv, cliSecretProbeEnv
		cfg.Server.BoltTLSKey, cfg.Server.HTTPTLSKey = cliSecretProbeEnv, cliSecretProbeEnv
		assertCLISecretAbsent(t, "Config.String", []byte(fmt.Sprintf("%s %v %+v", cfg, cfg, cfg)))
		assertCLISecretAbsent(t, "ReplicationConfig.String", []byte((&replication.Config{ReplicationSecret: cliSecretProbeEnv}).String()))
	})
	t.Run("remote-error", func(t *testing.T) {
		executor := fabric.NewRemoteFragmentExecutor()
		defer executor.Close()
		_, err := executor.Execute(context.Background(), &fabric.LocationRemote{DBName: "remote", URI: "ftp://user:" + cliSecretProbeFlag + "@127.0.0.1", AuthMode: "user_password", User: "probe", Password: cliSecretProbeEnv}, "RETURN 1", nil, "")
		if err == nil {
			t.Fatal("unsupported remote URI did not fail")
		}
		assertCLISecretAbsent(t, "Fabric/remote error", []byte(err.Error()))
	})
}
