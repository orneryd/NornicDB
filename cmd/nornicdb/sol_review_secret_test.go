package main

import (
	"bytes"
	"context"
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
)

const solSecret = "synthetic-sol-review-credential-62487319"

var solRealBinary string

func solBuild(t *testing.T, target string) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), filepath.Base(target))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-tags=noui,nolocalllm", "-o", binary, target)
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s failed: %v\n%s", target, err, output)
	}
	return binary
}

func solEnv(dir string, extra ...string) []string {
	env := []string{"HOME=" + dir, "PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "NORNICDB_LANGUAGE=en", "NORNICDB_BOLT_ENABLED=false", "NORNICDB_EMBEDDING_ENABLED=false", "NORNICDB_TELEMETRY_ENABLED=false", "NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0"}
	return append(env, extra...)
}

func solAbsent(t *testing.T, sink string, data []byte) {
	t.Helper()
	if bytes.Contains(data, []byte(solSecret)) {
		t.Errorf("%s disclosed synthetic credential; output withheld", sink)
	}
}

func solPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return fmt.Sprint(ln.Addr().(*net.TCPAddr).Port)
}

func solStart(t *testing.T, bin string, env []string, args ...string) {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = dir, solEnv(dir, env...), f, f
	if err := cmd.Start(); err != nil {
		cancel()
		f.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		cancel()
		f.Close()
		data, _ := os.ReadFile(f.Name())
		solAbsent(t, "process logs", data)
	})
}

func solGet(t *testing.T, target string) []byte {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(target)
		if err == nil {
			data, e := io.ReadAll(resp.Body)
			resp.Body.Close()
			if e == nil && resp.StatusCode == 200 {
				return data
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("disposable server endpoint not ready; details withheld")
	return nil
}

func TestSolReviewRealBinaries(t *testing.T) {
	solRealBinary = solBuild(t, "./cmd/nornicdb")
	defer func() { solRealBinary = "" }()
	// Same public path matrix, now through a standalone built binary.
	t.Run("registered-cli-paths", TestCLISecretFamilyPublicOutput)
	t.Run("live-config-and-pprof", TestCLISecretFamilyConfigAndDebug)
	t.Run("actual-credential-precedence", TestCLISecretCredentialPrecedence)
	t.Run("url-auth-query-alias", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(solRealBinary, "serve", "--help")
		cmd.Env = solEnv(dir, "NORNICDB_EMBEDDING_API_URL=http://127.0.0.1:1/v1?model=probe&auth="+solSecret)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatal("help failed")
		}
		solAbsent(t, "serve help URL auth query", output)
	})
	t.Run("bolt-oauth-discovery", func(t *testing.T) {
		hp, bp := solPort(t), solPort(t)
		dir := t.TempDir()
		solStart(t, solRealBinary, []string{"NORNICDB_BOLT_ENABLED=true", "NORNICDB_AUTH_PROVIDER=oauth", "NORNICDB_OAUTH_CLIENT_ID=public-client-id", "NORNICDB_OAUTH_CLIENT_SECRET=synthetic-oauth-client-secret-0981726354", "NORNICDB_OAUTH_ISSUER=http://probe:" + solSecret + "@127.0.0.1:1/issuer", "NORNICDB_OAUTH_CALLBACK_URL=http://127.0.0.1/callback?api_key=" + solSecret}, "serve", "--no-auth", "--headless", "--http-port="+hp, "--bolt-port="+bp, "--data-dir="+filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0")
		data := solGet(t, "http://127.0.0.1:"+bp+"/")
		if !bytes.Contains(data, []byte("public-client-id")) {
			t.Error("public client ID lost")
		}
		solAbsent(t, "Bolt HTTP OAuth discovery", data)
	})
	t.Run("eval-url-banner", func(t *testing.T) {
		bin := solBuild(t, "./cmd/eval")
		dir := t.TempDir()
		cmd := exec.Command(bin, "-url=http://probe:"+solSecret+"@127.0.0.1:1/v1")
		cmd.Env, cmd.Dir = solEnv(dir), dir
		data, err := cmd.CombinedOutput()
		if err == nil {
			t.Error("unreachable eval did not fail")
		}
		if !bytes.Contains(data, []byte("Connecting to NornicDB")) {
			t.Fatal("banner not reached")
		}
		solAbsent(t, "eval startup banner", data)
	})
	t.Run("oauth-provider-discovery", func(t *testing.T) {
		bin := solBuild(t, "./cmd/oauth-provider")
		port := solPort(t)
		solStart(t, bin, nil, "-port="+port, "-issuer=http://probe:"+solSecret+"@127.0.0.1:"+port, "-client-secret=synthetic-provider-client-credential-8271649")
		data := solGet(t, "http://127.0.0.1:"+port+"/.well-known/oauth-authorization-server")
		solAbsent(t, "oauth-provider discovery", data)
	})
	t.Run("admin-help", func(t *testing.T) {
		bin := solBuild(t, "./cmd/nornicdb-admin")
		dir := t.TempDir()
		cmd := exec.Command(bin, "--help")
		cmd.Env = solEnv(dir, "NORNICDB_EMBEDDING_API_KEY="+solSecret)
		cmd.Dir = dir
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatal(err)
		}
		solAbsent(t, "admin help", data)
		if !strings.Contains(string(data), "./data") {
			t.Error("ordinary path default lost")
		}
	})
}

func TestSolReviewOAuthPprof(t *testing.T) {
	bin := solBuild(t, "./cmd/oauth-provider")
	port := solPort(t)
	solStart(t, bin, nil, "-port="+port, "-client-secret="+solSecret)
	_ = solGet(t, "http://127.0.0.1:"+port+"/health")
	response, err := (&http.Client{Timeout: time.Second}).Get("http://127.0.0.1:" + port + "/debug/pprof/cmdline")
	if err != nil {
		t.Fatal("OAuth HTTP request failed; details withheld")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal("OAuth HTTP response read failed")
	}
	if response.StatusCode != http.StatusNotFound {
		t.Error("OAuth listener unexpectedly serves profiling")
	}
	solAbsent(t, "oauth-provider default-mux pprof cmdline", data)
}

// Check every command's production dependency closure, not just one import form
// or the packages imported by this test binary.
func TestSolReviewNoDefaultPprofImports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-deps", "-tags=noui,nolocalllm", "./cmd/...")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatal("binary dependency inventory failed; output withheld")
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == "net/http/pprof" {
			t.Error("a binary imports a package that registers the raw default-mux profiler")
		}
	}
}
