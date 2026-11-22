package main

// Report gap X02 (v1.5.2 verification campaign, test-side only): the
// container start, run for real. The lab has no route that builds a
// container image, so this test does what the image does, with the same
// pieces: it builds the server binary from this tree the way the headless
// image's builder stage does (CGO_ENABLED=1, tags noui,nolocalllm,
// ./cmd/nornicdb), takes the image's ENV block from docker/Dockerfile.amd64-cpu
// (parsed, not copied), adds the quick-start authentication settings, and
// starts the real docker/entrypoint.sh with /bin/sh (NORNICDB_BIN pointing at
// the built binary; ports moved to free ones and the data directory to a
// temporary one, everything else as the image sets it). It then asks the
// running server, over HTTP, for a Cypher statement without credentials and
// with the quick-start credentials.
// Cases: quick-start (image ENV + NORNICDB_AUTH); control (the same with
// NORNICDB_NO_AUTH=false).
// Diagnostic: logs GAPX02 lines; fails only on harness errors.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func gapX02ImageEnv(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "docker", "Dockerfile.amd64-cpu"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	headless := "false"
	if m := regexp.MustCompile(`(?m)^ARG HEADLESS=(\S+)`).FindStringSubmatch(text); m != nil {
		headless = m[1]
	}
	start := strings.Index(text, "\nENV NORNICDB_")
	if start < 0 {
		t.Fatal("no ENV block")
	}
	env := map[string]string{}
	pair := regexp.MustCompile(`^\s*(?:ENV\s+)?(NORNICDB_[A-Z_]+)=(\S*)`)
	for _, line := range strings.Split(text[start+1:], "\n") {
		match := pair.FindStringSubmatch(line)
		if match == nil {
			break
		}
		v := strings.TrimSuffix(match[2], "\\")
		v = strings.ReplaceAll(v, "${HEADLESS}", headless)
		env[match[1]] = v
		if !strings.HasSuffix(strings.TrimSpace(line), "\\") {
			break
		}
	}
	return env
}

func gapX02FreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func gapX02Build(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "app", "nornicdb")
	cmd := exec.Command("go", "build", "-o", out, ".")
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GOFLAGS=") && !strings.HasPrefix(kv, "CGO_ENABLED=") {
			env = append(env, kv)
		}
	}
	cmd.Env = append(env, "CGO_ENABLED=1", "GOFLAGS=-tags=noui,nolocalllm")
	start := time.Now()
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
	t.Logf("GAPX02 built ./cmd/nornicdb (CGO_ENABLED=1, tags noui,nolocalllm) in %v", time.Since(start).Round(time.Second))
	return out
}

func gapX02Post(port int, user, pass string) (int, string) {
	body := []byte(`{"statements":[{"statement":"RETURN 1 AS one"}]}`)
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/db/nornic/tx/commit", port), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	s := b.String()
	if len(s) > 200 {
		s = s[:200]
	}
	return resp.StatusCode, s
}

func gapX02Run(t *testing.T, bin, name string, env map[string]string) {
	httpPort, boltPort, httpsPort, grpcPort := gapX02FreePort(t), gapX02FreePort(t), gapX02FreePort(t), gapX02FreePort(t)
	run := map[string]string{}
	for k, v := range env {
		run[k] = v
	}
	run["NORNICDB_DATA_DIR"] = filepath.Join(t.TempDir(), "data")
	run["NORNICDB_HTTP_PORT"] = fmt.Sprint(httpPort)
	run["NORNICDB_BOLT_PORT"] = fmt.Sprint(boltPort)
	run["NORNICDB_HTTPS_PORT"] = fmt.Sprint(httpsPort)
	run["NORNICDB_QDRANT_GRPC_LISTEN_ADDR"] = fmt.Sprintf("127.0.0.1:%d", grpcPort)
	run["NORNICDB_BIN"] = bin
	script, err := filepath.Abs(filepath.Join("..", "..", "docker", "entrypoint.sh"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", script)
	cmd.Dir = filepath.Dir(bin)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir()}
	var keys []string
	for k, v := range run {
		cmd.Env = append(cmd.Env, k+"="+v)
		keys = append(keys, k+"="+v)
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	ready := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/health", httpPort))
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !ready {
		var errLines []string
		for _, line := range strings.Split(output.String(), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "Error:") {
				errLines = append(errLines, strings.TrimSpace(line))
			}
		}
		t.Logf("GAPX02 case=%s env NO_AUTH=%q AUTH=%q ADDRESS=%q CORS_ORIGINS=%q", name, run["NORNICDB_NO_AUTH"], run["NORNICDB_AUTH"], run["NORNICDB_ADDRESS"], run["NORNICDB_CORS_ORIGINS"])
		t.Logf("GAPX02 case=%s DID NOT START within 90s; error lines: %q", name, errLines)
		return
	}
	argv := "unknown"
	if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", cmd.Process.Pid)); err == nil {
		argv = strings.ReplaceAll(strings.TrimRight(string(b), "\x00"), "\x00", " ")
	}
	codeAnon, bodyAnon := gapX02Post(httpPort, "", "")
	user, pass := "", ""
	if a := run["NORNICDB_AUTH"]; a != "" {
		if i := strings.IndexByte(a, ':'); i > 0 {
			user, pass = a[:i], a[i+1:]
		}
	}
	codeAuth, bodyAuth := gapX02Post(httpPort, user, pass)
	codeWrong, _ := gapX02Post(httpPort, "admin", "wrong-password-gap-x02")
	out := output.String()
	var notable []string
	for _, line := range strings.Split(out, "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, "auth") || strings.Contains(l, "security") {
			if len(line) > 220 {
				line = line[:220]
			}
			notable = append(notable, strings.TrimSpace(line))
		}
	}
	if len(notable) > 8 {
		notable = notable[:8]
	}
	t.Logf("GAPX02 case=%s env NO_AUTH=%q AUTH=%q ADDRESS=%q CORS_ORIGINS=%q", name, run["NORNICDB_NO_AUTH"], run["NORNICDB_AUTH"], run["NORNICDB_ADDRESS"], run["NORNICDB_CORS_ORIGINS"])
	t.Logf("GAPX02 case=%s process argv (/proc cmdline after the entrypoint exec): %s", name, argv)
	t.Logf("GAPX02 case=%s POST /db/nornic/tx/commit RETURN 1: no credentials -> %d %q; quick-start credentials -> %d %q; wrong password -> %d", name, codeAnon, bodyAnon, codeAuth, bodyAuth, codeWrong)
	t.Logf("GAPX02 case=%s startup lines mentioning auth/security: %q", name, notable)
}

func TestGapX02ContainerStartAuth(t *testing.T) {
	if b, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "PRETTY_NAME=") {
				t.Logf("GAPX02 runner OS %s (image runtime base: debian:bookworm-slim)", l)
			}
		}
	}
	image := gapX02ImageEnv(t)
	t.Logf("GAPX02 image ENV from docker/Dockerfile.amd64-cpu: %v", image)
	bin := gapX02Build(t)
	quick := map[string]string{
		"NORNICDB_AUTH":            "admin:gap-x02-long-password-1",
		"NORNICDB_AUTH_JWT_SECRET": "gap-x02-jwt-secret-at-least-32-characters",
	}
	merged := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range image {
			m[k] = v
		}
		for k, v := range quick {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	gapX02Run(t, bin, "quick-start", merged(nil))
	gapX02Run(t, bin, "quick-start-loopback", merged(map[string]string{"NORNICDB_ADDRESS": "127.0.0.1"}))
	gapX02Run(t, bin, "control-NO_AUTH-false", merged(map[string]string{"NORNICDB_NO_AUTH": "false"}))
	gapX02Run(t, bin, "control-NO_AUTH-false-cors-origin", merged(map[string]string{"NORNICDB_NO_AUTH": "false", "NORNICDB_CORS_ORIGINS": "http://localhost:3000"}))
	gapX02Run(t, bin, "control-NO_AUTH-false-loopback", merged(map[string]string{"NORNICDB_NO_AUTH": "false", "NORNICDB_ADDRESS": "127.0.0.1"}))
}
