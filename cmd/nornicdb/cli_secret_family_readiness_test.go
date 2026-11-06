package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// cliProbeOutcome is the readiness classification for one disposable CLI server.
// It carries sanitized facts only — an attempt count, elapsed time, the last HTTP
// status code, the transport error class and the child's exit state — so a failed
// readiness wait is diagnosable without ever echoing captured CLI output or
// configuration values.
type cliProbeOutcome struct {
	Ready      bool
	Attempts   int
	Elapsed    time.Duration
	LastStatus int
	LastClass  string
	Exited     bool
	ExitCode   int
}

// describe renders the sanitized classification. Every field is a number or a
// fixed class name; captured output is never part of it.
func (o cliProbeOutcome) describe() string {
	process := "running"
	switch {
	case o.Exited && o.ExitCode >= 0:
		process = fmt.Sprintf("exited code=%d", o.ExitCode)
	case o.Exited:
		process = "exited code=signal"
	}
	return fmt.Sprintf("process=%s elapsed=%s attempts=%d last_status=%d last_transport=%s",
		process, o.Elapsed.Round(time.Millisecond), o.Attempts, o.LastStatus, o.LastClass)
}

// cliTransportClass names a transport failure as a fixed class so the readiness
// report never echoes addresses, request targets or captured output.
func cliTransportClass(err error) string {
	if err == nil {
		return "none"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "transport_timeout"
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.EPIPE):
		return "connection_closed"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "empty_response"
	}
	return "transport_error"
}

// cliProbeServer polls target until it answers wantStatus and returns that body
// with a ready outcome. The child's completion signal and process state make a
// dead server fail immediately with its exit code instead of consuming the whole
// budget, and every other attempt records why the probe did not succeed.
func cliProbeServer(t *testing.T, client *http.Client, cmd *exec.Cmd, exited <-chan struct{}, target string, wantStatus int, budget time.Duration) ([]byte, cliProbeOutcome) {
	t.Helper()
	outcome := cliProbeOutcome{LastClass: "no_response", ExitCode: -1}
	started := time.Now()
	for {
		select {
		case <-exited:
			outcome.Exited = true
			if cmd != nil && cmd.ProcessState != nil {
				outcome.ExitCode = cmd.ProcessState.ExitCode()
			}
			outcome.Elapsed = time.Since(started)
			return nil, outcome
		default:
		}
		outcome.Attempts++
		resp, err := client.Get(target)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			outcome.LastStatus = resp.StatusCode
			switch {
			case readErr != nil:
				outcome.LastClass = "unreadable_response"
			case resp.StatusCode == wantStatus:
				outcome.Ready = true
				outcome.Elapsed = time.Since(started)
				return body, outcome
			default:
				outcome.LastClass = "unexpected_status"
			}
		} else {
			outcome.LastClass = cliTransportClass(err)
		}
		if time.Since(started) > budget {
			outcome.Elapsed = time.Since(started)
			return nil, outcome
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestCLISecretFamilyReadinessDiagnostics pins the readiness contract for the
// live CLI scenario: a disposable server that dies before its first successful
// probe is reported by its exit state instead of an anonymous budget expiry, and
// the report never carries captured CLI output.
func TestCLISecretFamilyReadinessDiagnostics(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Close() }()
	occupied := fmt.Sprint(blocker.Addr().(*net.TCPAddr).Port)

	dir := t.TempDir()
	cmd := cliSecretProbeCommand(t, dir, nil, []string{
		"serve",
		"--no-auth",
		"--headless",
		"--data-dir=" + filepath.Join(dir, "data"),
		"--http-port=" + occupied,
		"--bolt-port=0",
		"--embedding-enabled=false",
		"--search-vector-enabled=false",
		"--search-bm25-enabled=false",
		"--embedding-key=" + cliSecretProbeFlag,
		"--admin-password=" + cliSecretProbeFlag,
		"--stdio-log-max-kb=0",
	})
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
	exited := make(chan struct{})
	go func() {
		done <- cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-done
		_ = file.Close()
		output, readErr := os.ReadFile(logPath)
		if readErr != nil {
			t.Error(readErr)
			return
		}
		assertCLISecretAbsent(t, "blocked-port startup logs", output)
	})

	const budget = 10 * time.Second
	target := "http://127.0.0.1:" + occupied + "/admin/databases/nornic/config"
	body, outcome := cliProbeServer(t, &http.Client{Timeout: time.Second}, cmd, exited, target, http.StatusOK, budget)
	report := outcome.describe()
	t.Logf("readiness classification: %s", report)
	if outcome.Ready {
		t.Fatal("blocked port unexpectedly answered the readiness probe")
	}
	if body != nil {
		t.Error("unready probe returned a body")
	}
	if !outcome.Exited {
		t.Error("readiness wait did not report the exited CLI process")
	}
	if outcome.ExitCode == 0 {
		t.Errorf("readiness wait did not classify a failed startup exit code (got %d)", outcome.ExitCode)
	}
	if !strings.Contains(report, "exited") {
		t.Errorf("readiness report omitted the process exit state: %s", report)
	}
	if outcome.Elapsed >= budget {
		t.Errorf("exited CLI process consumed the whole readiness budget: %s", report)
	}
	for _, marker := range []string{cliSecretProbeEnv, cliSecretProbeFlag, cliSecretProbeOverride} {
		if strings.Contains(report, marker) {
			t.Errorf("readiness report disclosed a synthetic credential (report withheld)")
			return
		}
	}
}
