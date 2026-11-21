package main

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-019: TestCompositeDDLPublicParserParity/antlr/bolt_tx failed under -race
// with "bolt: failed to listen on 127.0.0.1:40269: bind: address already in
// use" (HTTP on 32881). compositeDDLServer (composite_ddl_protocol_test.go)
// picks each port by listening on 127.0.0.1:0 and closing the listener, then
// starts the server process, which binds the ports seconds later; anything
// that takes the closed port in between makes the server exit.
//
//   control:  the same probe and command line; both ports answer; the probe-to-
//             listen window is logged.
//   occupied: the same, with the probed Bolt port taken by another listener
//             between the probe and the start; expected: the server exits with
//             the same "address already in use" error.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func l7019Probe(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

func l7019Start(t *testing.T, binary, dir, hp, bp string) (*exec.Cmd, chan error, string) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "server.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "serve", "--address=127.0.0.1", "--http-port="+hp, "--bolt-port="+bp, "--data-dir="+filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0", "--no-auth")
	cmd.Dir = dir
	cmd.Env = []string{"HOME=" + dir, "PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "NORNICDB_LANGUAGE=en", "NORNICDB_BOLT_ENABLED=true", "NORNICDB_EMBEDDING_ENABLED=false", "NORNICDB_TELEMETRY_ENABLED=false", "NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0", "NORNICDB_AUTH_JWT_SECRET=synthetic-composite-ddl-key-982741", "NORNICDB_PARSER=nornic"}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-exited; _ = log.Close() })
	return cmd, exited, logPath
}

func l7019Dial(port string) bool {
	c, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func TestL7019PortProbeWindow(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := cliTestExecutablePath(dir, "nornicdb")
	build := exec.Command("go", "build", "-tags=noui,nolocalllm", "-o", binary, "./cmd/nornicdb")
	build.Dir = root
	if data, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, data)
	}

	t.Run("control", func(t *testing.T) {
		hp, bp := l7019Probe(t), l7019Probe(t)
		probed := time.Now()
		_, exited, logPath := l7019Start(t, binary, filepath.Join(dir, "control"), hp, bp)
		deadline := time.Now().Add(60 * time.Second)
		for !(l7019Dial(hp) && l7019Dial(bp)) {
			select {
			case err := <-exited:
				data, _ := os.ReadFile(logPath)
				t.Fatalf("server exited: %v\n%s", err, data)
			default:
			}
			if time.Now().After(deadline) {
				data, _ := os.ReadFile(logPath)
				t.Fatalf("server not ready\n%s", data)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("L7_019 control http=%s bolt=%s both listening %s after the probe closed its listener", hp, bp, time.Since(probed))
	})

	t.Run("occupied", func(t *testing.T) {
		hp, bp := l7019Probe(t), l7019Probe(t)
		squatter, err := net.Listen("tcp", "127.0.0.1:"+bp)
		if err != nil {
			t.Fatalf("taking the probed port: %v", err)
		}
		defer squatter.Close()
		_, exited, logPath := l7019Start(t, binary, filepath.Join(dir, "occupied"), hp, bp)
		select {
		case err := <-exited:
			data, _ := os.ReadFile(logPath)
			text := string(data)
			t.Logf("L7_019 occupied http=%s bolt=%s server exited: %v", hp, bp, err)
			want := "bolt: failed to listen on 127.0.0.1:" + bp
			if !strings.Contains(text, want) || !strings.Contains(text, "address already in use") {
				t.Fatalf("exit without the expected error %q\n%s", want, text)
			}
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, "address already in use") {
					t.Logf("L7_019 occupied server log: %s", strings.TrimSpace(line))
				}
			}
		case <-time.After(60 * time.Second):
			data, _ := os.ReadFile(logPath)
			t.Fatalf("server did not exit with the Bolt port taken\n%s", data)
		}
	})
}
