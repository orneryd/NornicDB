package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCLISecretYAMLDiagnostics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	// A leading '*' is YAML alias syntax. Its parser error quotes the input
	// before a typed configuration or its credential fields can be recovered.
	data := []byte("embedding:\n  api_key: *" + cliSecretProbeFlag + "\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := cliSecretProbeCommand(t, dir, nil, []string{"serve", "--config=" + path})
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("invalid YAML did not fail configuration")
	}
	assertCLISecretAbsent(t, "YAML syntax diagnostic", output)
}

func TestCLISecretAuditSigningKeyYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte("encryption_audit_sign_key: &audit " + cliSecretProbeFlag + "\nserver:\n  bolt_port: *audit\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := cliSecretProbeCommand(t, dir, nil, []string{"serve", "--config=" + path})
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("invalid YAML did not fail configuration")
	}
	if bytes.Contains(output, []byte(cliSecretProbeFlag[:7])) {
		t.Error("CLI YAML diagnostic disclosed audit signing key material; output withheld")
	}
}
