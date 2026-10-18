package main

import (
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
