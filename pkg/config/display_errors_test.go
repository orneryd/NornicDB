package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigDisplaySecretDecode(t *testing.T) {
	const credential = "synthetic-yaml-credential-314159"
	path := filepath.Join(t.TempDir(), "config.yaml")
	// A misplaced credential triggers YAML's scalar-quoting type error. The
	// correctly named API-key field supplies the same owned credential context.
	data := []byte("embedding:\n  api_key: " + credential + "\nserver:\n  bolt_port: " + credential + "\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("expected configuration type error")
	}
	var typed *yaml.TypeError
	if !errors.As(err, &typed) {
		t.Error("YAML error identity lost")
	}
	if strings.Contains(err.Error(), credential[:7]) {
		t.Error("YAML diagnostic disclosed a credential (details withheld)")
	}
}
