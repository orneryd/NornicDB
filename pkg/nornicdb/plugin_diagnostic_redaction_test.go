package nornicdb

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
)

func TestLoadPluginsFromDirRedactsConfiguredCredentialsFromOutput(t *testing.T) {
	apiKey := "synthetic-api-key"
	ctx := &heimdall.SubsystemContext{Config: heimdall.Config{
		APIURL: "https://synthetic-user:synthetic-pass@example.invalid/v1",
		APIKey: apiKey,
	}}
	dir := filepath.Join(t.TempDir(), apiKey)

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal("failed to capture plugin loader output")
	}
	previousStdout := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = previousStdout })
	loadErr := LoadPluginsFromDir(dir, ctx)
	_ = writer.Close()
	os.Stdout = previousStdout
	output, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || loadErr != nil {
		t.Fatal("plugin directory scan failed")
	}
	if strings.Contains(string(output), apiKey) {
		t.Fatal("plugin loader output exposed a configured credential")
	}
}
