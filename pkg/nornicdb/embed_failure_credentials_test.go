package nornicdb

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestTerminalEmbeddingFailureDiagnosticsRedactConfiguredCredentials(t *testing.T) {
	const marker = "synthetic-r73-embed-failure-764219"
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, err := store.CreateNode(&storage.Node{
		ID:         "tenant:document",
		Labels:     []string{"Document"},
		Properties: map[string]any{"text": "synthetic document"},
	})
	if err != nil {
		t.Fatal("could not create test node")
	}
	worker := NewEmbedWorker(nil, store, &EmbedWorkerConfig{NumWorkers: 0, DeferWorkerStart: true})
	defer worker.Close()
	database := &DB{embedQueue: worker}
	database.SetDefaultEmbedConfig(&embed.Config{APIURL: "https://default.example.invalid", APIKey: "synthetic-default-key"})
	database.SetEmbedConfigForDB(func(name string) (*embed.Config, error) {
		if name != "tenant" {
			return nil, nil
		}
		return &embed.Config{
			APIURL: "https://remote.example.invalid/vectors?api_key=" + marker,
			APIKey: marker,
		}, nil
	})

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal("could not capture worker output")
	}
	t.Cleanup(func() {
		os.Stdout = oldStdout
		_ = writer.Close()
		_ = reader.Close()
	})
	os.Stdout = writer
	worker.markNodeEmbeddingFailed("tenant:document", errors.New(
		"provider rejected https://remote.example.invalid/vectors?api_key="+marker+" key="+marker,
	))
	_ = writer.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(reader)
	_ = reader.Close()

	node, err := store.GetNode("tenant:document")
	if err != nil {
		t.Fatal("failed node was not stored")
	}
	storedError, _ := node.EmbedMeta["embedding_error"].(string)
	if strings.Contains(storedError, marker) || strings.Contains(string(output), marker) {
		t.Error("terminal embedding failure disclosed a configured credential; value withheld")
	}
	failures, err := worker.ParkedEmbeddingFailures(t.Context(), 10)
	if err != nil || len(failures) != 1 || strings.Contains(failures[0].Error, marker) {
		t.Error("stored embedding failure response disclosed a configured credential; value withheld")
	}
}
