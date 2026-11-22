//go:build closeout_l2_e31

package main

// L2 close-out, report entry 31 (fixture B): nornicdb-admin cannot import an
// export that holds a property the server accepted. The database's default
// store profile takes a value up to its 256 MiB value-log file size; on
// 2dbc67e96 `database import full` opened its store with the default engine
// profile (128 MiB) and failed with "Value with size ... exceeded 134217728
// limit", leaving no such node in the imported store.
//
// Scaling: the value must be larger than the 128 MiB limit the import hit and
// smaller than the server's 256 MiB, so it cannot shrink below 128 MiB. The
// entry's 142,606,336-character base64 text is replaced by 130 MiB of
// repeated ASCII (136,314,880 bytes; the stored node is a few hundred bytes
// more, above 134,217,728). Only the stored length is compared, as in the
// entry. Expected peak a few GiB; a soft memory limit of 6 GiB keeps the
// collector ahead of the 8 GiB ceiling.

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/cli"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	l2E31BigBytes   = 130 << 20
	l2E31SmallBytes = 1 << 20
)

func l2E31Payload(size int) string {
	block := strings.Repeat("l2-e31-large-value-0123456789abcdef", (64<<10)/35+1)
	var builder strings.Builder
	builder.Grow(size)
	for builder.Len() < size {
		builder.WriteString(block[:min(len(block), size-builder.Len())])
	}
	return builder.String()
}

func l2E31Open(t *testing.T, dir string) *nornicdb.DB {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.AutoLinksEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.EmbeddingEnabled = false
	db, err := nornicdb.Open(dir, config)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return db
}

// l2E31Read returns the id and stored length of every Big node.
func l2E31Read(t *testing.T, dir string) []string {
	t.Helper()
	db := l2E31Open(t, dir)
	defer func() { _ = db.Close() }()
	result, err := db.ExecuteCypher(context.Background(), "MATCH (n:Big) RETURN n.id, size(n.blob)", nil)
	if err != nil {
		t.Fatalf("read Big: %v", err)
	}
	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, fmt.Sprint(row...))
	}
	return rows
}

// l2E31Source writes the entry's fixture B through the embedded database,
// closes it, and checks the value reads back after a reopen.
func l2E31Source(t *testing.T, dir string, size int) []string {
	t.Helper()
	db := l2E31Open(t, dir)
	ctx := context.Background()
	if _, err := db.ExecuteCypher(ctx, "CREATE (:Small {id: 's1'})", nil); err != nil {
		_ = db.Close()
		t.Fatalf("create Small: %v", err)
	}
	payload := l2E31Payload(size)
	_, err := db.ExecuteCypher(ctx, "CREATE (:Big {id: 'big', blob: $b})", map[string]interface{}{"b": payload})
	payload = ""
	if err != nil {
		_ = db.Close()
		t.Fatalf("server refused the value of %d bytes: %v", size, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	runtime.GC()
	return l2E31Read(t, dir)
}

// l2E31Admin runs the tool's root command in-process.
func l2E31Admin(dataDir string, args ...string) error {
	root := cli.NewRoot("nornicdb-admin", nil, localization.AdminCLIRootShort(), localization.Message{})
	registerAdminCommands(root, nil)
	root.SilenceErrors = true
	root.SilenceUsage = true
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs(append([]string{"--data-dir", dataDir}, args...))
	return root.Execute()
}

type l2E31Journey struct {
	source, imported []string
	exportErr        error
	importErr        error
	exportDir        string
	peakSys          uint64
}

func l2E31Run(t *testing.T, size int) l2E31Journey {
	t.Helper()
	previous := debug.SetMemoryLimit(6 << 30)
	defer debug.SetMemoryLimit(previous)
	var journey l2E31Journey
	sourceDir := filepath.Join(t.TempDir(), "source")
	journey.source = l2E31Source(t, sourceDir, size)
	journey.exportDir = filepath.Join(t.TempDir(), "neo4j-csv")
	journey.exportErr = l2E31Admin(sourceDir, "database", "export", "neo4j-csv", "nornic", "--to-path", journey.exportDir)
	runtime.GC()
	if journey.exportErr == nil {
		importDir := filepath.Join(t.TempDir(), "imported")
		journey.importErr = l2E31Admin(importDir, "database", "import", "full", "nornic", "--from-path", journey.exportDir)
		runtime.GC()
		journey.imported = l2E31Read(t, importDir)
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	journey.peakSys = stats.Sys
	return journey
}

func (j l2E31Journey) figures(size int) string {
	return fmt.Sprintf("value=%d bytes source=%v exportErr=%v importErr=%v imported=%v runtimeSys=%dMiB",
		size, j.source, j.exportErr, j.importErr, j.imported, j.peakSys>>20)
}

func TestL2_e31_Repro(t *testing.T) {
	journey := l2E31Run(t, l2E31BigBytes)
	want := fmt.Sprint("big", l2E31BigBytes)
	figures := journey.figures(l2E31BigBytes)
	if len(journey.source) != 1 || journey.source[0] != want {
		t.Fatalf("source store does not hold the value it accepted (want %q): %s", want, figures)
	}
	if journey.exportErr != nil {
		t.Logf("L2RESULT E31 DEFECT export failed: %s", figures)
		t.Fatalf("database export neo4j-csv failed: %s", figures)
	}
	if journey.importErr != nil || len(journey.imported) != 1 || journey.imported[0] != want {
		t.Logf("L2RESULT E31 DEFECT %s", figures)
		t.Errorf("database import full of the export does not reproduce the node the server accepted: %s", figures)
		return
	}
	t.Logf("L2RESULT E31 OK %s", figures)
}

// The control: the same journey with a 1 MiB value, inside every profile's
// limit, must round-trip.
func TestL2_e31_Control(t *testing.T) {
	journey := l2E31Run(t, l2E31SmallBytes)
	want := fmt.Sprint("big", l2E31SmallBytes)
	figures := journey.figures(l2E31SmallBytes)
	if journey.exportErr != nil || journey.importErr != nil || len(journey.imported) != 1 || journey.imported[0] != want {
		t.Fatalf("control round trip failed: %s", figures)
	}
	t.Logf("L2RESULT E31 CONTROL OK %s", figures)
}

// The cause the entry names: the store profile the import opens. The export
// is imported by adminimport.ImportFull, the call runImportFull makes
// (cmd/nornicdb-admin/main.go:297 at the pin), into a store opened with the
// default engine profile (storage.NewBadgerEngine, 128 MiB value-log files)
// and into one opened with the high-performance profile the server uses
// (256 MiB). If only the default profile fails, the import's profile is the
// cause; the pin's runImportFull opens HighPerformance.
func TestL2_e31_Cause(t *testing.T) {
	journey := l2E31Run(t, l2E31BigBytes)
	t.Logf("L2CAUSE E31 journey %s", journey.figures(l2E31BigBytes))
	if journey.exportErr != nil {
		t.Fatalf("export failed, no import to diagnose: %v", journey.exportErr)
	}
	nodes, rels, err := adminimport.DiscoverNeo4jCSVSources(journey.exportDir, adminimport.Options{Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: ';', Quote: '"'})
	if err != nil {
		t.Fatalf("discover export: %v", err)
	}
	for _, profile := range []struct {
		name string
		open func(dir string) (*storage.BadgerEngine, error)
	}{
		{"default", func(dir string) (*storage.BadgerEngine, error) { return storage.NewBadgerEngine(dir) }},
		{"high-performance", func(dir string) (*storage.BadgerEngine, error) {
			return storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, HighPerformance: true})
		}},
	} {
		dir := filepath.Join(t.TempDir(), profile.name)
		engine, err := profile.open(dir)
		if err != nil {
			t.Fatalf("open %s: %v", profile.name, err)
		}
		report, importErr := adminimport.ImportFull(context.Background(), engine, adminimport.Options{
			DatabaseName: "nornic", NodeSources: nodes, RelSources: rels, DataDir: dir,
			Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: ';', Quote: '"',
			NormalizeTypes: true, BuildIndexes: true,
		})
		_ = engine.Close()
		runtime.GC()
		t.Logf("L2CAUSE E31 ImportFull into %s profile: err=%v nodesImported=%d status=%q", profile.name, importErr, report.NodesImported, report.Status)
	}
}
