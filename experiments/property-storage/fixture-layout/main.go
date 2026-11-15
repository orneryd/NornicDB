// Command fixture-layout inspects or settles an offline laboratory store copy.
// Build the same source against each comparison's module to preserve its codecs.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type level struct {
	Tables                       int
	Keys, Disk, Raw, Stale       uint64
	IndexBytes, BloomFilterBytes uint64
}

func snapshot(db *badger.DB) map[int]*level {
	levels := make(map[int]*level)
	for _, table := range db.Tables() {
		value := levels[table.Level]
		if value == nil {
			value = new(level)
			levels[table.Level] = value
		}
		value.Tables++
		value.Keys += uint64(table.KeyCount)
		value.Disk += uint64(table.OnDiskSize)
		value.Raw += uint64(table.UncompressedSize)
		value.Stale += uint64(table.StaleDataSize)
		value.IndexBytes += uint64(table.IndexSz)
		value.BloomFilterBytes += uint64(table.BloomFilterSize)
	}
	return levels
}

func run() error {
	data := flag.String("data", "", "offline laboratory store copy")
	out := flag.String("out", "", "receipt JSON file")
	mode := flag.String("mode", "inspect", "inspect (read only) or settle (physical compaction)")
	flag.Parse()
	if *data == "" || *out == "" {
		return fmt.Errorf("data and out are required")
	}
	var before, after map[int]*level
	switch *mode {
	case "inspect":
		db, err := badger.OpenManaged(badger.DefaultOptions(*data).WithReadOnly(true).WithLogger(nil).WithBlockCacheSize(1 << 20))
		if err != nil {
			return err
		}
		before = snapshot(db)
		if err := db.Close(); err != nil {
			return err
		}
	case "settle":
		// Nornic configures the table encoder and field-layout annotation. Keep
		// its open/close and managed transaction ownership, with no application writes.
		engine, err := storage.NewBadgerEngine(*data)
		if err != nil {
			return err
		}
		before = snapshot(engine.DB())
		if err := engine.DB().Flatten(4); err != nil {
			_ = engine.Close()
			return err
		}
		after = snapshot(engine.DB())
		if err := engine.Close(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
	var size int64
	if err := filepath.Walk(*data, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	}); err != nil {
		return err
	}
	report := map[string]any{"data": *data, "mode": *mode, "experimental_env": os.Getenv("NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED"), "before": before, "after": after, "closed_file_bytes": size}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*out, append(raw, '\n'), 0600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
