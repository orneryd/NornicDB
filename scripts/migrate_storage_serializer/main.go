// migrate_storage_serializer
// Rewrites legacy gob-encoded BadgerDB bodies as msgpack in place.
//
// Usage:
//
//	go run scripts/migrate_storage_serializer/main.go --data-dir ./data
//	go run scripts/migrate_storage_serializer/main.go --data-dir ./data --dry-run
//
// IMPORTANT:
//   - Stop NornicDB before running (offline migration).
//   - Take a backup before converting.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func main() {
	dataDir := flag.String("data-dir", "", "Badger data directory (required)")
	dryRun := flag.Bool("dry-run", false, "scan only; do not write")
	batchSize := flag.Int("batch-size", 1000, "write batch size (0 = auto)")
	flag.Parse()

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "missing --data-dir")
		flag.Usage()
		os.Exit(2)
	}

	stats, err := storage.MigrateBadgerToMsgpack(*dataDir, storage.SerializerMigrationOptions{
		BatchSize: *batchSize,
		DryRun:    *dryRun,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}

	if !stats.HasLegacyData {
		fmt.Printf("No legacy gob data found in %s. Nothing to migrate.\n", stats.DataDir)
		return
	}

	fmt.Printf("Storage migration (gob -> msgpack)\n")
	fmt.Printf("Data dir: %s\n", stats.DataDir)
	fmt.Printf("Dry run: %v\n", *dryRun)
	fmt.Printf("Scanned: %d\n", stats.TotalScanned)
	fmt.Printf("Converted nodes: %d\n", stats.NodesConverted)
	fmt.Printf("Converted edges: %d\n", stats.EdgesConverted)
	fmt.Printf("Converted embeddings: %d\n", stats.EmbeddingsConverted)
	fmt.Printf("Already msgpack: %d\n", stats.SkippedExisting)

	if *dryRun {
		fmt.Println("Dry run complete. Re-run without --dry-run to apply.")
	}
}
