// Qdrant → NornicDB migration (Go).
//
// Streams collections + points from a source Qdrant instance and replays
// them into NornicDB through NornicDB's Qdrant-compatible gRPC endpoint.
// Both sides speak the same gRPC API, so a single client library
// (`github.com/qdrant/go-client/qdrant`) is used for both.
//
// Build & run:
//   go run ./scripts/migration/qdrant/migrate.go \
//       --source-host qdrant.local --source-port 6334 \
//       --target-host nornicdb.local --target-port 6334 \
//       --batch-size 256
//
// The script:
//   1. Lists collections on the source.
//   2. Replicates each collection's vector config into the target.
//   3. Scrolls points in batches and Upserts them into the target.
//   4. Verifies the point count.
//
// Re-running is idempotent: collections are skipped if they already exist
// (with --skip-existing), and Upsert is idempotent by point ID.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/qdrant/go-client/qdrant"
)

type config struct {
	sourceHost     string
	sourcePort     int
	sourceAPIKey   string
	targetHost     string
	targetPort     int
	targetAPIKey   string
	collections    string
	batchSize      uint32
	skipExisting   bool
	dryRun         bool
	connectTimeout time.Duration
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.sourceHost, "source-host", "localhost", "Qdrant source host")
	flag.IntVar(&cfg.sourcePort, "source-port", 6334, "Qdrant source gRPC port")
	flag.StringVar(&cfg.sourceAPIKey, "source-api-key", "", "Qdrant source API key (optional)")
	flag.StringVar(&cfg.targetHost, "target-host", "localhost", "NornicDB host")
	flag.IntVar(&cfg.targetPort, "target-port", 6334, "NornicDB Qdrant-compat gRPC port")
	flag.StringVar(&cfg.targetAPIKey, "target-api-key", "", "NornicDB API key (optional)")
	flag.StringVar(&cfg.collections, "collections", "", "Comma-separated subset; empty = all")
	batch := flag.Uint("batch-size", 256, "Points per upsert batch")
	flag.BoolVar(&cfg.skipExisting, "skip-existing", false, "Skip collections already present in target")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Print the plan but write nothing")
	flag.DurationVar(&cfg.connectTimeout, "connect-timeout", 10*time.Second, "Initial connection timeout")
	flag.Parse()
	cfg.batchSize = uint32(*batch)
	return cfg
}

func newClient(host string, port int, apiKey string) (*qdrant.Client, error) {
	c, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 apiKey,
		UseTLS:                 false,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s:%d: %w", host, port, err)
	}
	return c, nil
}

func collectionExists(ctx context.Context, c *qdrant.Client, name string) (bool, error) {
	_, err := c.GetCollectionInfo(ctx, name)
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "doesn't exist") {
		return false, nil
	}
	return false, err
}

func replicateCollectionConfig(ctx context.Context, src, dst *qdrant.Client, name string, dryRun bool) error {
	info, err := src.GetCollectionInfo(ctx, name)
	if err != nil {
		return fmt.Errorf("get source config: %w", err)
	}

	params := info.GetConfig().GetParams()
	vectorsCfg := params.GetVectorsConfig()
	if vectorsCfg == nil {
		return fmt.Errorf("collection %q has no vectors_config on source", name)
	}

	fmt.Printf("  → create collection %q\n", name)
	if dryRun {
		return nil
	}

	create := &qdrant.CreateCollection{
		CollectionName: name,
		VectorsConfig:  vectorsCfg,
	}
	if err := dst.CreateCollection(ctx, create); err != nil {
		return fmt.Errorf("create on target: %w", err)
	}
	return nil
}

// vectorsOutputToInput converts the read-side VectorsOutput shape (returned
// by Scroll) into the write-side Vectors shape that Upsert accepts. Handles
// the single-dense and named-dense cases, which is what the Qdrant
// compatibility layer in NornicDB exposes today. Sparse and multi-vector
// cases are skipped with a warning since NornicDB doesn't ingest them.
func vectorsOutputToInput(out *qdrant.VectorsOutput) *qdrant.Vectors {
	if out == nil {
		return nil
	}
	if single := out.GetVector(); single != nil {
		data := single.GetData()
		if len(data) == 0 {
			return nil
		}
		return qdrant.NewVectors(data...)
	}
	if named := out.GetVectors(); named != nil {
		m := named.GetVectors()
		if len(m) == 0 {
			return nil
		}
		converted := make(map[string]*qdrant.Vector, len(m))
		for vname, v := range m {
			data := v.GetData()
			if len(data) == 0 {
				continue
			}
			converted[vname] = &qdrant.Vector{Data: data}
		}
		if len(converted) == 0 {
			return nil
		}
		return &qdrant.Vectors{
			VectorsOptions: &qdrant.Vectors_Vectors{
				Vectors: &qdrant.NamedVectors{Vectors: converted},
			},
		}
	}
	return nil
}

func migratePoints(ctx context.Context, src, dst *qdrant.Client, name string, batch uint32, dryRun bool) (uint64, error) {
	var (
		offset *qdrant.PointId
		total  uint64
	)
	withPayload := qdrant.NewWithPayload(true)
	withVectors := qdrant.NewWithVectors(true)

	for {
		req := &qdrant.ScrollPoints{
			CollectionName: name,
			Limit:          &batch,
			Offset:         offset,
			WithPayload:    withPayload,
			WithVectors:    withVectors,
		}
		points, err := src.Scroll(ctx, req)
		if err != nil {
			return total, fmt.Errorf("scroll: %w", err)
		}
		if len(points) == 0 {
			break
		}

		ups := make([]*qdrant.PointStruct, 0, len(points))
		for _, p := range points {
			ups = append(ups, &qdrant.PointStruct{
				Id:      p.GetId(),
				Vectors: vectorsOutputToInput(p.GetVectors()),
				Payload: p.GetPayload(),
			})
		}

		if !dryRun {
			_, err := dst.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: name,
				Points:         ups,
			})
			if err != nil {
				return total, fmt.Errorf("upsert: %w", err)
			}
		}
		total += uint64(len(ups))
		fmt.Printf("  · %s: upserted %d points\n", name, total)

		if len(points) < int(batch) {
			break
		}
		offset = points[len(points)-1].GetId()
	}
	return total, nil
}

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	src, err := newClient(cfg.sourceHost, cfg.sourcePort, cfg.sourceAPIKey)
	if err != nil {
		log.Fatalf("source: %v", err)
	}
	dst, err := newClient(cfg.targetHost, cfg.targetPort, cfg.targetAPIKey)
	if err != nil {
		log.Fatalf("target: %v", err)
	}

	collections, err := src.ListCollections(ctx)
	if err != nil {
		log.Fatalf("list source collections: %v", err)
	}

	requested := map[string]struct{}{}
	for _, n := range strings.Split(cfg.collections, ",") {
		if t := strings.TrimSpace(n); t != "" {
			requested[t] = struct{}{}
		}
	}

	var names []string
	for _, c := range collections {
		if len(requested) == 0 {
			names = append(names, c)
			continue
		}
		if _, ok := requested[c]; ok {
			names = append(names, c)
		}
	}
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "No collections to migrate.")
		os.Exit(1)
	}

	fmt.Printf("Migrating %d collection(s): %v\n", len(names), names)
	started := time.Now()

	for _, name := range names {
		fmt.Printf("\n[%s]\n", name)
		exists, err := collectionExists(ctx, dst, name)
		if err != nil {
			log.Fatalf("collection exists check: %v", err)
		}
		if exists && cfg.skipExisting {
			fmt.Printf("  · target already has %q, --skip-existing → skipping\n", name)
			continue
		}
		if !exists {
			if err := replicateCollectionConfig(ctx, src, dst, name, cfg.dryRun); err != nil {
				log.Fatalf("replicate config: %v", err)
			}
		}

		exact := true
		srcCount, err := src.Count(ctx, &qdrant.CountPoints{CollectionName: name, Exact: &exact})
		if err != nil {
			log.Fatalf("source count: %v", err)
		}
		moved, err := migratePoints(ctx, src, dst, name, cfg.batchSize, cfg.dryRun)
		if err != nil {
			log.Fatalf("migrate points: %v", err)
		}

		if !cfg.dryRun {
			dstCount, err := dst.Count(ctx, &qdrant.CountPoints{CollectionName: name, Exact: &exact})
			if err != nil {
				log.Fatalf("target count: %v", err)
			}
			marker := "✓"
			if srcCount != dstCount {
				marker = "✗"
			}
			fmt.Printf("  %s source=%d target=%d (migrated %d)\n", marker, srcCount, dstCount, moved)
			if srcCount != dstCount {
				fmt.Fprintln(os.Stderr, "    !! count mismatch — investigate before proceeding")
			}
		}
	}

	fmt.Printf("\nDone in %s\n", time.Since(started).Round(time.Second))
}
