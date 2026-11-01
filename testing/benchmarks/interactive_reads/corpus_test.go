// Author: Codex NOR-285 implementation agent, 2026-10-26.
package interactive_reads_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

const corpusDigestAlgorithm = "sha256-canonical-file-map-v1"

// Hash the actual file bytes, not a supplied inventory or filesystem path. The
// canonical manifest is compact JSON mapping slash-relative paths to file SHA256s;
// encoding/json sorts map keys. File modes and timestamps are not content identity.
func savedCorpusDigest(root string) (string, error) {
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("corpus contains a non-regular file: %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, file)
		closeErr := file.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = fmt.Sprintf("%x", hash.Sum(nil))
		return nil
	})
	if err != nil {
		return "", err
	}
	manifest, err := json.Marshal(files)
	if err != nil {
		return "", err
	}
	return digest(manifest), nil
}

func requireCorpusIdentity(actual, expected string) error {
	if expected == "" || actual != expected {
		return fmt.Errorf("saved corpus SHA256 %s differs from frozen packet %s", actual, expected)
	}
	return nil
}

// An empty expected identity is reserved for the separately labelled historical
// census. Both measured reads and maintained-store plumbing supply a frozen hash.
func openSavedCorpus(t *testing.T, expectedCorpusSHA256 string) (*cypher.StorageExecutor, string) {
	t.Helper()
	corpus := os.Getenv("NOR285_CORPUS")
	info, err := os.Stat(corpus)
	must(t, err)
	if !info.IsDir() {
		t.Fatal("NOR285_CORPUS must be a complete stopped database directory")
	}
	storePath := filepath.Join(t.TempDir(), "database")
	must(t, os.CopyFS(storePath, os.DirFS(corpus)))
	// Verify precisely the private bytes the engine will open, before Open can
	// upgrade storage or write WAL/search state, and outside Execute timing.
	corpusSHA256, err := savedCorpusDigest(storePath)
	must(t, err)
	if expectedCorpusSHA256 != "" {
		must(t, requireCorpusIdentity(corpusSHA256, expectedCorpusSHA256))
	}
	// The normal DB owner constructs WAL, storage, event subscriptions and the
	// executor. TOPIC_GROUPS physically writes a temporary Louvain projection,
	// so opening bare Badger would omit part of its supported execution cost.
	// Keep the explicit config and environment on the same library namespace.
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "library")
	t.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "false")
	config := nornicdb.DefaultConfig()
	config.Database.DefaultDatabase = "library"
	config.Database.AllowStorageUpgrade = true
	config.Database.AsyncWritesEnabled = false
	config.Database.StrictDurability = true
	config.Database.WALSyncMode = "immediate"
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	// This profiles the graph executor without external providers. The separate
	// Rust acceptance uses the complete original deployment configuration.
	config.Memory.EmbeddingEnabled = false
	config.Memory.SearchBM25Properties = []string{"text", "search_text"}
	config.Memory.SearchRelationshipBM25Properties = []string{"search_text"}
	config.Memory.EmbeddingDimensions = 2048
	db, err := nornicdb.Open(storePath, config)
	must(t, err)
	fixtureowner.Own(t, db)
	_, err = db.EnsureSearchIndexes(context.Background(), "library", db.GetStorage(), nornicdb.SearchIndexBuildWait)
	must(t, err)
	emit(t, "database-path", map[string]interface{}{"owner": "nornicdb.Open/GetCypherExecutor", "namespace": "library", "strict_durability": true, "wal_sync": "immediate", "async_writes": false, "embeddings": false, "decay": false, "auto_links": false, "search_indexes_ready": true})
	return db.GetCypherExecutor(), corpusSHA256
}

func graphCounts(t *testing.T, executor *cypher.StorageExecutor) map[string]interface{} {
	t.Helper()
	counts := map[string]interface{}{}
	for _, entry := range []struct{ name, query string }{
		{"nodes", "MATCH (n) RETURN count(n) AS n"},
		{"relationships", "MATCH ()-[r]->() RETURN count(r) AS n"},
		{"topic_nodes", "MATCH (n:PDTopicLink) RETURN count(n) AS n"},
		{"topic_relationships", "MATCH ()-[r:PD_TOPIC_LINK]->() RETURN count(r) AS n"},
	} {
		result, err := executor.Execute(context.Background(), entry.query, nil)
		must(t, err)
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatalf("invalid graph count: %+v", result)
		}
		counts[entry.name] = result.Rows[0][0]
	}
	return counts
}

func requireNoTopicScratch(t *testing.T, counts map[string]interface{}) {
	t.Helper()
	if fmt.Sprint(counts["topic_nodes"]) != "0" || fmt.Sprint(counts["topic_relationships"]) != "0" {
		t.Fatalf("temporary topic projection remains: %v", counts)
	}
}
