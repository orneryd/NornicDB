package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// The public database writes the source; the CLI exports it and imports a new
// directory. Ordinary CSV quoting is checked here. The tagged large-value
// qualification reuses this exact journey across the former128MiB limit.
const (
	capacityOrdinaryBytes = 32 << 10

	capacitySmallLabel    = "CapacitySmall"
	capacityBigLabel      = "CapacityBig"
	capacitySmallName     = "capacity-small"
	capacitySmallKind     = "ordinary fixture"
	capacitySmallRevision = int64(7)
	capacitySmallVerified = true
)

func TestAdminCLIImportFullValueRoundTrip(t *testing.T) {
	checkAdminImportCapacityRoundTrip(t, capacityOrdinaryBytes, true)
}

func checkAdminImportCapacityRoundTrip(t *testing.T, size int, withQuotesAndNewlines bool) {
	t.Helper()
	// The payload is returned to the public store as one property value.
	// Only its length and digest are asserted afterwards, never the text.
	payload := capacityFixtureText(size, withQuotesAndNewlines)
	require.Len(t, payload, size)
	wantDigest := capacityTextDigest(payload)

	sourceDir := filepath.Join(t.TempDir(), "source")
	db := openCapacityDatabase(t, sourceDir)
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	_, err := db.ExecuteCypher(t.Context(),
		"CREATE (n:"+capacitySmallLabel+" {capacity_name: $name, capacity_kind: $kind, capacity_revision: $revision, capacity_verified: $verified})",
		map[string]any{
			"name":     capacitySmallName,
			"kind":     capacitySmallKind,
			"revision": capacitySmallRevision,
			"verified": capacitySmallVerified,
		})
	require.NoError(t, err)
	_, err = db.ExecuteCypher(t.Context(),
		"CREATE (n:"+capacityBigLabel+" {capacity_payload: $payload})",
		map[string]any{"payload": payload})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	db = nil

	// The written source reopens in the public database and holds the
	// same value before any admin tool reads the directory.
	db = openCapacityDatabase(t, sourceDir)
	verifyCapacityPayload(t, db, wantDigest, size)
	require.NoError(t, db.Close())
	db = nil

	exportDir := filepath.Join(t.TempDir(), "neo4j-csv")
	require.NoError(t, runCapacityAdminCommand(t, sourceDir,
		"database", "export", "neo4j-csv", "nornic", "--to-path", exportDir),
		"the CLI export reads the closed public source")

	importDir := filepath.Join(t.TempDir(), "imported")
	require.NoError(t, runCapacityAdminCommand(t, importDir,
		"database", "import", "full", "nornic", "--from-path", exportDir),
		"the CLI import writes the exported value into a fresh data directory")

	// The imported store is read through the public database twice: the
	// first reopen verifies the imported value, the second proves it is
	// stored rather than only visible in the first opened instance.
	for range 2 {
		db = openCapacityDatabase(t, importDir)
		verifyCapacitySmallProperties(t, db)
		verifyCapacityPayload(t, db, wantDigest, size)
		require.NoError(t, db.Close())
		db = nil
	}
}

// capacityFixtureText returns a deterministic value of exactly size bytes. The
// ordinary fixture repeats a pattern with a quote and a newline, so the CSV
// writer must escape it; the large fixture is plain ASCII so the capacity case
// does not also exercise CSV escaping across its whole value.
func capacityFixtureText(size int, withQuotesAndNewlines bool) string {
	pattern := "capacity-large-value-0123456789"
	if withQuotesAndNewlines {
		pattern = "capacity \"quoted\" value\n"
	}
	// A block that is a whole number of patterns keeps truncated writes on the
	// same deterministic sequence.
	block := strings.Repeat(pattern, (64<<10)/len(pattern)+1)
	var builder strings.Builder
	builder.Grow(size)
	for builder.Len() < size {
		remaining := size - builder.Len()
		if remaining < len(block) {
			builder.WriteString(block[:remaining])
			continue
		}
		builder.WriteString(block)
	}
	return builder.String()
}

func capacityTextDigest(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

// openCapacityDatabase opens a disposable database with the repository's
// normal public defaults with automatic compaction and embedding-related
// background owners disabled. The round-trip fixture owns its lifetime.
func openCapacityDatabase(t *testing.T, dataDir string) *nornicdb.DB {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Database.WALAutoCompactionEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(dataDir, config)
	require.NoError(t, err)
	return db
}

func runCapacityAdminCommand(t *testing.T, dataDir string, args ...string) error {
	t.Helper()
	command := newTestAdminRoot(nil)
	command.SilenceErrors = true
	command.SilenceUsage = true
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	command.SetArgs(append([]string{"--data-dir", dataDir}, args...))
	return command.Execute()
}

func verifyCapacitySmallProperties(t *testing.T, db *nornicdb.DB) {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(),
		"MATCH (n:"+capacitySmallLabel+") "+
			"RETURN labels(n) AS labels, n.capacity_name AS name, n.capacity_kind AS kind, "+
			"n.capacity_revision AS revision, n.capacity_verified AS verified",
		nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "the database holds one capacity-small node")
	row := result.Rows[0]
	require.Len(t, row, 5)
	require.Equal(t, []any{capacitySmallLabel}, row[0], "the small node keeps its label")
	require.Equal(t, capacitySmallName, row[1])
	require.Equal(t, capacitySmallKind, row[2])
	require.Equal(t, capacitySmallRevision, row[3])
	require.Equal(t, capacitySmallVerified, row[4])
}

func verifyCapacityPayload(t *testing.T, db *nornicdb.DB, wantDigest string, wantLength int) {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(),
		"MATCH (n:"+capacityBigLabel+") RETURN n.capacity_payload AS payload",
		nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "the database holds one capacity-big node")
	require.Len(t, result.Rows[0], 1)
	payload, ok := result.Rows[0][0].(string)
	require.True(t, ok, "the capacity payload is a string")
	// Compare only the length and digest so a failure never prints the value.
	require.Equal(t, wantLength, len(payload), "the capacity payload length round-trips")
	require.Equal(t, wantDigest, capacityTextDigest(payload), "the capacity payload digest round-trips")
}
