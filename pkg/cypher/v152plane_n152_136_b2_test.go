package cypher

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-390 corruption-policy reproduction for public Cypher reads.
//
// A MATCH that reaches a stored row the storage engine cannot decode must
// return an error, in both executor parser modes, instead of returning the
// decodable rows as a complete answer. The fixture writes through the public
// Cypher path, closes the on-disk test store, replaces one stored entity body
// with undecodable bytes (pkg/storage/badger.go: prefixNode 0x01,
// prefixEdge 0x02), and reopens it. Each case runs a valid control over the
// same fixture first, proving the query reaches the rows and returns them; the
// corrupt case runs the same public path and expects a visible failure with no
// rows.
//
// Node bodies are narrower: current source's engine constructor verifies
// derived label counts by decoding every stored node body, so *any* corrupt
// node body fails the public constructor before an executor exists. The
// node-side case records that visible failure instead of claiming the MATCH
// route is reachable; the relationship case is the reachable query-level
// corruption.

const (
	nor390CypherNodeBodyPrefix = byte(0x01)
	nor390CypherEdgeBodyPrefix = byte(0x02)
)

// nor390CypherStore creates the fixture through the public Cypher path, closes
// the on-disk engine, and returns the storage directory.
func nor390CypherStore(t *testing.T, create string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "graph")
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	fixtureowner.Own(t, base)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
	_, err = executor.Execute(context.Background(), create, nil)
	require.NoError(t, err)
	require.NoError(t, base.Close())
	return dir
}

// nor390CorruptCypherRecord replaces every stored body whose key carries
// prefix and whose value contains marker (an empty marker selects every body
// of that kind) with undecodable bytes in the closed test-owned store. The
// marker is a property value the fixture wrote, so the case does not depend on
// an executor-generated entity ID.
func nor390CorruptCypherRecord(t *testing.T, dir string, prefix byte, marker string) {
	t.Helper()
	raw, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var targets [][]byte
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 || key[0] != prefix {
				continue
			}
			if marker != "" {
				matches := false
				if err := it.Item().Value(func(value []byte) error {
					matches = bytes.Contains(value, []byte(marker))
					return nil
				}); err != nil {
					return err
				}
				if !matches {
					continue
				}
			}
			targets = append(targets, it.Item().KeyCopy(nil))
		}
		if len(targets) == 0 {
			return os.ErrNotExist
		}
		for _, target := range targets {
			if err := txn.Set(target, []byte{0xff}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func nor390OpenCypherStore(t *testing.T, dir string) *storage.BadgerEngine {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	fixtureowner.Own(t, base)
	return base
}

// nor390ExecuteOnReopenedStore runs query against the reopened test store.
func nor390ExecuteOnReopenedStore(t *testing.T, dir, query string) ([][]any, error) {
	t.Helper()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(nor390OpenCypherStore(t, dir), "test"))
	result, err := executor.Execute(context.Background(), query, nil)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func TestNOR390CypherNodeQueryCannotReachCorruptStoredNode(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		const create = `CREATE (:Doc {name: 'alpha'}), (:Doc {name: 'bravo'}), (u {name: 'loose'})`
		const query = `MATCH (n) RETURN n.name`

		t.Run("valid control", func(t *testing.T) {
			rows, err := nor390ExecuteOnReopenedStore(t, nor390CypherStore(t, create), query)
			require.NoError(t, err)
			require.Len(t, rows, 3, "the query reaches every stored node")
		})

		t.Run("corrupt row", func(t *testing.T) {
			dir := nor390CypherStore(t, create)
			nor390CorruptCypherRecord(t, dir, nor390CypherNodeBodyPrefix, "loose")
			// No executor is obtained: the public constructor verifies derived
			// label counts by decoding stored bodies and reports the corrupt
			// row. Neither parser mode can produce a partial answer because no
			// statement runs.
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			if base != nil {
				fixtureowner.Own(t, base)
			}
			require.ErrorContains(t, err, "failed to initialize label counts")
			require.ErrorContains(t, err, "0xff")
		})
	})
}

func TestNOR390CypherTraversalFailsOnCorruptStoredEdge(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		const create = `CREATE (:Doc {name: 'alpha'})-[:LINK]->(:Doc {name: 'bravo'})`
		const query = `MATCH (a)-[r:LINK]->(b) RETURN r`

		t.Run("valid control", func(t *testing.T) {
			rows, err := nor390ExecuteOnReopenedStore(t, nor390CypherStore(t, create), query)
			require.NoError(t, err)
			require.Len(t, rows, 1, "the traversal reaches the stored relationship")
		})

		t.Run("corrupt row", func(t *testing.T) {
			dir := nor390CypherStore(t, create)
			nor390CorruptCypherRecord(t, dir, nor390CypherEdgeBodyPrefix, "")
			rows, err := nor390ExecuteOnReopenedStore(t, dir, query)
			require.Error(t, err, "the traversal must not report a partial relationship set")
			require.Empty(t, rows)
		})
	})
}
