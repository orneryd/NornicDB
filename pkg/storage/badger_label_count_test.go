package storage

import (
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_LabelCountMetadataMaintainedAcrossNodeMutations(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person", "Employee"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:n2", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n3", Labels: []string{"Person"}})
	require.NoError(t, err)

	count, err := engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	count, err = engine.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = engine.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	require.NoError(t, engine.UpdateNode(&Node{ID: "db1:n1", Labels: []string{"Employee", "Admin"}}))

	count, err = engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = engine.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = engine.NodeCountByLabelInNamespace("db1", "Admin")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	require.NoError(t, engine.DeleteNode("db1:n2"))

	count, err = engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Zero(t, count)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "db1:b1", Labels: []string{"Person"}},
		{ID: "db1:b2", Labels: []string{"Person", "Team"}},
	}))

	count, err = engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	count, err = engine.NodeCountByLabelInNamespace("db1", "Team")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestBadgerEngine_LabelCountMetadataMaintainedAcrossTransactionalCommit(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	_, err = tx.CreateNode(&Node{ID: "db1:t1", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "db1:t1", Labels: []string{"Employee"}}))
	require.NoError(t, tx.Commit())

	count, err := engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Zero(t, count)

	count, err = engine.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.DeleteNode("db1:t1"))
	require.NoError(t, tx.Commit())

	count, err = engine.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestBadgerEngine_LabelCountMetadataRebuildsOnReopenWhenDriftDetected(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person", "Employee"}})
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "db1:n2", Labels: []string{"Person"}})
	require.NoError(t, err)

	require.NoError(t, engine1.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		wrong := make([]byte, 8)
		binary.BigEndian.PutUint64(wrong, 99)
		if err := txn.Set(labelCountKey("db1", "person"), wrong); err != nil {
			return err
		}
		return txn.Set(labelCountReadyKey, []byte{1})
	}))
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	count, err := engine2.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	count, err = engine2.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	ready, err := engine2.labelCountReady()
	require.NoError(t, err)
	require.True(t, ready)
}
