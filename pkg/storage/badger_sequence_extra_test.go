package storage

import (
	"encoding/binary"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_MVCCSequenceLoadInitializeAndPersist(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	t.Run("loadPersistedMVCCSequence missing key returns zero", func(t *testing.T) {
		seq, err := engine.loadPersistedMVCCSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(0), seq)
	})

	t.Run("loadPersistedMVCCSequence invalid length errors", func(t *testing.T) {
		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(mvccSequenceKey(), []byte{0x01, 0x02, 0x03})
		}))
		seq, err := engine.loadPersistedMVCCSequence()
		require.Error(t, err)
		require.Equal(t, uint64(0), seq)
		require.Contains(t, err.Error(), "invalid mvcc sequence length")
	})

	t.Run("initializeMVCCSequence reads persisted seed", func(t *testing.T) {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 42)
		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(mvccSequenceKey(), buf)
		}))
		require.NoError(t, engine.initializeMVCCSequence())
		require.Equal(t, uint64(42), engine.mvccLegacyGlobalSeed)
	})
}

func TestNewBadgerEngineWithOptions_ValidEncryptionKey(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		InMemory:      true,
		EncryptionKey: []byte("1234567890abcdef"),
	}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NotNil(t, engine)
	require.NoError(t, engine.Close())
}
