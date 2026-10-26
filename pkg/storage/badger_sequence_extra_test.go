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

	t.Run("persistMVCCSequence branch behavior", func(t *testing.T) {
		// Empty namespace short-circuits.
		engine.persistMVCCSequence("", standalonePublication)

		state, err := engine.namespaceMVCC("test")
		require.NoError(t, err)

		// Zero sequence short-circuits (no key written).
		state.seq.Store(0)
		engine.persistMVCCSequence("test", standalonePublication)
		err = engine.db.View(func(txn *badger.Txn) error {
			_, gErr := txn.Get(state.persistKey)
			require.ErrorIs(t, gErr, badger.ErrKeyNotFound)
			return nil
		})
		require.NoError(t, err)

		// Non-zero sequence persists value.
		state.seq.Store(77)
		engine.persistMVCCSequence("test", standalonePublication)
		err = engine.db.View(func(txn *badger.Txn) error {
			item, gErr := txn.Get(state.persistKey)
			require.NoError(t, gErr)
			return item.Value(func(val []byte) error {
				require.Len(t, val, 8)
				require.Equal(t, uint64(77), binary.BigEndian.Uint64(val))
				return nil
			})
		})
		require.NoError(t, err)
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
