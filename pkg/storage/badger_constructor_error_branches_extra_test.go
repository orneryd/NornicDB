package storage

import (
	"bytes"
	"encoding/gob"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type badgerNoopLogger struct{}

func (badgerNoopLogger) Errorf(string, ...interface{})   {}
func (badgerNoopLogger) Warningf(string, ...interface{}) {}
func (badgerNoopLogger) Infof(string, ...interface{})    {}
func (badgerNoopLogger) Debugf(string, ...interface{})   {}

func seedBadgerDir(t *testing.T, seed func(txn *badger.Txn) error) string {
	t.Helper()
	dir := t.TempDir()
	db, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
	if db != nil {
		fixtureowner.Own(t, db)
	}
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return seed(txn)
	}))
	require.NoError(t, db.Close())
	return dir
}

func TestNewBadgerEngineWithOptions_ConstructorErrorBranches(t *testing.T) {
	t.Run("badger internal logger option path", func(t *testing.T) {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
			InMemory:             true,
			BadgerInternalLogger: badgerNoopLogger{},
		}))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		require.NoError(t, engine.Close())
	})

	t.Run("data dir points to file", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o644))
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: filePath}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open BadgerDB")
	})

	t.Run("serializer detection error from malformed header", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			badHeader := append([]byte(serializationMagic), byte(99), serializerIDMsgpack)
			return txn.Set(nodeKey(NodeID("test:n1")), badHeader)
		})
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "detecting storage serializer")
	})

	t.Run("legacy gob detection warning branch still opens", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			var buf bytes.Buffer
			legacy := &Node{ID: NodeID("test:n2"), Labels: []string{"L"}}
			if err := gob.NewEncoder(&buf).Encode(legacy); err != nil {
				return err
			}
			return txn.Set(nodeKey(NodeID("test:n2")), buf.Bytes())
		})
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: true}))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		require.NoError(t, engine.Close())
	})

	t.Run("id dictionary load failure", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			// Wrong size; idDictionary.loadFromBadger expects exactly 8 bytes.
			return txn.Set(nodeIDForwardKey(NodeID("test:bad-dict")), []byte{0x01})
		})
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load id dictionary")
	})

	t.Run("property key dictionary load failure", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			// Empty varint payload => malformed property-key forward value.
			return txn.Set(propKeyForwardKey("test", "name"), []byte{})
		})
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load property key dictionary")
	})

	t.Run("persisted schema decode failure", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			return txn.Set(schemaKey("test"), []byte("not-msgpack-schema"))
		})
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load persisted schema")
	})

	t.Run("label count initialization failure on malformed count", func(t *testing.T) {
		dir := seedBadgerDir(t, func(txn *badger.Txn) error {
			// Mark label counts as ready so ensureLabelCounts tries to decode persisted values.
			if err := txn.Set([]byte{prefixMVCCMeta, prefixMVCCMetaLabelCountReady}, []byte{1}); err != nil {
				return err
			}
			// Malformed count payload (must be 8 bytes).
			return txn.Set(labelCountKey("test", "person"), []byte{0x01})
		})
		_, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize label counts")
	})
}

func TestBadgerEngine_ReopenForPostMigrationCompaction_ErrorBranches(t *testing.T) {
	t.Run("reopen db fails with invalid data dir option", func(t *testing.T) {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o644))
		_, err = engine.reopenForPostMigrationCompaction(badger.DefaultOptions(filePath).WithLogger(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "reopen DB")
	})

	t.Run("reload id dictionary fails after reopen", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(nodeIDForwardKey(NodeID("test:bad-reload-id-dict")), []byte{0x01})
		}))

		_, err = engine.reopenForPostMigrationCompaction(badger.DefaultOptions(dir).WithLogger(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "reload id dictionary")
	})

	t.Run("reload property key dictionary fails after reopen", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(propKeyForwardKey("test", "bad-reload-prop-dict"), []byte{})
		}))

		_, err = engine.reopenForPostMigrationCompaction(badger.DefaultOptions(dir).WithLogger(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "reload property-key dictionary")
	})

	t.Run("reinitialize mvcc sequence fails after reopen", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(mvccSequenceKey(), []byte{0x01})
		}))
		_, err = engine.reopenForPostMigrationCompaction(badger.DefaultOptions(dir).WithLogger(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "reinitialize mvcc sequence")
	})

}
