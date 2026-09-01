package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireStorageClientLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestStorageClientReceiptValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		create    func() error
		messageID localization.MessageID
		text      string
	}{
		{
			name: "transaction ID required",
			create: func() error {
				_, err := NewReceipt("", 1, 1, "neo4j", time.Time{})
				return err
			},
			messageID: localization.MessageStorageClientReceiptTransactionIDRequired,
			text:      "receipt: tx_id is required",
		},
		{
			name: "WAL sequence required",
			create: func() error {
				_, err := NewReceipt("tx-1", 0, 1, "neo4j", time.Time{})
				return err
			},
			messageID: localization.MessageStorageClientReceiptWALSequenceRequired,
			text:      "receipt: wal sequence must be non-zero",
		},
		{
			name: "WAL range invalid",
			create: func() error {
				_, err := NewReceipt("tx-1", 9, 4, "neo4j", time.Time{})
				return err
			},
			messageID: localization.MessageStorageClientReceiptWALRangeInvalid,
			text:      "receipt: wal_seq_end (4) < wal_seq_start (9)",
		},
		{
			name: "nil receipt",
			create: func() error {
				var receipt *Receipt
				return receipt.UpdateHash()
			},
			messageID: localization.MessageStorageClientReceiptNilReceiver,
			text:      "receipt: nil receiver",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireStorageClientLocalizedError(t, test.create(), test.messageID, test.text)
		})
	}
}

func TestStorageClientNodeNamespaceValidation(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	tests := []struct {
		name      string
		operation func() error
		messageID localization.MessageID
		text      string
	}{
		{
			name: "create",
			operation: func() error {
				_, err := engine.CreateNode(&Node{ID: "node-1"})
				return err
			},
			messageID: localization.MessageStorageClientNodeIDNamespaceUnprefixed,
			text:      "node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: node-1",
		},
		{
			name:      "update",
			operation: func() error { return engine.UpdateNode(&Node{ID: "node-2"}) },
			messageID: localization.MessageStorageClientNodeIDNamespaceUnprefixed,
			text:      "node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: node-2",
		},
		{
			name:      "bulk",
			operation: func() error { return engine.BulkCreateNodes([]*Node{{ID: "node-3"}}) },
			messageID: localization.MessageStorageClientNodeIDNamespaceRequired,
			text:      "node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: node-3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			localizedErr := requireStorageClientLocalizedError(t, test.operation(), test.messageID, test.text)
			require.NotEmpty(t, localizedErr.Message.Data["NodeID"])
		})
	}
}

func TestStorageClientAsyncFlushAggregateError(t *testing.T) {
	inner := newErrorEngine()
	inner.failNodeIDs["fail"] = true
	engine := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "fail", Labels: []string{"Test"}})
	require.NoError(t, err)
	err = engine.Flush()
	localizedErr := requireStorageClientLocalizedError(t, err, localization.MessageStorageClientAsyncFlushIncompleteDetailed, "flush incomplete: 1 nodes failed, 0 edges failed, 0 deletes failed (simulated node update failure)")
	require.Equal(t, 1, localizedErr.Message.Data["NodesFailed"])
	require.Equal(t, 0, localizedErr.Message.Data["EdgesFailed"])
	require.Equal(t, 0, localizedErr.Message.Data["DeletesFailed"])
	require.Equal(t, "simulated node update failure", localizedErr.Message.Data["Details"])
}

func TestStorageClientBackupAndDeletePrefixErrorsPreserveCauses(t *testing.T) {
	t.Run("backup create file", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, engine.Close()) })

		path := filepath.Join(t.TempDir(), "missing", "backup.bin")
		err = engine.Backup(path)
		localizedErr := requireStorageClientLocalizedError(t, err, localization.MessageStorageClientBackupFileCreateFailed, "failed to create backup file: open "+filepath.Dir(path)+": no such file or directory")
		require.Equal(t, path, localizedErr.Message.Data["Path"])
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
	})

	t.Run("closed backup", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		require.NoError(t, engine.Close())

		err = engine.Backup(filepath.Join(t.TempDir(), "backup.bin"))
		requireStorageClientLocalizedError(t, err, localization.MessageStorageClientStorageClosed, "storage closed")
		require.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("empty delete prefix", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, engine.Close()) })

		_, _, err = engine.DeleteByPrefix("")
		requireStorageClientLocalizedError(t, err, localization.MessageStorageClientDeletePrefixRequired, "prefix cannot be empty")
	})

	t.Run("drop prefix cause", func(t *testing.T) {
		cause := errors.New("drop failed")
		err := localizedError(localization.StorageClientDropPrefixFailed(0x6e, cause), cause)
		localizedErr := requireStorageClientLocalizedError(t, err, localization.MessageStorageClientDropPrefixFailed, "failed to drop prefix 6e: drop failed")
		require.Equal(t, byte(0x6e), localizedErr.Message.Data["Prefix"])
		require.ErrorIs(t, err, cause)
	})
}

func TestStorageClientCatalogsRenderCountsIDsAndCauses(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	message := localization.StorageClientAsyncFlushIncompleteDetailed(2, 3, 4, "node tenant:42 failed")
	require.Equal(t, 2, message.Data["NodesFailed"])
	require.Equal(t, 3, message.Data["EdgesFailed"])
	require.Equal(t, 4, message.Data["DeletesFailed"])
	require.Equal(t, "node tenant:42 failed", message.Data["Details"])

	spanish, _, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, "vaciado incompleto: fallaron 2 nodos, 3 relaciones y 4 eliminaciones (node tenant:42 failed)", spanish)

	pseudo, _, err := manager.Render(localization.WithPreferences(context.Background(), language.MustParse("en-XA")), localization.StorageClientNodeIDNamespaceRequired("tenant:42"))
	require.NoError(t, err)
	require.Equal(t, "[!! node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: tenant:42 !!]", pseudo)
}
