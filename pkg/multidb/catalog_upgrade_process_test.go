package multidb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This child uses the real native opener and canonical transformer. The signal
// is after cryptographic preparation, inside the uncommitted native transaction.
func TestCatalogV4ProcessChild(t *testing.T) {
	dir := os.Getenv("NORNIC_TEST_V4_PROCESS_DIR")
	if dir == "" {
		t.Skip("isolated child only")
	}
	plan, err := NewCatalogUpgradeV4("system", "synthetic-process-upgrade-key")
	require.NoError(t, err)
	if os.Getenv("NORNIC_TEST_V4_PROCESS_PHASE") == "prepared" {
		transform := plan.Transform
		plan.Transform = func(ctx context.Context, data string) (string, bool, error) {
			result, changed, err := transform(ctx, data)
			if err != nil {
				return result, changed, err
			}
			if err := os.WriteFile(filepath.Join(dir, "prepared"), []byte("ready"), 0600); err != nil {
				return "", false, err
			}
			select {} // Parent kills this isolated process before the transaction commits.
		}
	}
	_, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, AllowStorageUpgrade: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	// Deliberately omit Close: prove committed recovery after abrupt process exit.
	os.Exit(0)
}

func TestCatalogV4ProcessInterruptionAndReopen(t *testing.T) {
	dir := t.TempDir()
	inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	cfg := DefaultConfig()
	manager, err := NewDatabaseManager(t.Context(), inner, cfg)
	require.NoError(t, err)
	ref := ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://synthetic-user:synthetic-process-password@127.0.0.1:1?token=enc:v1:literal"}
	manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{ref}}
	system := storage.NewNamespacedEngine(inner, "system")
	node, err := system.GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	legacy, err := json.Marshal(manager.databases)
	require.NoError(t, err)
	node.Properties["data"] = string(legacy)
	require.NoError(t, system.UpdateNode(storage.NodeUpdateRequest{After: node}))
	require.NoError(t, manager.Close())
	markClosedASCIICatalogFixtureV3(t, dir)
	before := closedCatalogFixtureDigest(t, dir)
	child := func(phase string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCatalogV4ProcessChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), "NORNIC_TEST_V4_PROCESS_DIR="+dir, "NORNIC_TEST_V4_PROCESS_PHASE="+phase)
		return cmd
	}
	cmd := child("prepared")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "prepared")); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "child did not reach uncommitted transform")
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	// Badger must replay its process-crash WAL before read-only inspection.
	recoverNative := func() {
		db, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
	recoverNative()
	require.Equal(t, before, closedCatalogFixtureDigest(t, dir), "interrupted transform changed logical native data")
	require.NoError(t, child("committed").Run())
	recoverNative()
	raw, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil).WithReadOnly(true)))
	require.NoError(t, err)
	require.NoError(t, raw.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte{0x10, 0x02})
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(4), binary.BigEndian.Uint64(value))
		return nil
	}))
	require.NoError(t, raw.Close())
	inner, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	cfg.RemoteCredentialEncryptionKey = "synthetic-process-upgrade-key"
	manager, err = NewDatabaseManager(t.Context(), inner, cfg)
	require.NoError(t, err)
	defer manager.Close()
	refs, err := manager.GetCompositeConstituents("legacy")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.NotEmpty(t, refs[0].ConnectionV1)
	decoded, err := manager.openRemoteConnection(refs[0])
	require.NoError(t, err)
	require.Equal(t, ref.URI, decoded.uri)
	node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	require.NotContains(t, node.Properties["data"], "synthetic-process-password")
}
