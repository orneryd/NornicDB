package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type walStreamErrorEngine struct {
	Engine
	nodeErr error
	edgeErr error
}

type walPrefixErrorEngine struct {
	Engine
	nodeErr error
	edgeErr error
}

type walPrefixStatsEngine struct {
	Engine
	nodeCount int64
	edgeCount int64
}

type walSchemaProviderEngine struct {
	Engine
	schema *SchemaManager
}

type walStreamingCountEngine struct {
	Engine
	nodes []*Node
	edges []*Edge
}

type walPrefixStreamingEngine struct {
	Engine
	nodes             []*Node
	streamNodesCalls  int
	streamPrefixCalls int
	lastPrefix        string
}

type walNamespaceListerEngine struct {
	Engine
	namespaces []string
}

type walEmbeddingDispatchEngine struct {
	Engine
	updateNodeCalls      int
	updateEmbeddingCalls int
	updateNodeErr        error
	updateEmbeddingErr   error
}

type walCaptureLogger struct {
	entries []map[string]any
}

type walErrWriter struct {
	err error
}

func (w *walErrWriter) Write(p []byte) (int, error) {
	if w.err == nil {
		w.err = errors.New("writer failure")
	}
	return 0, w.err
}

func (e *walStreamErrorEngine) StreamNodes(_ context.Context, _ func(*Node) error) error {
	return e.nodeErr
}

func (e *walStreamErrorEngine) StreamEdges(_ context.Context, _ func(*Edge) error) error {
	return e.edgeErr
}

func (e *walStreamErrorEngine) StreamNodeChunks(_ context.Context, chunkSize int, fn func([]*Node) error) error {
	if e.nodeErr != nil {
		return e.nodeErr
	}
	return fn(nil)
}

func (e *walPrefixErrorEngine) NodeCountByPrefix(prefix string) (int64, error) {
	return 0, e.nodeErr
}

func (e *walPrefixErrorEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	return 0, e.edgeErr
}

func (e *walPrefixStatsEngine) NodeCountByPrefix(prefix string) (int64, error) {
	return e.nodeCount, nil
}

func (e *walPrefixStatsEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	return e.edgeCount, nil
}

func (e *walSchemaProviderEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	return e.schema
}

func (e *walSchemaProviderEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := e.Engine.(interface{ NodeCountByPrefix(string) (int64, error) }); ok {
		return stats.NodeCountByPrefix(prefix)
	}
	return 0, nil
}

func (e *walSchemaProviderEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := e.Engine.(interface{ EdgeCountByPrefix(string) (int64, error) }); ok {
		return stats.EdgeCountByPrefix(prefix)
	}
	return 0, nil
}

func (e *walStreamingCountEngine) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	for _, node := range e.nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *walStreamingCountEngine) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	for _, edge := range e.edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *walStreamingCountEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(e.nodes); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		end := i + chunkSize
		if end > len(e.nodes) {
			end = len(e.nodes)
		}
		if err := fn(e.nodes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *walPrefixStreamingEngine) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	e.streamNodesCalls++
	for _, node := range e.nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *walPrefixStreamingEngine) StreamEdges(_ context.Context, _ func(*Edge) error) error {
	return nil
}

func (e *walPrefixStreamingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(e.nodes); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		end := i + chunkSize
		if end > len(e.nodes) {
			end = len(e.nodes)
		}
		if err := fn(e.nodes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *walPrefixStreamingEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	e.streamPrefixCalls++
	e.lastPrefix = prefix
	for _, node := range e.nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *walNamespaceListerEngine) ListNamespaces() []string {
	return append([]string(nil), e.namespaces...)
}

func (e *walEmbeddingDispatchEngine) UpdateNode(node *Node) error {
	e.updateNodeCalls++
	if e.updateNodeErr != nil {
		return e.updateNodeErr
	}
	return e.Engine.UpdateNode(node)
}

func (e *walEmbeddingDispatchEngine) UpdateNodeEmbedding(node *Node) error {
	e.updateEmbeddingCalls++
	if e.updateEmbeddingErr != nil {
		return e.updateEmbeddingErr
	}
	if updater, ok := e.Engine.(interface{ UpdateNodeEmbedding(*Node) error }); ok {
		return updater.UpdateNodeEmbedding(node)
	}
	return nil
}

func (l *walCaptureLogger) Log(level, msg string, fields map[string]any) {
	entry := map[string]any{
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		entry[k] = v
	}
	l.entries = append(l.entries, entry)
}

func buildAtomicWALRecord(t *testing.T, entry WALEntry) []byte {
	t.Helper()

	payload, err := json.Marshal(entry)
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = writeAtomicRecordV2(&buf, payload)
	require.NoError(t, err)

	return buf.Bytes()
}

func TestNewWAL(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("creates_wal_with_default_config", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		assert.NotNil(t, wal)
		assert.Equal(t, dir, wal.config.Dir)
		assert.Equal(t, "batch", wal.config.SyncMode)
	})

	t.Run("creates_wal_with_custom_config", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:               dir,
			SyncMode:          "immediate",
			BatchSyncInterval: 50 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		assert.Equal(t, "immediate", wal.config.SyncMode)
	})

	t.Run("creates_directory_if_not_exists", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", "wal", "dir")
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		_, err = os.Stat(dir)
		assert.NoError(t, err)
	})

	t.Run("repairs_incomplete_tail_on_startup", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:      dir,
			SyncMode: "none", // deterministic tests; only flushes bufio.Writer
		}

		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		// Write two entries and flush to disk.
		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1"}}))
		require.NoError(t, wal.Sync())
		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n2"}}))
		require.NoError(t, wal.Sync())
		require.NoError(t, wal.Close())

		walPath := filepath.Join(dir, "wal.log")
		fi, err := os.Stat(walPath)
		require.NoError(t, err)
		require.Greater(t, fi.Size(), int64(3))

		// Simulate a crash mid-write by truncating the file tail (torn record).
		require.NoError(t, os.Truncate(walPath, fi.Size()-3))

		// Reopen WAL: startup repair should truncate the incomplete record before appending.
		wal2, err := NewWAL("", cfg)
		require.NoError(t, err)

		// The second entry was torn and should have been discarded by tail repair.
		require.Equal(t, uint64(1), wal2.Sequence())

		// Appending should work and produce a readable WAL.
		require.NoError(t, wal2.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n3"}}))
		require.NoError(t, wal2.Sync())
		require.NoError(t, wal2.Close())

		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, uint64(1), entries[0].Sequence)
		require.Equal(t, uint64(2), entries[1].Sequence)
	})
}

func TestWALTailRepairHelpers(t *testing.T) {
	makeNodeData := func(id NodeID) json.RawMessage {
		t.Helper()
		data, err := json.Marshal(WALNodeData{Node: &Node{ID: id, Labels: []string{"Test"}}})
		require.NoError(t, err)
		return data
	}

	makeEntry := func(seq uint64, data json.RawMessage) WALEntry {
		return WALEntry{
			Sequence:  seq,
			Timestamp: time.Now(),
			Operation: OpCreateNode,
			Data:      data,
			Checksum:  crc32Checksum(data),
		}
	}

	t.Run("repair wal tail no-op cases", func(t *testing.T) {
		repaired, diag, err := repairWALTailIfNeeded("", nil)
		require.NoError(t, err)
		assert.False(t, repaired)
		assert.Nil(t, diag)

		missingPath := filepath.Join(t.TempDir(), "missing.log")
		repaired, diag, err = repairWALTailIfNeeded(missingPath, nil)
		require.NoError(t, err)
		assert.False(t, repaired)
		assert.Nil(t, diag)

		emptyPath := filepath.Join(t.TempDir(), "empty.log")
		require.NoError(t, os.WriteFile(emptyPath, nil, 0644))
		repaired, diag, err = repairWALTailIfNeeded(emptyPath, nil)
		require.NoError(t, err)
		assert.False(t, repaired)
		assert.Nil(t, diag)

		legacyPath := filepath.Join(t.TempDir(), "legacy.log")
		require.NoError(t, os.WriteFile(legacyPath, []byte(`{"legacy":true}`), 0644))
		repaired, diag, err = repairWALTailIfNeeded(legacyPath, nil)
		require.NoError(t, err)
		assert.False(t, repaired)
		assert.Nil(t, diag)
	})

	t.Run("repair truncates incomplete header to zero", func(t *testing.T) {
		walPath := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte{0x57, 0x41}, 0644))

		logger := &walCaptureLogger{}
		repaired, diag, err := repairWALTailIfNeeded(walPath, logger)
		require.NoError(t, err)
		require.True(t, repaired)
		require.NotNil(t, diag)
		assert.Equal(t, "truncate_incomplete_tail", diag.RecoveryAction)
		assert.Len(t, logger.entries, 1)

		fi, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.Zero(t, fi.Size())
	})

	t.Run("truncate wal file truncates logs and no-op when already short enough", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte("123456789"), 0644))

		repaired, diag, err := truncateWALFile(walPath, 99, 3, "crc_mismatch", nil)
		require.NoError(t, err)
		assert.False(t, repaired)
		assert.Nil(t, diag)

		logger := &walCaptureLogger{}
		repaired, diag, err = truncateWALFile(walPath, -5, 7, "crc_mismatch", logger)
		require.NoError(t, err)
		require.True(t, repaired)
		require.NotNil(t, diag)
		assert.Equal(t, uint64(8), diag.CorruptedSeq)
		assert.Equal(t, "truncate_corrupt_tail", diag.RecoveryAction)
		assert.Equal(t, "crc_mismatch", diag.Operation)
		assert.Len(t, logger.entries, 1)
		assert.Equal(t, int64(0), logger.entries[0]["truncate_offset"])

		fi, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.Zero(t, fi.Size())
	})

	t.Run("scan atomic wal for repair detects tail corruption reasons", func(t *testing.T) {
		first := buildAtomicWALRecord(t, makeEntry(1, makeNodeData("n1")))

		validSecond := buildAtomicWALRecord(t, makeEntry(2, makeNodeData("n2")))
		payloadLen := int(binary.LittleEndian.Uint32(validSecond[5:9]))
		crcOffset := 9 + payloadLen
		trailerOffset := crcOffset + 4

		invalidJSONSecond := func() []byte {
			var buf bytes.Buffer
			_, err := writeAtomicRecordV2(&buf, []byte(`{"broken"`))
			require.NoError(t, err)
			return buf.Bytes()
		}

		dataChecksumMismatch := makeEntry(2, makeNodeData("n2"))
		dataChecksumMismatch.Checksum++

		cases := []struct {
			name   string
			record []byte
			reason string
		}{
			{
				name: "invalid magic",
				record: func() []byte {
					b := append([]byte(nil), validSecond...)
					binary.LittleEndian.PutUint32(b[0:4], 0xDEADBEEF)
					return b
				}(),
				reason: "invalid_magic",
			},
			{
				name:   "unsupported version",
				record: func() []byte { b := append([]byte(nil), validSecond...); b[4] = walFormatVersion + 1; return b }(),
				reason: "unsupported_version",
			},
			{
				name: "invalid payload size",
				record: func() []byte {
					b := make([]byte, 9)
					binary.LittleEndian.PutUint32(b[0:4], walMagic)
					b[4] = walFormatVersion
					binary.LittleEndian.PutUint32(b[5:9], walMaxEntrySize+1)
					return b
				}(),
				reason: "invalid_payload_size",
			},
			{
				name:   "crc mismatch",
				record: func() []byte { b := append([]byte(nil), validSecond...); b[crcOffset] ^= 0xFF; return b }(),
				reason: "crc_mismatch",
			},
			{
				name:   "invalid trailer",
				record: func() []byte { b := append([]byte(nil), validSecond...); b[trailerOffset] ^= 0xFF; return b }(),
				reason: "invalid_trailer",
			},
			{
				name:   "invalid json payload",
				record: invalidJSONSecond(),
				reason: "invalid_json",
			},
			{
				name:   "inner data checksum mismatch",
				record: buildAtomicWALRecord(t, dataChecksumMismatch),
				reason: "data_checksum_mismatch",
			},
			{
				name:   "incomplete tail",
				record: validSecond[:len(validSecond)-2],
				reason: "incomplete_tail",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "wal.log")
				data := append(append([]byte(nil), first...), tc.record...)
				require.NoError(t, os.WriteFile(path, data, 0644))

				f, err := os.Open(path)
				require.NoError(t, err)
				defer f.Close()

				truncateOffset, lastGoodSeq, reason, ok, err := scanAtomicWALForRepair(f, int64(len(data)))
				require.NoError(t, err)
				assert.False(t, ok)
				assert.Equal(t, int64(len(first)), truncateOffset)
				assert.Equal(t, uint64(1), lastGoodSeq)
				assert.Equal(t, tc.reason, reason)
			})
		}

		t.Run("healthy atomic wal returns ok", func(t *testing.T) {
			second := buildAtomicWALRecord(t, makeEntry(2, makeNodeData("n2")))
			data := append(append([]byte(nil), first...), second...)
			path := filepath.Join(t.TempDir(), "wal.log")
			require.NoError(t, os.WriteFile(path, data, 0644))

			f, err := os.Open(path)
			require.NoError(t, err)
			defer f.Close()

			truncateOffset, lastGoodSeq, reason, ok, err := scanAtomicWALForRepair(f, int64(len(data)))
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Zero(t, truncateOffset)
			assert.Equal(t, uint64(2), lastGoodSeq)
			assert.Empty(t, reason)
		})
	})
}

func TestWAL_Config(t *testing.T) {
	t.Run("nil wal returns nil config", func(t *testing.T) {
		var wal *WAL
		assert.Nil(t, wal.Config())
	})

	t.Run("returns configured wal config", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:                  dir,
			SyncMode:             "immediate",
			BatchSyncInterval:    25 * time.Millisecond,
			RetentionMaxSegments: 3,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		got := wal.Config()
		require.NotNil(t, got)
		assert.Equal(t, dir, got.Dir)
		assert.Equal(t, "immediate", got.SyncMode)
		assert.Equal(t, 3, got.RetentionMaxSegments)
	})
}

func TestWAL_InternalBranchErrors(t *testing.T) {
	t.Run("new wal returns expected filesystem errors", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0644))
		_, err := NewWAL(filePath, nil)
		require.ErrorContains(t, err, "failed to create directory")

		segRoot := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(segRoot, "segments"), []byte("x"), 0644))
		_, err = NewWAL(segRoot, nil)
		require.ErrorContains(t, err, "failed to create segments directory")

		openRoot := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(openRoot, "wal.log"), 0755))
		_, err = NewWAL(openRoot, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "wal")
	})

	t.Run("append marshal and sync helper errors", func(t *testing.T) {
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		defer wal.Close()

		cycle := map[string]any{}
		cycle["self"] = cycle
		_, err = marshalJSONCompact(bytes.NewBuffer(nil), cycle)
		require.Error(t, err)

		bad := &WAL{
			config: &WALConfig{SyncMode: "none"},
			writer: bufio.NewWriterSize(&walErrWriter{err: errors.New("flush boom")}, 16),
		}
		_, _ = bad.writer.Write([]byte("trigger flush"))
		require.ErrorContains(t, bad.syncLocked(), "flush failed")

	})

	t.Run("rotation guard and failure branches", func(t *testing.T) {
		cfg := &WALConfig{Dir: t.TempDir(), SyncMode: "none", MaxFileSize: 8}
		w := &WAL{
			config: cfg,
			writer: bufio.NewWriterSize(bytes.NewBuffer(nil), 16),
			file:   os.NewFile(^uintptr(0), "invalid"),
		}

		require.NoError(t, w.maybeRotateLocked(1))   // segmentEntries == 0 fast path
		require.NoError(t, w.rotateSegmentLocked(1)) // segmentEntries == 0 fast path

		w.segmentEntries = 1
		w.segmentBytes = 10
		err := w.maybeRotateLocked(2)
		require.Error(t, err)
	})

	t.Run("loadLastSequence tolerates malformed manifest and returns zero sequence", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, writeWALManifest(dir, &WALManifest{
			Version: walManifestVersion,
			Segments: []WALSegment{
				{FirstSeq: 1, LastSeq: 1, Path: "../bad-segment.wal"},
			},
		}))

		w := &WAL{config: &WALConfig{Dir: dir}}
		seq, err := w.loadLastSequence()
		require.NoError(t, err)
		require.Zero(t, seq)
	})
}

func TestWAL_Append(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("appends_entries_when_enabled", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:      dir,
			SyncMode: "none",
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		node := &Node{
			ID:     "test-node",
			Labels: []string{"Test"},
		}

		err = wal.Append(OpCreateNode, WALNodeData{Node: node})
		require.NoError(t, err)

		assert.Equal(t, uint64(1), wal.Sequence())
		stats := wal.Stats()
		assert.Equal(t, int64(1), stats.TotalWrites)
	})

	t.Run("append_returns_sequence", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		seq1, err := wal.AppendReturningSeq(OpCreateNode, WALNodeData{Node: &Node{ID: "seq-1"}})
		require.NoError(t, err)
		seq2, err := wal.AppendReturningSeq(OpCreateNode, WALNodeData{Node: &Node{ID: "seq-2"}})
		require.NoError(t, err)

		assert.Equal(t, uint64(1), seq1)
		assert.Equal(t, uint64(2), seq2)
		assert.Equal(t, uint64(2), wal.Sequence())
	})

	t.Run("append_tx_markers_return_sequences", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		beginSeq, err := wal.AppendTxBegin("testdb", "tx-1", map[string]string{"source": "test"})
		require.NoError(t, err)
		commitSeq, err := wal.AppendTxCommit("testdb", "tx-1", 2)
		require.NoError(t, err)
		abortSeq, err := wal.AppendTxAbort("testdb", "tx-2", "test abort")
		require.NoError(t, err)

		assert.Equal(t, uint64(1), beginSeq)
		assert.Equal(t, uint64(2), commitSeq)
		assert.Equal(t, uint64(3), abortSeq)

		entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
		require.NoError(t, err)
		require.Len(t, entries, 3)
		assert.Equal(t, OpTxBegin, entries[0].Operation)
		assert.Equal(t, OpTxCommit, entries[1].Operation)
		assert.Equal(t, OpTxAbort, entries[2].Operation)

		var beginData WALTxData
		require.NoError(t, json.Unmarshal(entries[0].Data, &beginData))
		assert.Equal(t, "tx-1", beginData.TxID)
	})

	t.Run("skips_when_wal_disabled", func(t *testing.T) {
		config.DisableWAL()
		defer config.EnableWAL()

		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		assert.NoError(t, err) // Should succeed but not write

		// Sequence should still increment (the append was called but skipped internally)
		// Actually with WAL disabled, Append returns early so sequence doesn't increment
	})

	t.Run("returns_error_when_closed", func(t *testing.T) {
		config.EnableWAL()

		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		assert.Equal(t, ErrWALClosed, err)
	})

	t.Run("increments_sequence_monotonically", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		for i := 0; i < 100; i++ {
			err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
			require.NoError(t, err)
		}

		assert.Equal(t, uint64(100), wal.Sequence())
	})
}

func TestWAL_Sync(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("immediate_sync_mode", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		require.NoError(t, err)

		stats := wal.Stats()
		assert.GreaterOrEqual(t, stats.TotalSyncs, int64(1))
	})

	t.Run("manual_sync", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		require.NoError(t, err)

		err = wal.Sync()
		assert.NoError(t, err)
	})

	t.Run("sync_returns_error_when_closed", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		wal.Close()

		err = wal.Sync()
		assert.Equal(t, ErrWALClosed, err)
	})
}

func TestWAL_Checkpoint(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Add some entries
	for i := 0; i < 5; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}

	// Create checkpoint
	err = wal.Checkpoint()
	require.NoError(t, err)

	assert.Equal(t, uint64(6), wal.Sequence()) // 5 creates + 1 checkpoint
}

func TestWAL_Close(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("closes_cleanly", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err)
		assert.True(t, wal.Stats().Closed)
	})

	t.Run("double_close_is_safe", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err) // Should not error on second close
	})
}

func TestWAL_Stats(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Initial stats
	stats := wal.Stats()
	assert.Equal(t, uint64(0), stats.Sequence)
	assert.False(t, stats.Closed)

	// After writes
	for i := 0; i < 10; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}

	stats = wal.Stats()
	assert.Equal(t, uint64(10), stats.Sequence)
	assert.Equal(t, int64(10), stats.TotalWrites)
}

func TestWAL_ReadEntries(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	// Write entries
	nodes := []*Node{
		{ID: "n1", Labels: []string{"A"}},
		{ID: "n2", Labels: []string{"B"}},
		{ID: "n3", Labels: []string{"C"}},
	}
	for _, n := range nodes {
		err = wal.Append(OpCreateNode, WALNodeData{Node: n})
		require.NoError(t, err)
	}
	wal.Close()

	// Read entries back
	walPath := filepath.Join(dir, "wal.log")
	entries, err := ReadWALEntries(walPath)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Equal(t, OpCreateNode, entries[0].Operation)
}

func TestWAL_ReadEntriesAfter(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	// Write 10 entries
	for i := 0; i < 10; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune('0'+i)))}})
	}
	wal.Close()

	// Read only entries after sequence 5
	walPath := filepath.Join(dir, "wal.log")
	entries, err := ReadWALEntriesAfter(walPath, 5)
	require.NoError(t, err)
	assert.Len(t, entries, 5) // Entries 6-10
	assert.Equal(t, uint64(6), entries[0].Sequence)
}

func TestCheckWALIntegrity(t *testing.T) {
	t.Run("missing wal is healthy", func(t *testing.T) {
		report, err := CheckWALIntegrity(filepath.Join(t.TempDir(), "missing.log"))
		require.NoError(t, err)
		require.NotNil(t, report)
		assert.True(t, report.Healthy)
		assert.Equal(t, "unknown", report.Format)
	})

	t.Run("empty wal is healthy", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(path, nil, 0644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotNil(t, report)
		assert.True(t, report.Healthy)
		assert.Equal(t, int64(0), report.FileSize)
	})

	t.Run("corrupted legacy wal is unhealthy", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		entry := WALEntry{
			Sequence:  1,
			Operation: OpCreateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "bad-crc"}}),
			Checksum:  123, // Deliberately wrong
		}
		payload, err := json.Marshal(entry)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotNil(t, report)
		assert.Equal(t, "legacy", report.Format)
		assert.False(t, report.Healthy)
		assert.Equal(t, 1, report.CorruptedEntries)
	})

	t.Run("legacy embedding corruption is skipped but counted", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		entry := WALEntry{
			Sequence:  1,
			Operation: OpUpdateEmbedding,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "embed-1"}}),
			Checksum:  0, // force mismatch
		}
		payload, err := json.Marshal(entry)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotNil(t, report)
		assert.Equal(t, "legacy", report.Format)
		assert.Equal(t, 1, report.CorruptedEntries)
		assert.Equal(t, 1, report.SkippedEmbeddings)
	})

	t.Run("legacy decode error is recorded in report", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		entry := WALEntry{
			Sequence:  1,
			Operation: OpCreateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "ok"}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)
		first, err := json.Marshal(entry)
		require.NoError(t, err)
		content := append(first, '\n')
		content = append(content, []byte("{bad-json")...)
		require.NoError(t, os.WriteFile(path, content, 0644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotNil(t, report)
		assert.NotEmpty(t, report.Errors)
		assert.Contains(t, strings.Join(report.Errors, " "), "JSON decode error")
	})
}

func TestWAL_ApplyRetention(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("closed wal returns error", func(t *testing.T) {
		wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		require.NoError(t, wal.Close())
		assert.Equal(t, ErrWALClosed, wal.ApplyRetention(1))
	})

	t.Run("removes old segments after snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.Dir = dir
		cfg.RetentionMaxSegments = 1
		cfg.RetentionMaxAge = 0

		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		segDir := walSegmentsDir(dir)
		oldSeg := WALSegment{
			FirstSeq:  1,
			LastSeq:   5,
			SizeBytes: 10,
			CreatedAt: time.Now().Add(-time.Hour),
			Path:      "seg-00000000000000000001-00000000000000000005.wal",
		}
		newerSeg := WALSegment{
			FirstSeq:  6,
			LastSeq:   10,
			SizeBytes: 10,
			CreatedAt: time.Now(),
			Path:      "seg-00000000000000000006-00000000000000000010.wal",
		}
		for _, seg := range []WALSegment{oldSeg, newerSeg} {
			require.NoError(t, os.WriteFile(filepath.Join(segDir, seg.Path), []byte("segment"), 0644))
		}
		require.NoError(t, writeWALManifest(dir, &WALManifest{
			Version:  walManifestVersion,
			Segments: []WALSegment{oldSeg, newerSeg},
		}))

		require.NoError(t, wal.ApplyRetention(10))

		_, err = os.Stat(filepath.Join(segDir, oldSeg.Path))
		assert.True(t, os.IsNotExist(err), "oldest segment should be pruned")

		_, err = os.Stat(filepath.Join(segDir, newerSeg.Path))
		assert.NoError(t, err, "newest retained segment should remain")

		manifest, err := loadWALManifest(dir)
		require.NoError(t, err)
		require.Len(t, manifest.Segments, 1)
		assert.Equal(t, newerSeg.Path, manifest.Segments[0].Path)
	})

	t.Run("invalid manifest json returns load error", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.Dir = dir
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		require.NoError(t, os.WriteFile(walManifestPath(dir), []byte("{bad-json"), 0644))
		err = wal.ApplyRetention(10)
		require.Error(t, err)
	})

	t.Run("zero snapshot sequence is no-op", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.Dir = dir
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()
		require.NoError(t, wal.ApplyRetention(0))
	})

	t.Run("retention respects age and max-segment limits together", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.Dir = dir
		cfg.RetentionMaxAge = 30 * time.Minute
		cfg.RetentionMaxSegments = 1

		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		now := time.Now()
		segDir := walSegmentsDir(dir)
		segments := []WALSegment{
			{FirstSeq: 1, LastSeq: 2, CreatedAt: now.Add(-2 * time.Hour), Path: "seg-1-2.wal"},
			{FirstSeq: 3, LastSeq: 4, CreatedAt: now.Add(-90 * time.Minute), Path: "seg-3-4.wal"},
			{FirstSeq: 5, LastSeq: 6, CreatedAt: now.Add(-10 * time.Minute), Path: "seg-5-6.wal"}, // kept by age
			{FirstSeq: 7, LastSeq: 8, CreatedAt: now.Add(-2 * time.Hour), Path: "seg-7-8.wal"},    // > snapshot kept
		}
		for _, seg := range segments {
			require.NoError(t, os.WriteFile(filepath.Join(segDir, seg.Path), []byte("segment"), 0644))
		}
		require.NoError(t, writeWALManifest(dir, &WALManifest{
			Version:  walManifestVersion,
			Segments: segments,
		}))

		require.NoError(t, wal.ApplyRetention(6))

		manifest, loadErr := loadWALManifest(dir)
		require.NoError(t, loadErr)
		var kept []string
		for _, seg := range manifest.Segments {
			kept = append(kept, seg.Path)
		}
		assert.Contains(t, kept, "seg-5-6.wal") // kept by age
		assert.Contains(t, kept, "seg-7-8.wal") // beyond snapshot
		assert.Contains(t, kept, "seg-3-4.wal") // newest remaining candidate due max segment=1
		assert.NotContains(t, kept, "seg-1-2.wal")
	})
}

func TestSnapshot_CreateAndLoad(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Create engine with data
	engine := NewMemoryEngine()
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2")), Type: "KNOWS"}))

	// Create snapshot
	snapshot, err := wal.CreateSnapshot(engine)
	require.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Len(t, snapshot.Nodes, 2)
	assert.Len(t, snapshot.Edges, 1)
	assert.Equal(t, "1.0", snapshot.Version)

	// Save snapshot
	snapshotPath := filepath.Join(dir, "snapshot.json")
	err = SaveSnapshot(snapshot, snapshotPath)
	require.NoError(t, err)

	// Load snapshot
	loaded, err := LoadSnapshot(snapshotPath)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Sequence, loaded.Sequence)
	assert.Len(t, loaded.Nodes, 2)
	assert.Len(t, loaded.Edges, 1)
}

func TestSnapshot_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")

	snapshot := &Snapshot{
		Sequence:  100,
		Timestamp: time.Now(),
		Nodes:     []*Node{{ID: "n1"}},
		Edges:     []*Edge{{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "TEST"}},
		Version:   "1.0",
	}

	err := SaveSnapshot(snapshot, snapshotPath)
	require.NoError(t, err)

	// Verify temp file doesn't exist
	_, err = os.Stat(snapshotPath + ".tmp")
	assert.True(t, os.IsNotExist(err))

	// Verify actual file exists
	_, err = os.Stat(snapshotPath)
	assert.NoError(t, err)
}

func TestReplayWALEntry(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("replay_create_node", func(t *testing.T) {
		baseEngine := NewMemoryEngine()
		engine := NewNamespacedEngine(baseEngine, "test")
		entry := WALEntry{
			Sequence:  1,
			Operation: OpCreateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		node, err := engine.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, node)
	})

	t.Run("replay_update_node", func(t *testing.T) {
		baseEngine := NewMemoryEngine()
		engine := NewNamespacedEngine(baseEngine, "test")
		engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})

		entry := WALEntry{
			Sequence:  2,
			Operation: OpUpdateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Updated"}}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		node, _ := engine.GetNode("n1")
		// Labels are normalized to lowercase during storage
		found := false
		for _, l := range node.Labels {
			if l == "updated" || l == "Updated" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should have Updated label")
	})

	t.Run("replay_delete_node", func(t *testing.T) {
		baseEngine := NewMemoryEngine()
		engine := NewNamespacedEngine(baseEngine, "test")
		engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})

		entry := WALEntry{
			Sequence:  3,
			Operation: OpDeleteNode,
			Data:      mustMarshal(WALDeleteData{ID: "n1"}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		_, err = engine.GetNode("n1")
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("replay_create_edge", func(t *testing.T) {
		baseEngine := NewMemoryEngine()
		engine := NewNamespacedEngine(baseEngine, "test")
		engine.CreateNode(&Node{ID: "n1"})
		engine.CreateNode(&Node{ID: "n2"})

		entry := WALEntry{
			Sequence:  4,
			Operation: OpCreateEdge,
			Data:      mustMarshal(WALEdgeData{Edge: &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		edge, err := engine.GetEdge("e1")
		assert.NoError(t, err)
		assert.NotNil(t, edge)
	})

	t.Run("replay_bulk_nodes", func(t *testing.T) {
		baseEngine := NewMemoryEngine()
		engine := NewNamespacedEngine(baseEngine, "test")
		nodes := []*Node{
			{ID: "b1", Labels: []string{"Bulk"}},
			{ID: "b2", Labels: []string{"Bulk"}},
		}

		entry := WALEntry{
			Sequence:  5,
			Operation: OpBulkNodes,
			Data:      mustMarshal(WALBulkNodesData{Nodes: nodes}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("replay_checkpoint_is_noop", func(t *testing.T) {
		engine := NewMemoryEngine()
		entry := WALEntry{
			Sequence:  6,
			Operation: OpCheckpoint,
			Data:      mustMarshal(map[string]interface{}{"time": time.Now()}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)
	})

	t.Run("replay_unknown_operation", func(t *testing.T) {
		engine := NewMemoryEngine()
		entry := WALEntry{
			Sequence:  7,
			Operation: "unknown_op",
			Data:      []byte("{}"),
		}

		err := ReplayWALEntry(engine, entry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown operation")
	})
}

func TestRecoverFromWAL(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("recovery_with_snapshot_and_wal", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		// Phase 1: Create initial state and snapshot
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Original"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Original"}})
		require.NoError(t, err)

		// Create and save snapshot
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		err = SaveSnapshot(snapshot, snapshotPath)
		require.NoError(t, err)

		// Phase 2: Add more changes after snapshot
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "n3", Labels: []string{"AfterSnapshot"}}}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OpUpdateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Modified"}}}, "test"))
		wal.Close()

		// Recover.
		recovered, err := RecoverFromWAL(walDir, snapshotPath)
		require.NoError(t, err)

		// Verify state (wrap recovered base engine with namespace used in WAL)
		recoveredNS := NewNamespacedEngine(recovered, "test")
		count, _ := recoveredNS.NodeCount()
		assert.Equal(t, int64(3), count)

		n1, _ := recoveredNS.GetNode("n1")
		// Labels are normalized to lowercase
		found := false
		for _, l := range n1.Labels {
			if l == "modified" || l == "Modified" {
				found = true
				break
			}
		}
		assert.True(t, found, "n1 should have Modified label")

		n3, _ := recoveredNS.GetNode("n3")
		assert.NotNil(t, n3)
	})

	t.Run("recovery_without_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "n2", Labels: []string{"Test"}}}, "test"))
		wal.Close()

		recovered, err := RecoverFromWAL(walDir, "")
		require.NoError(t, err)

		recoveredNS := NewNamespacedEngine(recovered, "test")
		count, _ := recoveredNS.NodeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("recovery_no_wal_file", func(t *testing.T) {
		dir := t.TempDir()

		// Create empty WAL directory structure
		walDir := filepath.Join(dir, "wal")
		os.MkdirAll(walDir, 0755)

		recovered, err := RecoverFromWAL(walDir, "")
		// If no WAL file exists, should still return empty engine (not error)
		require.NoError(t, err)

		count, _ := recovered.NodeCount()
		assert.Equal(t, int64(0), count) // Empty engine
	})

	t.Run("recovery_with_failed_replay_entries_still_returns_engine", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		// This update will fail during replay (node doesn't exist), which exercises
		// RecoverFromWAL's failed-entry logging path while still returning an engine.
		require.NoError(t, wal.AppendWithDatabase(OpUpdateNode, WALNodeData{
			Node: &Node{ID: "missing", Labels: []string{"Missing"}},
		}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{
			Node: &Node{ID: "ok-node", Labels: []string{"Test"}},
		}, "test"))
		require.NoError(t, wal.Close())

		recovered, err := RecoverFromWAL(walDir, "")
		require.NoError(t, err)
		recoveredNS := NewNamespacedEngine(recovered, "test")

		node, err := recoveredNS.GetNode("ok-node")
		require.NoError(t, err)
		require.NotNil(t, node)
	})
}

func TestRecoverFromWALWithResult_ErrorAndRoutingBranches(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("invalid snapshot payload returns load error", func(t *testing.T) {
		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot.json")
		require.NoError(t, os.WriteFile(snapshotPath, []byte("{invalid-json"), 0644))

		_, _, err := RecoverFromWALWithResult(filepath.Join(dir, "wal"), snapshotPath)
		require.ErrorContains(t, err, "failed to load snapshot")
	})

	t.Run("snapshot restore edge failures return explicit errors", func(t *testing.T) {
		dir := t.TempDir()

		edgeSnapshotPath := filepath.Join(dir, "snapshot-bad-edge.json")
		require.NoError(t, SaveSnapshot(&Snapshot{
			Sequence: 1, Timestamp: time.Now(), Version: "1.0",
			Edges: []*Edge{
				{ID: "tenant_x:e1", StartNode: "tenant_x:a", EndNode: "tenant_x:b", Type: "REL"},
			},
		}, edgeSnapshotPath))
		_, _, err := RecoverFromWALWithResult(filepath.Join(dir, "wal-edge"), edgeSnapshotPath)
		require.ErrorContains(t, err, "failed to restore edges")
	})

	t.Run("snapshot edge prefix selects namespace for restore", func(t *testing.T) {
		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot-prefixed.json")
		require.NoError(t, SaveSnapshot(&Snapshot{
			Sequence: 1, Timestamp: time.Now(), Version: "1.0",
			Nodes: []*Node{
				{ID: "tenant_pref:n1", Labels: []string{"Doc"}},
				{ID: "tenant_pref:n2", Labels: []string{"Doc"}},
			},
			Edges: []*Edge{
				{ID: "tenant_pref:e1", StartNode: "tenant_pref:n1", EndNode: "tenant_pref:n2", Type: "REL"},
			},
		}, snapshotPath))

		engine, result, err := RecoverFromWALWithResult(filepath.Join(dir, "wal"), snapshotPath)
		require.NoError(t, err)
		require.Equal(t, 0, result.Failed)

		ns := NewNamespacedEngine(engine, "tenant_pref")
		node, getErr := ns.GetNode("n1")
		require.NoError(t, getErr)
		require.NotNil(t, node)
	})

	t.Run("invalid manifest path bubbles as read wal error", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, writeWALManifest(dir, &WALManifest{
			Version: walManifestVersion,
			Segments: []WALSegment{
				{FirstSeq: 1, LastSeq: 1, Path: "../escape.wal"},
			},
		}))
		_, _, err := RecoverFromWALWithResult(dir, "")
		require.ErrorContains(t, err, "failed to read WAL")
	})

	t.Run("RecoverFromWAL returns wrapped error on invalid snapshot", func(t *testing.T) {
		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot-bad.json")
		require.NoError(t, os.WriteFile(snapshotPath, []byte("{bad-json"), 0644))
		_, err := RecoverFromWAL(filepath.Join(dir, "wal"), snapshotPath)
		require.ErrorContains(t, err, "failed to load snapshot")
	})

	t.Run("RecoverFromWAL succeeds even when replay has failed entries", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		wal, err := NewWAL("", &WALConfig{Dir: walDir, SyncMode: "immediate"})
		require.NoError(t, err)
		require.NoError(t, wal.AppendWithDatabase(OpUpdateNode, WALNodeData{
			Node: &Node{ID: "missing", Labels: []string{"Missing"}},
		}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OperationType("unsupported_op"), map[string]any{
			"id": "x",
		}, "test"))
		require.NoError(t, wal.Close())

		engine, err := RecoverFromWAL(walDir, "")
		require.NoError(t, err)
		require.NotNil(t, engine)
	})
}

func TestSaveSnapshot_ErrorPaths(t *testing.T) {
	t.Run("returns encode error for non-serializable data and cleans temp file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snapshots", "snapshot.json")
		snapshot := &Snapshot{
			Sequence:  1,
			Timestamp: time.Now(),
			Nodes: []*Node{
				{
					ID:     "n1",
					Labels: []string{"Test"},
					Properties: map[string]interface{}{
						"bad": make(chan int), // json cannot encode channel
					},
				},
			},
			Version: "1.0",
		}

		err := SaveSnapshot(snapshot, path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to encode snapshot")
		_, statErr := os.Stat(path + ".tmp")
		require.True(t, os.IsNotExist(statErr))
	})
}

func TestWALEngine(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("logs_and_executes_operations", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		base := NewMemoryEngine()
		defer base.Close()
		namespaced := NewNamespacedEngine(base, "test")
		walEngine := NewWALEngine(namespaced, wal)
		defer walEngine.Close()

		// Create node
		_, err = walEngine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
		require.NoError(t, err)

		// Verify node exists
		node, err := walEngine.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, node)

		// Verify WAL entry was created
		assert.Equal(t, uint64(1), wal.Sequence())
	})

	t.Run("all_operations_logged", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		base := NewMemoryEngine()
		defer base.Close()
		namespaced := NewNamespacedEngine(base, "test")
		walEngine := NewWALEngine(namespaced, wal)
		defer walEngine.Close()

		// Create nodes
		walEngine.CreateNode(&Node{ID: "n1"})
		walEngine.CreateNode(&Node{ID: "n2"})

		// Update node
		walEngine.UpdateNode(&Node{ID: "n1", Labels: []string{"Updated"}})

		// Create edge
		walEngine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})

		// Update edge
		walEngine.UpdateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "FRIENDS"})

		// Delete edge
		walEngine.DeleteEdge("e1")

		// Delete node
		walEngine.DeleteNode("n2")

		// Bulk create
		walEngine.BulkCreateNodes([]*Node{{ID: "b1"}, {ID: "b2"}})
		walEngine.BulkCreateEdges([]*Edge{{ID: "be1", StartNode: "n1", EndNode: "b1", Type: "TEST"}})

		// Verify sequence
		assert.Equal(t, uint64(9), wal.Sequence())
	})

	t.Run("read_operations_not_logged", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1"})
		engine.CreateNode(&Node{ID: "n2"})
		engine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "TEST"})

		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		// All read operations - should not increase sequence
		walEngine.GetNode("n1")
		walEngine.GetEdge("e1")
		walEngine.GetNodesByLabel("Test")
		walEngine.GetOutgoingEdges("n1")
		walEngine.GetIncomingEdges("n2")
		walEngine.GetEdgesBetween("n1", "n2")
		walEngine.GetEdgeBetween("n1", "n2", "TEST")
		walEngine.AllNodes()
		walEngine.AllEdges()
		walEngine.GetAllNodes()
		walEngine.GetInDegree("n2")
		walEngine.GetOutDegree("n1")
		walEngine.GetSchema()
		walEngine.NodeCount()
		walEngine.EdgeCount()

		assert.Equal(t, uint64(0), wal.Sequence())
	})

	t.Run("getters_return_underlying_components", func(t *testing.T) {
		dir := t.TempDir()
		wal, _ := NewWAL(dir, nil)
		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		assert.Same(t, wal, walEngine.GetWAL())
		assert.Same(t, engine, walEngine.GetEngine())
	})
}

func TestWALEngine_WithFeatureFlagDisabled(t *testing.T) {
	config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	engine := NewMemoryEngine()
	walEngine := NewWALEngine(engine, wal)
	defer walEngine.Close()

	// Operations should succeed but not log
	walEngine.CreateNode(&Node{ID: "n1"})
	walEngine.UpdateNode(&Node{ID: "n1", Labels: []string{"Updated"}})
	walEngine.DeleteNode("n1")

	// No entries should be written when disabled
	assert.Equal(t, uint64(0), wal.Sequence())

	// But operations should still execute
	_, err = walEngine.GetNode("n1")
	assert.Equal(t, ErrNotFound, err) // Deleted
}

func TestWAL_ConcurrentAppends(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(id*1000+j)))}})
			}
		}(i)
	}

	wg.Wait()

	expected := uint64(numGoroutines * entriesPerGoroutine)
	assert.Equal(t, expected, wal.Sequence())
}

func TestWAL_SequenceRestoration(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	// First WAL session
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal1, err := NewWAL("", cfg)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		wal1.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}
	wal1.Close()

	// Second WAL session - should continue from where we left off
	wal2, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal2.Close()

	// Sequence should be restored
	assert.Equal(t, uint64(50), wal2.Sequence())

	// New entries should continue from 51
	wal2.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n51"}})
	assert.Equal(t, uint64(51), wal2.Sequence())
}

func TestCrc32Checksum(t *testing.T) {
	data := []byte("test data for checksum")
	checksum1 := crc32Checksum(data)
	checksum2 := crc32Checksum(data)

	assert.Equal(t, checksum1, checksum2, "Same data should produce same checksum")

	differentData := []byte("different data")
	checksum3 := crc32Checksum(differentData)
	assert.NotEqual(t, checksum1, checksum3, "Different data should produce different checksum")
}

func TestDefaultWALConfig(t *testing.T) {
	cfg := DefaultWALConfig()

	assert.Equal(t, "data/wal", cfg.Dir)
	assert.Equal(t, "batch", cfg.SyncMode)
	assert.Equal(t, 100*time.Millisecond, cfg.BatchSyncInterval)
	assert.Equal(t, int64(100*1024*1024), cfg.MaxFileSize)
	assert.Equal(t, int64(100000), cfg.MaxEntries)
	assert.Equal(t, 1*time.Hour, cfg.SnapshotInterval)
}

func TestWAL_FeatureFlagIntegration(t *testing.T) {
	t.Run("wal_enabled_by_default", func(t *testing.T) {
		config.ResetFeatureFlags()
		// After reset, WAL is disabled, but let's enable it
		config.EnableWAL()
		defer config.DisableWAL()

		assert.True(t, config.IsWALEnabled())
	})

	t.Run("wal_disable_toggle", func(t *testing.T) {
		config.EnableWAL()
		assert.True(t, config.IsWALEnabled())

		config.DisableWAL()
		assert.False(t, config.IsWALEnabled())
	})

	t.Run("with_wal_enabled_helper", func(t *testing.T) {
		config.ResetFeatureFlags()
		assert.False(t, config.IsWALEnabled())

		cleanup := config.WithWALEnabled()
		assert.True(t, config.IsWALEnabled())

		cleanup()
		assert.False(t, config.IsWALEnabled())
	})
}

// Helper function
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// Benchmarks

func BenchmarkWAL_Append(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	node := &Node{ID: "bench-node", Labels: []string{"Benchmark"}}
	data := WALNodeData{Node: node}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Append(OpCreateNode, data)
	}
}

func BenchmarkWAL_AppendWithSync(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	node := &Node{ID: "bench-node", Labels: []string{"Benchmark"}}
	data := WALNodeData{Node: node}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Append(OpCreateNode, data)
	}
}

func BenchmarkWALEngine_CreateNode(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	engine := NewMemoryEngine()
	walEngine := NewWALEngine(engine, wal)
	defer walEngine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		walEngine.CreateNode(&Node{ID: NodeID("n" + string(rune(i)))})
	}
}

// ============================================================================
// BatchWriter Tests
// ============================================================================

func TestBatchWriter_AppendNode(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()
	require.NotNil(t, batch)

	// Write multiple entries in batch
	node1 := &Node{ID: "batch-n1", Labels: []string{"Test"}}
	node2 := &Node{ID: "batch-n2", Labels: []string{"Test"}}

	err = batch.AppendNode(OpCreateNode, node1)
	require.NoError(t, err)

	err = batch.AppendNode(OpCreateNode, node2)
	require.NoError(t, err)

	// Before commit, entries should be buffered
	assert.Equal(t, 2, batch.Len())
}

func TestBatchWriter_Commit(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add entries
	for i := 0; i < 5; i++ {
		node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("commit-n%d", i))), Labels: []string{"Test"}}
		err = batch.AppendNode(OpCreateNode, node)
		require.NoError(t, err)
	}

	// Commit batch
	firstSeq, lastSeq, err := batch.CommitWithSeq()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), firstSeq)
	assert.Equal(t, uint64(5), lastSeq)

	// Batch should be cleared after commit
	assert.Equal(t, 0, batch.Len())
}

func TestBatchWriter_Rollback(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add entries
	for i := 0; i < 3; i++ {
		node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("rollback-n%d", i))), Labels: []string{"Test"}}
		batch.AppendNode(OpCreateNode, node)
	}

	// Rollback - should discard all entries
	batch.Rollback()

	// Batch should be cleared
	assert.Equal(t, 0, batch.Len())
}

func TestBatchWriter_AppendEdge(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add edge entries
	edge1 := &Edge{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"}
	edge2 := &Edge{ID: "e2", Type: "LIKES", StartNode: "n2", EndNode: "n3"}

	err = batch.AppendEdge(OpCreateEdge, edge1)
	require.NoError(t, err)

	err = batch.AppendEdge(OpCreateEdge, edge2)
	require.NoError(t, err)

	assert.Equal(t, 2, batch.Len())

	err = batch.Commit()
	require.NoError(t, err)
}

func TestBatchWriter_CommitWithTxID(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatchWithTxID("tx-batch-1")

	node := &Node{ID: "tx-node-1", Labels: []string{"Test"}}
	err = batch.AppendNode(OpCreateNode, node)
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.Len(t, entries, 1)

	var data WALNodeData
	require.NoError(t, json.Unmarshal(entries[0].Data, &data))
	assert.Equal(t, "tx-batch-1", data.TxID)
}

func TestWAL_TruncateAfterSnapshot(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("truncate_removes_old_entries", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		// Create WAL and engine
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()

		// Add entries before snapshot
		for i := 1; i <= 5; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"BeforeSnapshot"}}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Create snapshot (gets sequence number)
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		snapshotSeq := snapshot.Sequence

		// Save snapshot
		err = SaveSnapshot(snapshot, snapshotPath)
		require.NoError(t, err)

		// Add entries after snapshot
		for i := 6; i <= 10; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"AfterSnapshot"}}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Get WAL size before truncation
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		// Truncate WAL to remove entries before snapshot
		err = wal.TruncateAfterSnapshot(snapshotSeq)
		require.NoError(t, err)

		// Get WAL size after truncation
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		// WAL should be significantly smaller
		assert.Less(t, sizeAfter, sizeBefore, "WAL should shrink after truncation")

		// Read WAL entries to verify only post-snapshot entries remain
		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)

		// All remaining entries should have sequence > snapshotSeq
		for _, entry := range entries {
			assert.Greater(t, entry.Sequence, snapshotSeq,
				"All entries should be after snapshot sequence")
		}

		// Should have ~5 entries (nodes 6-10 + checkpoint is possible)
		assert.GreaterOrEqual(t, len(entries), 5, "Should have at least 5 post-snapshot entries")
		assert.LessOrEqual(t, len(entries), 6, "Should have at most 6 entries (5 nodes + possible checkpoint)")
	})

	t.Run("truncate_preserves_data_integrity", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()

		// Add 100 nodes
		for i := 1; i <= 100; i++ {
			prefixed := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			_, err := engine.CreateNode(prefixed)
			require.NoError(t, err)
			require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID(fmt.Sprintf("n%d", i))}}, "test"))
		}

		// Snapshot at node 50
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		SaveSnapshot(snapshot, snapshotPath)

		// Add 50 more nodes
		for i := 101; i <= 150; i++ {
			prefixed := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			_, err := engine.CreateNode(prefixed)
			require.NoError(t, err)
			require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID(fmt.Sprintf("n%d", i))}}, "test"))
		}

		// Truncate
		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		// Verify we can still append after truncation
		newNode := &Node{ID: "n-after-truncate", Labels: []string{"PostTruncate"}}
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: newNode}, "test"))

		wal.Close()

		// Recover from snapshot + truncated WAL
		recovered, err := RecoverFromWAL(walDir, snapshotPath)
		require.NoError(t, err)
		recoveredNS := NewNamespacedEngine(recovered, "test")

		// Should have all 100 nodes from snapshot + 50 post-snapshot + 1 after truncate
		count, err := recoveredNS.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(151), count, "Should have 151 nodes after recovery")

		// Verify specific nodes exist
		n1, err := recoveredNS.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		n150, err := recoveredNS.GetNode("n150")
		assert.NoError(t, err)
		assert.NotNil(t, n150)

		nAfter, err := recoveredNS.GetNode("n-after-truncate")
		assert.NoError(t, err)
		assert.NotNil(t, nAfter)
	})

	t.Run("truncate_with_empty_wal_after_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()

		// Add nodes
		for i := 1; i <= 10; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Snapshot everything
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		SaveSnapshot(snapshot, snapshotPath)

		// NO new entries after snapshot

		// Truncate should leave WAL nearly empty (just checkpoint possibly)
		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		// WAL should be very small or empty
		walPath := filepath.Join(walDir, "wal.log")
		stat, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.Less(t, stat.Size(), int64(1000), "WAL should be nearly empty after full truncation")

		// Verify we can still use WAL after truncation
		newNode := &Node{ID: "n-new"}
		err = wal.Append(OpCreateNode, WALNodeData{Node: newNode})
		require.NoError(t, err)
	})

	t.Run("truncate_on_closed_wal_returns_error", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		require.NoError(t, wal.Close())

		err = wal.TruncateAfterSnapshot(1)
		require.ErrorIs(t, err, ErrWALClosed)
	})

	t.Run("truncate_salvages_entries_when_wal_tail_is_corrupted", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()
		n1 := &Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"BeforeSnapshot"}}
		_, err = engine.CreateNode(n1)
		require.NoError(t, err)
		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: n1}))

		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		require.NoError(t, SaveSnapshot(snapshot, snapshotPath))

		n2 := &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"AfterSnapshot"}}
		_, err = engine.CreateNode(n2)
		require.NoError(t, err)
		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: n2}))
		require.NoError(t, wal.Sync())

		// Corrupt tail so ReadWALEntriesFromDir fails and salvage branch is exercised.
		walPath := filepath.Join(walDir, "wal.log")
		f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte{0x00, 0x01, 0x02, 0x03})
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)
		for _, e := range entries {
			require.Greater(t, e.Sequence, snapshot.Sequence)
		}
	})

	t.Run("truncate_corruption_with_no_salvage_starts_fresh", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1"}}))
		require.NoError(t, wal.Sync())

		walPath := filepath.Join(walDir, "wal.log")
		f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte{0x00, 0x01, 0x02})
		require.NoError(t, err)
		require.NoError(t, f.Close())

		// Snapshot sequence beyond all valid entries -> salvage returns empty.
		require.NoError(t, wal.TruncateAfterSnapshot(999))

		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)
		require.Len(t, entries, 0)
	})

	t.Run("truncate_returns_temp_file_creation_error_when_dir_missing", func(t *testing.T) {
		realDir := t.TempDir()
		walPath := filepath.Join(realDir, "wal.log")
		f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)

		w := &WAL{
			config: &WALConfig{
				Dir:      filepath.Join(realDir, "missing-parent"),
				SyncMode: "none",
			},
			file:   f,
			writer: bufio.NewWriterSize(bytes.NewBuffer(nil), 16),
		}

		err = w.TruncateAfterSnapshot(0)
		require.ErrorContains(t, err, "failed to create temp WAL")
	})

	t.Run("truncate_handles_read_entries_error_by_salvaging_or_fresh_start", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		require.NoError(t, wal.Append(OpCreateNode, WALNodeData{
			Node: &Node{ID: NodeID(prefixTestID("corrupt-manifest-node"))},
		}))
		require.NoError(t, wal.Sync())

		// Force ReadWALEntriesFromDir error in truncate path.
		require.NoError(t, writeWALManifest(dir, &WALManifest{
			Version: walManifestVersion,
			Segments: []WALSegment{
				{FirstSeq: 1, LastSeq: 1, Path: "../bad-segment.wal"},
			},
		}))

		require.NoError(t, wal.TruncateAfterSnapshot(0))

		// Truncate should recover by creating a usable WAL file.
		entries, readErr := ReadWALEntries(walActivePath(dir))
		require.NoError(t, readErr)
		assert.NotNil(t, entries)
	})
}

func TestWAL_TruncateAfterSnapshot_EarlyErrorBranches(t *testing.T) {
	t.Run("returns flush error before truncate when syncLocked fails", func(t *testing.T) {
		dir := t.TempDir()
		walPath := walActivePath(dir)
		f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer f.Close()

		w := &WAL{
			config: &WALConfig{Dir: dir, SyncMode: "none"},
			file:   f,
			writer: bufio.NewWriterSize(&walErrWriter{err: fmt.Errorf("flush-fail")}, 16),
		}
		_, _ = w.writer.WriteString("pending")
		err = w.TruncateAfterSnapshot(1)
		require.ErrorContains(t, err, "failed to flush before truncate")
	})

	t.Run("returns close error when WAL file close fails", func(t *testing.T) {
		dir := t.TempDir()
		walPath := walActivePath(dir)
		f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close()) // make subsequent close fail

		w := &WAL{
			config: &WALConfig{Dir: dir, SyncMode: "none"},
			file:   f,
			writer: bufio.NewWriterSize(bytes.NewBuffer(nil), 16),
		}
		err = w.TruncateAfterSnapshot(1)
		require.ErrorContains(t, err, "failed to close for truncate")
	})

	t.Run("returns rename error when wal active path is a directory", func(t *testing.T) {
		dir := t.TempDir()
		walPath := walActivePath(dir)
		require.NoError(t, os.MkdirAll(walPath, 0755))

		// Keep WAL object valid for sync/close path; active path intentionally points to a directory.
		backingFile, err := os.OpenFile(filepath.Join(dir, "backing.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer backingFile.Close()

		w := &WAL{
			config: &WALConfig{Dir: dir, SyncMode: "none"},
			file:   backingFile,
			writer: bufio.NewWriterSize(bytes.NewBuffer(nil), 16),
		}

		err = w.TruncateAfterSnapshot(0)
		require.ErrorContains(t, err, "failed to rename truncated WAL")
	})
}

func TestWALEngine_AutoCompaction(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("auto_compaction_truncates_wal_periodically", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// Create WAL with short snapshot interval for testing
		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 100 * time.Millisecond, // Very frequent for testing
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)
		defer walEngine.DisableAutoCompaction()

		// Add many nodes to WAL
		for i := 1; i <= 50; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			walEngine.CreateNode(node)
		}

		// Get WAL size before compaction
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		// Wait for at least one snapshot cycle
		time.Sleep(250 * time.Millisecond)

		// Check that snapshot was created
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(files), 1, "At least one snapshot should be created")

		// Check stats
		totalSnapshots, lastSnapshot := walEngine.GetSnapshotStats()
		assert.GreaterOrEqual(t, totalSnapshots, int64(1), "Should have created at least 1 snapshot")
		assert.False(t, lastSnapshot.IsZero(), "Last snapshot time should be set")

		// Add more nodes after first compaction
		for i := 51; i <= 100; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			walEngine.CreateNode(node)
		}

		// Wait for another snapshot cycle
		time.Sleep(250 * time.Millisecond)

		// WAL should have been truncated - size should be manageable
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		// WAL should not grow unbounded - truncation keeps it under control
		// It should be much smaller than if we had all 100 nodes in it
		assert.Less(t, sizeAfter, sizeBefore*3, "WAL should not grow unbounded due to truncation")

		// Verify all nodes are still accessible (data not lost)
		for i := 1; i <= 100; i++ {
			node, err := walEngine.GetNode(NodeID(prefixTestID(fmt.Sprintf("n%d", i))))
			assert.NoError(t, err)
			assert.NotNil(t, node)
		}
	})

	t.Run("auto_compaction_recoverable", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// First session: create data with auto-compaction
		func() {
			cfg := &WALConfig{
				Dir:              walDir,
				SyncMode:         "immediate",
				SnapshotInterval: 50 * time.Millisecond,
			}
			wal, err := NewWAL("", cfg)
			require.NoError(t, err)
			defer wal.Close()

			engine := NewMemoryEngine()
			walEngine := NewWALEngine(engine, wal)

			err = walEngine.EnableAutoCompaction(snapshotDir)
			require.NoError(t, err)

			// Create nodes
			for i := 1; i <= 100; i++ {
				node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
				walEngine.CreateNode(node)
			}

			// Wait for snapshot
			time.Sleep(200 * time.Millisecond)

			walEngine.DisableAutoCompaction()
		}()

		// Find latest snapshot (filter for .json files only, not .tmp files)
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)

		var jsonFiles []os.DirEntry
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".json") && !strings.HasSuffix(f.Name(), ".tmp") {
				jsonFiles = append(jsonFiles, f)
			}
		}
		require.GreaterOrEqual(t, len(jsonFiles), 1, "Should have at least one snapshot")

		// Use the chronologically most recent snapshot (names are snapshot-20060102-150405.json)
		sort.Slice(jsonFiles, func(i, j int) bool { return jsonFiles[i].Name() < jsonFiles[j].Name() })
		latestSnapshot := filepath.Join(snapshotDir, jsonFiles[len(jsonFiles)-1].Name())

		// Second session: recover from snapshot + WAL
		recovered, err := RecoverFromWAL(walDir, latestSnapshot)
		require.NoError(t, err)
		recoveredNS := NewNamespacedEngine(recovered, "test")

		// Verify all data recovered
		count, err := recoveredNS.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(100), count, "All nodes should be recovered")

		// Spot check nodes
		n1, err := recoveredNS.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		n100, err := recoveredNS.GetNode("n100")
		assert.NoError(t, err)
		assert.NotNil(t, n100)
	})

	t.Run("disable_auto_compaction_stops_snapshots", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 50 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Enable then immediately disable
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)
		walEngine.DisableAutoCompaction()

		// Add nodes
		for i := 1; i <= 20; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i)))}
			walEngine.CreateNode(node)
		}

		// Wait longer than snapshot interval
		time.Sleep(200 * time.Millisecond)

		// No snapshots should be created
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		assert.Equal(t, 0, len(files), "No snapshots should be created after disable")
	})
}

func TestBatchWriter_AppendDelete(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add delete entries
	err = batch.AppendDelete(OpDeleteNode, "node-to-delete-1")
	require.NoError(t, err)

	err = batch.AppendDelete(OpDeleteNode, "node-to-delete-2")
	require.NoError(t, err)

	assert.Equal(t, 2, batch.Len())

	err = batch.Commit()
	require.NoError(t, err)
}

func BenchmarkBatchWriter_Commit(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := wal.NewBatch()
		for j := 0; j < 100; j++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("bench-n%d-%d", i, j)))}
			batch.AppendNode(OpCreateNode, node)
		}
		batch.Commit()
	}
}

// ============================================================================
// WALEngine StreamingEngine Tests
// ============================================================================

func TestWALEngine_StreamNodes(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		_, err := walEngine.CreateNode(&Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllNodes", func(t *testing.T) {
		var count int
		err := walEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := walEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			if count >= 10 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 10, count, "Should stop after 10 nodes")
	})
}

func TestWALEngine_StreamNodesByPrefix(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("delegates to wrapped PrefixStreamingEngine", func(t *testing.T) {
		nodes := []*Node{
			{ID: "tenant_a:n1"},
			{ID: "tenant_b:n2"},
			{ID: "tenant_a:n3"},
		}
		inner := &walPrefixStreamingEngine{
			Engine: NewMemoryEngine(),
			nodes:  nodes,
		}
		t.Cleanup(func() { _ = inner.Engine.Close() })
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		walEngine := NewWALEngine(inner, wal)
		var got []NodeID
		err = walEngine.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
			got = append(got, node.ID)
			if len(got) == 1 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, inner.streamPrefixCalls, "prefix path must be used")
		assert.Equal(t, 0, inner.streamNodesCalls, "full stream fallback must not be used")
		assert.Equal(t, "tenant_a:", inner.lastPrefix)
		require.Len(t, got, 1)
		assert.Equal(t, NodeID("tenant_a:n1"), got[0])
	})

	t.Run("falls back to StreamNodes+prefix filter when prefix streamer is unavailable", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		inner := &walStreamingCountEngine{
			Engine: base,
			nodes: []*Node{
				{ID: "tenant_a:n1"},
				{ID: "tenant_b:n2"},
				{ID: "tenant_a:n3"},
			},
		}
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		walEngine := NewWALEngine(inner, wal)
		var got []NodeID
		err = walEngine.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
			got = append(got, node.ID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []NodeID{"tenant_a:n1", "tenant_a:n3"}, got)
	})
}

func TestWALEngine_ForEachNodeIDByLabel(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 5; i++ {
		_, err := base.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("nornic:n-%d", i)),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)
	}

	wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	walEngine := NewWALEngine(base, wal)
	lookup, ok := interface{}(walEngine).(LabelNodeIDLookupEngine)
	require.True(t, ok, "wal wrapper must expose LabelNodeIDLookupEngine")

	var count int
	err = lookup.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
		count++
		return count < 3
	})
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestWALEngine_StreamEdges(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create nodes first
	for i := 0; i < 10; i++ {
		_, err := walEngine.CreateNode(&Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Create edges
	for i := 0; i < 50; i++ {
		err := walEngine.CreateEdge(&Edge{
			ID:        EdgeID(prefixTestID(fmt.Sprintf("edge-%d", i))),
			Type:      "CONNECTS",
			StartNode: NodeID(prefixTestID(fmt.Sprintf("node-%d", i%10))),
			EndNode:   NodeID(prefixTestID(fmt.Sprintf("node-%d", (i+1)%10))),
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllEdges", func(t *testing.T) {
		var count int
		err := walEngine.StreamEdges(ctx, func(edge *Edge) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 50, count, "Should stream all 50 edges")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := walEngine.StreamEdges(ctx, func(edge *Edge) error {
			count++
			if count >= 5 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 5, count, "Should stop after 5 edges")
	})
}

func TestWALEngine_StreamNodeChunks(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		_, err := walEngine.CreateNode(&Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamInChunks", func(t *testing.T) {
		var totalNodes int
		var chunkCount int
		err := walEngine.StreamNodeChunks(ctx, 25, func(nodes []*Node) error {
			chunkCount++
			totalNodes += len(nodes)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, totalNodes, "Should stream all 100 nodes")
		assert.Equal(t, 4, chunkCount, "Should have 4 chunks of 25")
	})
}

func TestWALEngine_HelperDelegatesAndFallbacks(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	newWALEngine := func(t *testing.T, engine Engine) *WALEngine {
		t.Helper()
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		return NewWALEngine(engine, wal)
	}

	t.Run("prefix counts use fallback and propagate streaming errors", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &exportableOnlyEngine{
			Engine:   base,
			allNodes: []*Node{{ID: "test:n1"}, {ID: "other:n2"}},
			allEdges: []*Edge{{ID: "test:e1", Type: "REL"}, {ID: "other:e2", Type: "REL"}},
		}
		walEngine := newWALEngine(t, engine)

		nodes, err := walEngine.NodeCountByPrefix("test:")
		require.NoError(t, err)
		assert.Equal(t, int64(1), nodes)

		edges, err := walEngine.EdgeCountByPrefix("test:")
		require.NoError(t, err)
		assert.Equal(t, int64(1), edges)

		streamErrBase := NewMemoryEngine()
		t.Cleanup(func() { _ = streamErrBase.Close() })
		streamErrEngine := &walPrefixErrorEngine{
			Engine:  streamErrBase,
			nodeErr: errors.New("stream nodes failed"),
			edgeErr: errors.New("stream edges failed"),
		}
		walEngine = newWALEngine(t, streamErrEngine)

		_, err = walEngine.NodeCountByPrefix("test:")
		require.ErrorContains(t, err, "stream nodes failed")

		_, err = walEngine.EdgeCountByPrefix("test:")
		require.ErrorContains(t, err, "stream edges failed")
	})

	t.Run("prefix stats and schema provider delegation", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		schema := NewSchemaManager()
		engine := &walSchemaProviderEngine{
			Engine: &walPrefixStatsEngine{
				Engine:    base,
				nodeCount: 7,
				edgeCount: 4,
			},
			schema: schema,
		}
		walEngine := newWALEngine(t, engine)

		nodes, err := walEngine.NodeCountByPrefix("ignored:")
		require.NoError(t, err)
		assert.Equal(t, int64(7), nodes)

		edges, err := walEngine.EdgeCountByPrefix("ignored:")
		require.NoError(t, err)
		assert.Equal(t, int64(4), edges)

		assert.Equal(t, schema, walEngine.GetSchemaForNamespace("tenant"))

		noProvider := newWALEngine(t, &exportableOnlyEngine{Engine: base})
		assert.Equal(t, base.GetSchema(), noProvider.GetSchemaForNamespace("tenant"))
	})

	t.Run("prefix counts use streaming fallback", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &walStreamingCountEngine{
			Engine: base,
			nodes: []*Node{
				{ID: "test:stream-n1"},
				{ID: "other:stream-n2"},
				{ID: "test:stream-n3"},
			},
			edges: []*Edge{
				{ID: "test:stream-e1", Type: "REL"},
				{ID: "other:stream-e2", Type: "REL"},
				{ID: "test:stream-e3", Type: "REL"},
			},
		}
		walEngine := newWALEngine(t, engine)

		nodes, err := walEngine.NodeCountByPrefix("test:")
		require.NoError(t, err)
		assert.Equal(t, int64(2), nodes)

		edges, err := walEngine.EdgeCountByPrefix("test:")
		require.NoError(t, err)
		assert.Equal(t, int64(2), edges)
	})

	t.Run("embedding delegates and iterate fallback", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		walEngine := newWALEngine(t, engine)

		_, err := engine.CreateNode(&Node{ID: "test:embed", Labels: []string{"Doc"}, Properties: map[string]any{"text": "embed me"}})
		require.NoError(t, err)
		walEngine.AddToPendingEmbeddings("test:embed")
		assert.Equal(t, 1, walEngine.PendingEmbeddingsCount())

		found := walEngine.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
		assert.Equal(t, NodeID("test:embed"), found.ID)

		walEngine.MarkNodeEmbedded("test:embed")
		assert.Equal(t, 0, walEngine.PendingEmbeddingsCount())

		added := walEngine.RefreshPendingEmbeddingsIndex()
		assert.GreaterOrEqual(t, added, 0)

		visited := 0
		require.NoError(t, walEngine.IterateNodes(func(node *Node) bool {
			visited++
			return false
		}))
		assert.Equal(t, 1, visited)

		noIterBase := NewMemoryEngine()
		t.Cleanup(func() { _ = noIterBase.Close() })
		noIter := newWALEngine(t, &exportableOnlyEngine{Engine: noIterBase})
		err = noIter.IterateNodes(func(node *Node) bool { return true })
		require.ErrorContains(t, err, "does not support IterateNodes")
		assert.Nil(t, noIter.FindNodeNeedingEmbedding())
		assert.Equal(t, 0, noIter.RefreshPendingEmbeddingsIndex())
		assert.Equal(t, 0, noIter.PendingEmbeddingsCount())
		noIter.AddToPendingEmbeddings("test:none")
		noIter.MarkNodeEmbedded("test:none")
	})

	t.Run("stream fallback handles callback and load errors", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &exportableOnlyEngine{
			Engine:   base,
			allNodes: []*Node{{ID: "test:n1"}, {ID: "test:n2"}},
			allEdges: []*Edge{{ID: "test:e1", Type: "REL"}, {ID: "test:e2", Type: "REL"}},
		}
		walEngine := newWALEngine(t, engine)

		errBoom := errors.New("node callback failed")
		err := walEngine.StreamNodes(context.Background(), func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("edge callback failed")
		err = walEngine.StreamEdges(context.Background(), func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		engine.nodeErr = errors.New("all nodes failed")
		err = walEngine.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "all nodes failed")

		engine.edgeErr = errors.New("all edges failed")
		err = walEngine.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "all edges failed")
	})

	t.Run("direct streaming delegate propagates node edge and chunk errors", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &walStreamErrorEngine{
			Engine:  base,
			nodeErr: errors.New("delegate nodes failed"),
			edgeErr: errors.New("delegate edges failed"),
		}
		walEngine := newWALEngine(t, engine)

		err := walEngine.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "delegate nodes failed")

		err = walEngine.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "delegate edges failed")

		err = walEngine.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error { return nil })
		require.ErrorContains(t, err, "delegate nodes failed")
	})

	t.Run("stream node chunks fallback and delete by prefix", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &exportableOnlyEngine{
			Engine: base,
			allNodes: []*Node{
				{ID: "test:n1"},
				{ID: "test:n2"},
				{ID: "test:n3"},
			},
		}
		walEngine := newWALEngine(t, engine)

		var chunkSizes []int
		require.NoError(t, walEngine.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error {
			chunkSizes = append(chunkSizes, len(nodes))
			return nil
		}))
		assert.Equal(t, []int{2, 1}, chunkSizes)

		errBoom := errors.New("chunk callback failed")
		err := walEngine.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		_, err = base.CreateNode(&Node{ID: "test:drop-n1"})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "other:keep"})
		require.NoError(t, err)
		require.NoError(t, base.CreateEdge(&Edge{ID: "test:drop-e1", StartNode: "test:drop-n1", EndNode: "other:keep", Type: "REL"}))

		nodesDeleted, edgesDeleted, err := walEngine.DeleteByPrefix("test:")
		require.NoError(t, err)
		assert.Equal(t, int64(1), nodesDeleted)
		assert.Equal(t, int64(1), edgesDeleted)
	})

	t.Run("read delegates last-write time and namespace listing", func(t *testing.T) {
		assert.Equal(t, time.Time{}, (*WALEngine)(nil).LastWriteTime())

		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &walNamespaceListerEngine{
			Engine:     base,
			namespaces: []string{"tenant_a", "tenant_b"},
		}
		walEngine := newWALEngine(t, engine)

		assert.Equal(t, []string{"tenant_a", "tenant_b"}, walEngine.ListNamespaces())

		_, err := walEngine.CreateNode(&Node{ID: NodeID(prefixTestID("last-write-node")), Labels: []string{"Doc"}})
		require.NoError(t, err)
		lastWrite := walEngine.LastWriteTime()
		assert.False(t, lastWrite.IsZero())

		noLister := newWALEngine(t, &exportableOnlyEngine{Engine: base})
		assert.Nil(t, noLister.ListNamespaces())
	})

	t.Run("update embedding dispatch and bulk database guards", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		dispatch := &walEmbeddingDispatchEngine{Engine: base}
		walEngine := newWALEngine(t, dispatch)
		_, err := walEngine.CreateNode(&Node{
			ID:         "tenant_a:embed-node",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"k": "v1"},
		})
		require.NoError(t, err)

		err = walEngine.UpdateNodeEmbedding(&Node{
			ID:         "tenant_a:embed-node",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"k": "v2"},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, dispatch.updateEmbeddingCalls)
		assert.Equal(t, 0, dispatch.updateNodeCalls)

		dispatch.updateEmbeddingErr = errors.New("embed dispatch failed")
		err = walEngine.UpdateNodeEmbedding(&Node{ID: "tenant_a:embed-node", Labels: []string{"Doc"}})
		require.ErrorContains(t, err, "embed dispatch failed")

		fallbackEngine := newWALEngine(t, &exportableOnlyEngine{Engine: base})
		err = fallbackEngine.UpdateNodeEmbedding(&Node{
			ID:         "tenant_a:embed-node",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"k": "v3"},
		})
		require.NoError(t, err)

		node, err := base.GetNode("tenant_a:embed-node")
		require.NoError(t, err)
		require.Equal(t, "v3", node.Properties["k"])

		err = walEngine.BulkCreateNodes([]*Node{
			{ID: "tenant_a:n1"},
			{ID: "tenant_b:n2"},
		})
		require.ErrorContains(t, err, "multiple databases")

		err = walEngine.BulkCreateNodes([]*Node{
			{ID: "tenant_a:n3"},
			nil,
		})
		require.ErrorIs(t, err, ErrInvalidData)

		err = walEngine.BulkCreateEdges([]*Edge{
			{ID: "tenant_a:e1", StartNode: "tenant_a:n1", EndNode: "tenant_a:n2", Type: "REL"},
			{ID: "tenant_b:e2", StartNode: "tenant_b:n1", EndNode: "tenant_b:n2", Type: "REL"},
		})
		require.ErrorContains(t, err, "multiple databases")

		err = walEngine.BulkCreateEdges([]*Edge{
			{ID: "tenant_a:e3", StartNode: "tenant_b:n1", EndNode: "tenant_a:n2", Type: "REL"},
		})
		require.ErrorContains(t, err, "inconsistent database prefixes")

		err = walEngine.BulkCreateEdges([]*Edge{
			{ID: "tenant_a:e4", StartNode: "tenant_a:n1", EndNode: "tenant_a:n2", Type: "REL"},
			nil,
		})
		require.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("delegate getters and bulk delete edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		walEngine := newWALEngine(t, engine)

		_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"name": "one"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}, Properties: map[string]any{"name": "two"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:n3", Labels: []string{"Other"}, Properties: map[string]any{"name": "three"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:n2", EndNode: "test:n3", Type: "REL"}))

		first, err := walEngine.GetFirstNodeByLabel("Doc")
		require.NoError(t, err)
		require.NotNil(t, first)

		batch, err := walEngine.BatchGetNodes([]NodeID{"test:n1", "test:n2"})
		require.NoError(t, err)
		require.Len(t, batch, 2)

		byType, err := walEngine.GetEdgesByType("REL")
		require.NoError(t, err)
		require.Len(t, byType, 2)

		require.NoError(t, walEngine.BulkDeleteEdges([]EdgeID{"test:e1", "test:e2"}))
		_, err = engine.GetEdge("test:e1")
		assert.ErrorIs(t, err, ErrNotFound)
		_, err = engine.GetEdge("test:e2")
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

// TestWALEngine_ImplementsStreamingEngine verifies the interface is implemented
func TestWALEngine_ImplementsStreamingEngine(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)

	// This should compile - WALEngine implements StreamingEngine
	var _ StreamingEngine = walEngine
	t.Log("WALEngine implements StreamingEngine interface")
}

// TestFullStorageChain_Streaming tests streaming through the full storage chain:
// AsyncEngine -> WALEngine -> BadgerEngine
func TestFullStorageChain_Streaming(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	// Create the full storage chain
	badgerEngine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badgerEngine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(badgerEngine, wal)

	asyncEngine := NewAsyncEngine(walEngine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour, // Don't auto-flush
	})
	defer asyncEngine.Close()

	ctx := context.Background()

	// Create 100 nodes through the full chain
	for i := 0; i < 100; i++ {
		_, err := asyncEngine.CreateNode(&Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("chain-node-%d", i))),
			Labels: []string{"ChainTest"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamThroughFullChain", func(t *testing.T) {
		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes through full chain")
	})

	t.Run("EarlyTerminationThroughFullChain", func(t *testing.T) {
		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			if count >= 25 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 25, count, "Should stop after 25 nodes")
	})

	t.Run("StreamAfterFlush", func(t *testing.T) {
		require.NoError(t, asyncEngine.Flush())

		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes after flush")
	})
}

func TestCloneNodeForWAL(t *testing.T) {
	t.Run("nil node returns nil", func(t *testing.T) {
		result := cloneNodeForWAL("mydb", nil)
		assert.Nil(t, result)
	})

	t.Run("strips database prefix from node ID", func(t *testing.T) {
		node := &Node{
			ID:         NodeID("mydb:node-1"),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice"},
		}
		result := cloneNodeForWAL("mydb", node)
		require.NotNil(t, result)
		assert.Equal(t, NodeID("node-1"), result.ID)
		// Original should be unchanged
		assert.Equal(t, NodeID("mydb:node-1"), node.ID)
		// Properties should be the same reference (shallow clone)
		assert.Equal(t, "Alice", result.Properties["name"])
	})
}

func TestCloneEdgeForWAL(t *testing.T) {
	t.Run("nil edge returns nil", func(t *testing.T) {
		result := cloneEdgeForWAL("mydb", nil)
		assert.Nil(t, result)
	})

	t.Run("strips database prefix from all IDs", func(t *testing.T) {
		edge := &Edge{
			ID:         EdgeID("mydb:edge-1"),
			StartNode:  NodeID("mydb:node-1"),
			EndNode:    NodeID("mydb:node-2"),
			Type:       "KNOWS",
			Properties: map[string]interface{}{"since": 2020},
		}
		result := cloneEdgeForWAL("mydb", edge)
		require.NotNil(t, result)
		assert.Equal(t, EdgeID("edge-1"), result.ID)
		assert.Equal(t, NodeID("node-1"), result.StartNode)
		assert.Equal(t, NodeID("node-2"), result.EndNode)
		assert.Equal(t, "KNOWS", result.Type)
		// Original should be unchanged
		assert.Equal(t, EdgeID("mydb:edge-1"), edge.ID)
	})
}
