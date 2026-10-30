// Package storage - Unit tests for atomic WAL write format.
//
// These tests verify:
// 1. Atomic format writing and reading works correctly
// 2. Partial writes are detected and handled gracefully
// 3. CRC verification catches corruption
// 4. Superseded frame versions are refused, never repaired
//
// Fixtures are built with walFormatVersionCurrent and the current payload
// codec: this prototype refuses every older frame version outright, so an
// old-version fixture is only ever used to assert that rejection.
package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// ATOMIC FORMAT TESTS
// =============================================================================

// currentWALPayload encodes entry with the current payload codec, as an Append
// to the active WAL would.
func currentWALPayload(t *testing.T, entry WALEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err, "encoding a current payload must succeed")
	return payload
}

// atomicHeader writes the 9-byte frame header for a record whose payload is
// length payloadLen bytes.
func atomicHeader(version uint8, payloadLen uint32) []byte {
	header := make([]byte, 9)
	binary.LittleEndian.PutUint32(header[0:4], walMagic)
	header[4] = version
	binary.LittleEndian.PutUint32(header[5:9], payloadLen)
	return header
}

// TestAtomicWALWriteFormat verifies the atomic write format structure.
func TestAtomicWALWriteFormat(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()

	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write an entry
	node := &Node{ID: "test-node", Labels: []string{"Test"}}
	if err := wal.Append(OpCreateNode, WALNodeData{Node: node}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	wal.Close()

	// Read raw file to verify format
	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	// Verify magic bytes
	if len(data) < 4 {
		t.Fatal("WAL file too short")
	}
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != walMagic {
		t.Errorf("Expected magic 0x%x, got 0x%x", walMagic, magic)
	}

	// Verify version
	if data[4] != walFormatVersionCurrent {
		t.Errorf("Expected version %d, got %d", walFormatVersionCurrent, data[4])
	}

	// Verify we can read back the entry
	entries, err := ReadWALEntries(walPath, nil)
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].Operation != OpCreateNode {
		t.Errorf("Expected OpCreateNode, got %s", entries[0].Operation)
	}
}

func TestCheckAtomicWALIntegrity_DetectsCorruptionBranches(t *testing.T) {
	makePayload := func(seq uint64) []byte {
		data := mustMarshalRaw(WALNodeData{Node: &Node{ID: NodeID("n1"), Labels: []string{"Test"}}})
		return currentWALPayload(t, WALEntry{
			Sequence:  seq,
			Timestamp: time.Now(),
			Operation: OpCreateNode,
			Data:      data,
			Checksum:  crc32Checksum(data),
		})
	}

	t.Run("crc mismatch after valid entry", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")

		var buf bytes.Buffer
		_, err := writeAtomicRecord(&buf, makePayload(1), walFormatVersionCurrent)
		require.NoError(t, err)
		start := buf.Len()
		payload2 := makePayload(2)
		_, err = writeAtomicRecord(&buf, payload2, walFormatVersionCurrent)
		require.NoError(t, err)
		data := buf.Bytes()
		crcOffset := start + 9 + len(payload2)
		data[crcOffset] ^= 0xFF
		require.NoError(t, os.WriteFile(path, data, 0644))

		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()
		report, err := checkAtomicWALIntegrity(file, &WALIntegrityReport{Healthy: true, Format: "atomic"})
		require.NoError(t, err)
		require.False(t, report.Healthy)
		require.Equal(t, 1, report.CorruptedEntries)
		require.NotEmpty(t, report.CorruptionDetails)
	})

	t.Run("oversized payload after valid entry", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")

		var buf bytes.Buffer
		_, err := writeAtomicRecord(&buf, makePayload(1), walFormatVersionCurrent)
		require.NoError(t, err)
		buf.Write(atomicHeader(walFormatVersionCurrent, walMaxEntrySize+1))
		require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()
		report, err := checkAtomicWALIntegrity(file, &WALIntegrityReport{Healthy: true, Format: "atomic"})
		require.NoError(t, err)
		require.False(t, report.Healthy)
		require.Equal(t, 1, report.CorruptedEntries)
		require.NotEmpty(t, report.Errors)
	})

	t.Run("invalid trailer and partial header", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")

		payload := makePayload(1)
		var buf bytes.Buffer
		_, err := writeAtomicRecord(&buf, payload, walFormatVersionCurrent)
		require.NoError(t, err)
		data := buf.Bytes()
		trailerOffset := 9 + len(payload) + 4
		data[trailerOffset] ^= 0xAA
		data = append(data, 0x57, 0x41)
		require.NoError(t, os.WriteFile(path, data, 0644))

		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()
		report, err := checkAtomicWALIntegrity(file, &WALIntegrityReport{Healthy: true, Format: "atomic"})
		require.NoError(t, err)
		require.NotEmpty(t, report.Errors)
	})
}

func TestReadWALEntriesForTruncation_Formats(t *testing.T) {
	t.Run("legacy format is refused", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		f, err := os.Create(path)
		require.NoError(t, err)
		enc := json.NewEncoder(f)
		require.NoError(t, enc.Encode(&WALEntry{Sequence: 1, Operation: OpCheckpoint}))
		require.NoError(t, enc.Encode(&WALEntry{Sequence: 2, Operation: OpCreateNode}))
		require.NoError(t, enc.Encode(&WALEntry{Sequence: 3, Operation: OpCreateNode}))
		_, _ = f.WriteString("{bad-json")
		require.NoError(t, f.Close())

		// A bare-JSON WAL is an unsupported format: truncation salvage must
		// refuse it rather than salvage entries from it.
		entries, err := readWALEntriesForTruncation(path, 1)
		require.ErrorIs(t, err, ErrWALUnsupportedVersion)
		require.Empty(t, entries)
	})

	t.Run("atomic format reader", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		var buf bytes.Buffer
		for _, seq := range []uint64{1, 2, 3} {
			payload := currentWALPayload(t, WALEntry{Sequence: seq, Timestamp: time.Now(), Operation: OpCreateNode})
			_, err := writeAtomicRecord(&buf, payload, walFormatVersionCurrent)
			require.NoError(t, err)
		}
		buf.Write([]byte{0x57, 0x41})
		require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

		entries, err := readWALEntriesForTruncation(path, 1)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, uint64(2), entries[0].Sequence)
		require.Equal(t, uint64(3), getLastSeq(entries))
	})
}

// TestAtomicWALMultipleEntries verifies the current writer and reader with
// both previously covered batch sizes and every sequence number.
func TestAtomicWALMultipleEntries(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	for _, tc := range []struct {
		name  string
		count int
	}{{"fifty", 50}, {"hundred", 100}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			for i := 0; i < tc.count; i++ {
				node := &Node{ID: NodeID("node-" + string(rune('a'+i%26))), Labels: []string{"Test"}}
				require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: node}), "entry %d", i)
			}
			require.NoError(t, wal.Close())
			entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"), nil)
			require.NoError(t, err)
			require.Len(t, entries, tc.count)
			for i, entry := range entries {
				require.Equal(t, uint64(i+1), entry.Sequence, "entry %d", i)
			}
		})
	}
}

// =============================================================================
// PARTIAL WRITE DETECTION TESTS
// =============================================================================

// TestAtomicWALDetectsIncompleteOrCorruptFrames covers the incomplete
// magic, header, payload and CRC cases as well as a bad complete CRC.
func TestAtomicWALDetectsIncompleteOrCorruptFrames(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	payload := func(t *testing.T) []byte {
		data := mustMarshalRaw(WALNodeData{Node: &Node{ID: "n2"}})
		return currentWALPayload(t, WALEntry{
			Sequence: 2, Timestamp: time.Now(), Operation: OpCreateNode,
			Data: data, Checksum: crc32Checksum(data),
		})
	}
	cases := []struct {
		name      string
		validHead bool
		corrupt   bool
		tail      func(*testing.T) []byte
	}{
		{"partial magic", true, false, func(*testing.T) []byte { return []byte{0x57, 0x41} }},
		{"partial header", true, false, func(*testing.T) []byte {
			header := atomicHeader(walFormatVersionCurrent, 0)[:6]
			header[5] = 0x10
			return header
		}},
		{"partial payload", true, false, func(*testing.T) []byte {
			return append(atomicHeader(walFormatVersionCurrent, 100), []byte("short")...)
		}},
		{"missing CRC", true, false, func(t *testing.T) []byte {
			p := payload(t)
			return append(atomicHeader(walFormatVersionCurrent, uint32(len(p))), p...)
		}},
		{"CRC mismatch", false, true, func(t *testing.T) []byte {
			p := payload(t)
			record := append(atomicHeader(walFormatVersionCurrent, uint32(len(p))), p...)
			return binary.LittleEndian.AppendUint32(record, 0xDEADBEEF)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "wal.log")
			if tc.validHead {
				wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
				require.NoError(t, err)
				require.NoError(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}}))
				require.NoError(t, wal.Close())
			}
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			require.NoError(t, err)
			_, err = f.Write(tc.tail(t))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			entries, err := ReadWALEntries(path, nil)
			if tc.corrupt {
				require.ErrorIs(t, err, ErrWALChecksumFailed)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, uint64(1), entries[0].Sequence)
		})
	}
}

// =============================================================================
// FRAME VERSION REJECTION TESTS
// =============================================================================

// TestLegacyJSONFormatRejected verifies the superseded bare-JSON WAL layout
// (a file starting with '{') is refused rather than read.
func TestLegacyJSONFormatRejected(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	data1 := []byte(`{"node":{"id":"n1","labels":["Test"]}}`)
	data2 := []byte(`{"node":{"id":"n2","labels":["Test"]}}`)
	entries := []WALEntry{
		{
			Sequence:  1,
			Timestamp: time.Now(),
			Operation: OpCreateNode,
			Data:      data1,
			Checksum:  crc32Checksum(data1),
		},
		{
			Sequence:  2,
			Timestamp: time.Now(),
			Operation: OpCreateNode,
			Data:      data2,
			Checksum:  crc32Checksum(data2),
		},
	}

	f, _ := os.Create(walPath)
	encoder := json.NewEncoder(f)
	for _, entry := range entries {
		encoder.Encode(&entry)
	}
	f.Close()

	// The legacy layout is an unsupported format, not a torn current record.
	result, err := ReadWALEntries(walPath, nil)
	require.ErrorIs(t, err, ErrWALUnsupportedVersion)
	require.Empty(t, result)

	after, readErr := os.ReadFile(walPath)
	require.NoError(t, readErr)
	require.NotZero(t, len(after))
	require.Equal(t, byte('{'), after[0], "an unsupported file must be refused, never rewritten")
}

// =============================================================================
// FRAME AUTO-DETECTION TESTS
// =============================================================================

// TestFormatAutoDetection verifies format detection: files written by the
// engine use the current frame, and a superseded bare-JSON file is reported as
// an unsupported format instead of being parsed.
func TestFormatAutoDetection(t *testing.T) {
	t.Run("atomic format", func(t *testing.T) {
		config.EnableWAL()
		defer config.DisableWAL()
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, _ := NewWAL(dir, cfg)
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}})
		wal.Close()

		// Verify file starts with magic
		data, _ := os.ReadFile(filepath.Join(dir, "wal.log"))
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic != walMagic {
			t.Errorf("New files should use atomic format")
		}
		if data[4] != walFormatVersionCurrent {
			t.Errorf("New files should use version %d, got %d", walFormatVersionCurrent, data[4])
		}
	})

	t.Run("legacy format", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "wal.log")

		// Create proper legacy JSON entry with correct checksum
		testData := []byte(`{"node":{"id":"n1"}}`)
		checksum := crc32Checksum(testData)

		entry := WALEntry{
			Sequence:  1,
			Timestamp: time.Now(),
			Operation: OpCreateNode,
			Data:      testData,
			Checksum:  checksum,
		}

		f, _ := os.Create(walPath)
		encoder := json.NewEncoder(f)
		encoder.Encode(&entry)
		f.Close()

		// Verify file starts with '{' (legacy format)
		data, _ := os.ReadFile(walPath)
		if data[0] != '{' {
			t.Error("Legacy files should start with '{'")
		}

		// The legacy layout is refused rather than read.
		entries, err := ReadWALEntries(walPath, nil)
		require.ErrorIs(t, err, ErrWALUnsupportedVersion)
		require.Empty(t, entries)
	})
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

// TestAtomicWALFullRecovery tests complete write-crash-recover cycle.
func TestAtomicWALFullRecovery(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()

	// Phase 1: Write data
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal1, _ := NewWAL(dir, cfg)

	nodes := []*Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Charlie"}},
	}

	// Use a specific database name for this test
	testDBName := "test"
	for _, node := range nodes {
		wal1.AppendWithDatabase(OpCreateNode, WALNodeData{Node: node}, testDBName)
	}
	wal1.Close()

	// Phase 2: Recover from WAL
	// Recovery will use the database name from each WAL entry
	baseEngine, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(dir, ""))
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Wrap with NamespacedEngine using the same database name as the WAL entries
	engine := NewNamespacedEngine(baseEngine, testDBName)

	if result.Applied != 3 {
		t.Errorf("Expected 3 applied, got %d", result.Applied)
	}

	// Verify all nodes recovered
	for _, node := range nodes {
		recovered, err := engine.GetNode(node.ID)
		if err != nil {
			t.Errorf("Failed to get node %s: %v", node.ID, err)
			continue
		}
		if recovered == nil {
			t.Errorf("Node %s not found", node.ID)
		}
	}
}

// TestAtomicWALRecoveryWithPartialWrite tests recovery after simulated crash.
func TestAtomicWALRecoveryWithPartialWrite(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Write valid entries with database name
	testDBName := "test"
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)
	wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}}, testDBName)
	wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "n2", Labels: []string{"Test"}}}, testDBName)
	wal.Close()

	// Simulate crash: append partial entry
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write(atomicHeader(walFormatVersionCurrent, 500)) // Says 500 bytes coming
	f.Write([]byte("incomplete..."))                    // But crash before completion
	f.Close()

	// Recovery should work, getting the 2 valid entries
	// Use the same database name as the WAL entries (testDBName declared above)
	baseEngine, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(dir, ""))
	if err != nil {
		t.Fatalf("Recovery should succeed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, testDBName)

	if result.Applied != 2 {
		t.Errorf("Expected 2 applied (ignoring partial), got %d", result.Applied)
	}

	// Verify data
	n1, _ := engine.GetNode("n1")
	n2, _ := engine.GetNode("n2")
	if n1 == nil || n2 == nil {
		t.Error("Valid nodes should be recovered")
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if containsSubstr(s, sub) {
			return true
		}
	}
	return false
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// =============================================================================
// TRAILER CANARY AND ALIGNMENT TESTS
// =============================================================================

// TestAtomicWALTrailerPresent verifies the trailer canary is written.
func TestAtomicWALTrailerPresent(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write an entry
	node := &Node{ID: "test-node", Labels: []string{"Test"}}
	if err := wal.Append(OpCreateNode, WALNodeData{Node: node}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	wal.Close()

	// Read raw file and verify trailer canary is present
	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	// Record format: header(9) + payload(N) + crc(4) + trailer(8) + padding
	// Find the trailer by looking for the canary value
	found := false
	for i := 0; i <= len(data)-8; i++ {
		trailer := binary.LittleEndian.Uint64(data[i:])
		if trailer == walTrailer {
			found = true
			break
		}
	}
	if !found {
		t.Error("Trailer canary 0xDEADBEEFFEEDFACE not found in WAL file")
	}
}

// TestAtomicWALAlignment verifies records are 8-byte aligned.
func TestAtomicWALAlignment(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write multiple entries of varying sizes
	for i := 0; i < 10; i++ {
		node := &Node{ID: NodeID("node-" + string(rune('a'+i))), Labels: []string{"Test", "More"}}
		if err := wal.Append(OpCreateNode, WALNodeData{Node: node}); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}
	wal.Close()

	// Read raw file and verify size is 8-byte aligned
	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if info.Size()%8 != 0 {
		t.Errorf("WAL file size %d is not 8-byte aligned", info.Size())
	}
}

// TestAtomicWALDetectsMissingTrailer verifies missing trailer is detected.
func TestAtomicWALDetectsMissingTrailer(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Write a valid entry first
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}})
	wal.Close()

	// Manually craft a current-version record with missing trailer
	// (simulates crash after CRC)
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)

	// Create payload with the current codec
	entryBytes := currentWALPayload(t, WALEntry{
		Sequence:  2,
		Timestamp: time.Now(),
		Operation: OpCreateNode,
	})
	entryCRC := crc32Checksum(entryBytes)

	// Write header + payload + CRC (but NO trailer)
	f.Write(atomicHeader(walFormatVersionCurrent, uint32(len(entryBytes))))
	f.Write(entryBytes)

	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, entryCRC)
	f.Write(crcBytes)
	// Intentionally NOT writing trailer - simulating crash
	f.Close()

	// Should recover first entry and detect incomplete second entry
	entries, err := ReadWALEntries(walPath, nil)
	if err != nil {
		t.Fatalf("Should recover valid entries, got error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 valid entry (second incomplete), got %d", len(entries))
	}
}

// TestAtomicWALDetectsCorruptedTrailer verifies corrupted trailer is detected.
func TestAtomicWALDetectsCorruptedTrailer(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Write a valid entry first
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}})
	wal.Close()

	// Manually craft a current-version record with corrupted trailer
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)

	// Create payload with the current codec
	entryBytes := currentWALPayload(t, WALEntry{
		Sequence:  2,
		Timestamp: time.Now(),
		Operation: OpCreateNode,
	})
	entryCRC := crc32Checksum(entryBytes)

	// Write header + payload + CRC + WRONG trailer
	f.Write(atomicHeader(walFormatVersionCurrent, uint32(len(entryBytes))))
	f.Write(entryBytes)

	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, entryCRC)
	f.Write(crcBytes)

	// Write corrupted trailer (wrong value)
	trailerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(trailerBytes, 0xBADBADBADBADBAD)
	f.Write(trailerBytes)

	// Pad to 8-byte alignment
	rawLen := int64(9 + len(entryBytes) + 4 + 8)
	alignedLen := (rawLen + 7) &^ 7
	padding := make([]byte, alignedLen-rawLen)
	f.Write(padding)
	f.Close()

	// Should recover first entry and detect corrupted second entry
	entries, err := ReadWALEntries(walPath, nil)
	if err != nil {
		t.Fatalf("Should recover valid entries, got error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 valid entry (second corrupted), got %d", len(entries))
	}
}

// TestAtomicWALBatchWriterTrailer verifies batch writes include trailer.
func TestAtomicWALBatchWriterTrailer(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()
	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Use batch writer
	batch := wal.NewBatch()
	for i := 0; i < 5; i++ {
		node := &Node{ID: NodeID("batch-node-" + string(rune('a'+i))), Labels: []string{"Batch"}}
		batch.AppendNode(OpCreateNode, node)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Batch commit failed: %v", err)
	}
	wal.Close()

	// Read raw file and verify trailer canary is present for all entries
	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	// Count trailer occurrences
	trailerCount := 0
	for i := 0; i <= len(data)-8; i++ {
		trailer := binary.LittleEndian.Uint64(data[i:])
		if trailer == walTrailer {
			trailerCount++
		}
	}
	if trailerCount != 5 {
		t.Errorf("Expected 5 trailer canaries (one per entry), got %d", trailerCount)
	}

	// Verify all entries can be read back
	entries, err := ReadWALEntries(walPath, nil)
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}
}
