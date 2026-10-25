package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func init() { walDowngradeRecovery = RecoverBufferedCommitsAtOpen }

func walEpochRawBodies(t *testing.T, path string) ([][]byte, []uint8) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var bodies [][]byte
	var versions []uint8
	for offset := 0; offset < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[offset+5 : offset+9]))
		end := offset + int(alignUp(int64(9+size+4+8)))
		entry, err := decodeWALEntry(raw[offset+9:offset+9+size], raw[offset+4])
		require.NoError(t, err)
		versions = append(versions, raw[offset+4])
		if entry.Operation == OpBufferedCommit {
			bodies = append(bodies, append([]byte(nil), raw[offset:end]...))
		}
		offset = end
	}
	return bodies, versions
}

func TestWALPropertyEpochCleanStopClosesAuthority(t *testing.T) {
	for _, order := range []string{"engine first", "WAL first"} {
		t.Run(order, func(t *testing.T) { testWALPropertyEpochCleanStop(t, order == "WAL first") })
	}
}

func TestWALPropertyEpochSynchronousRestoreCleanStop(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NORNICDB_WAL_COMPAT_DIR", dir)
	t.Setenv("NORNICDB_WAL_COMPAT_MODE", "write-clean-sync-restore")
	TestWALPublicDowngradeFixture(t)
	_, versions := walEpochRawBodies(t, filepath.Join(dir, "wal", "wal.log"))
	require.NotEmpty(t, versions)
	for _, version := range versions {
		require.Equal(t, walFormatVersionV3, version, "a clean synchronous restore must permit the old reader without compaction")
	}
}

func testWALPropertyEpochCleanStop(t *testing.T, walFirst bool) {
	t.Cleanup(config.WithWALEnabled())
	dir, logDir := t.TempDir(), t.TempDir()
	source := reviewBindingEngine(t, dir)
	_, err := source.CreateNode(&Node{ID: "close:a", Properties: map[string]any{"alpha": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, source.FlushWriteBehind())
	w, err := NewWAL(logDir, DefaultWALConfig())
	require.NoError(t, err)
	source.SetBufferedCommitWAL(w)
	tx, err := source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("close"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateNode(&Node{ID: "close:a", Properties: map[string]any{"alpha": int64(2)}}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	path := filepath.Join(logDir, "wal.log")
	before, versions := walEpochRawBodies(t, path)
	require.Contains(t, versions, walFormatVersionPropertyEpoch)
	if walFirst {
		require.NoError(t, w.Close())
	}
	require.NoError(t, source.Close())
	require.NoError(t, w.Close())
	after, versions := walEpochRawBodies(t, path)
	require.Equal(t, before, after, "clean conversion must copy ordinary frames byte for byte")
	for _, version := range versions {
		require.Equal(t, walFormatVersionV3, version)
	}
	entries, err := ReadWALEntriesFromDir(logDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].PropertyEpoch, "foreign historical replay still has its complete names")
	// Emulate an older writer by adding an ordinary v3 frame directly, without
	// the new generic-append owner clearing authority on its behalf.
	legacy := entries[0]
	legacy.Sequence++
	legacy.PropertyEpoch = nil
	var buffer bytes.Buffer
	payload, err := encodeWALEntryV3(&buffer, &legacy)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	writer := bufio.NewWriter(file)
	_, err = writeAtomicRecordBufio(writer, payload, walFormatVersionV3)
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, file.Close())
	entries, err = ReadWALEntriesFromDir(logDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Nil(t, entries[1].PropertyEpoch, "old appends cannot borrow closed authority")
}

func TestWALPropertyEpochCleanProofInvalidatedByForeignRestore(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, restore := range []bool{false, true} {
		dir := t.TempDir()
		dest := reviewBindingEngine(t, dir)
		require.NoError(t, dest.beginSemanticPropertyRestore())
		w, err := NewWAL(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		dest.SetBufferedCommitWAL(w)
		require.NoError(t, dest.Close())
		require.NoError(t, w.Close())
		dest = reviewBindingEngine(t, dir)
		require.True(t, dest.walPropertySource.clean)
		identity := dest.walPropertySource.identity
		if restore {
			require.NoError(t, dest.beginSemanticPropertyRestore())
		} else {
			require.NoError(t, markRecoveryPropertyForeign(dest))
		}
		require.False(t, dest.walPropertySource.clean)
		require.NotEqual(t, identity, dest.walPropertySource.identity)
		require.NoError(t, dest.Close())
		dest = reviewBindingEngine(t, dir)
		require.False(t, dest.walPropertySource.clean)
	}
}

func TestWALPropertyEpochRestoreRearmsOlderReaderRefusal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dest := reviewBindingEngine(t, t.TempDir())
	w, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	dest.SetBufferedCommitWAL(w)
	snapshot, err := w.CreateSnapshot(dest)
	require.NoError(t, err)
	require.NoError(t, SaveSnapshot(snapshot, filepath.Join(t.TempDir(), "empty.snapshot")))
	require.NoError(t, w.TruncateAfterSnapshot(snapshot.Sequence))
	_, versions := walEpochRawBodies(t, walActivePath(w.Config().Dir))
	require.NotContains(t, versions, walFormatVersionPropertyEpoch)
	require.NoError(t, dest.beginSemanticPropertyRestore())
	_, versions = walEpochRawBodies(t, walActivePath(w.Config().Dir))
	require.Contains(t, versions, walFormatVersionPropertyEpoch, "a crash during restore must not expose the old clean log")
}

func TestWALPropertyEpochFailedConversionKeepsOriginal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.NoError(t, wal.Close())
	path := walActivePath(wal.Config().Dir)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	// A truncated final ordinary record must not cause any preceding v4
	// authority to be converted in place before complete validation succeeds.
	raw = raw[:len(raw)-1]
	require.NoError(t, os.WriteFile(path, raw, 0600))
	require.Error(t, sealWALPropertyEpochFile(path))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}

// Close reads each file once to validate it, check unapplied records and find
// its controls. Legacy JSON and unlisted segments still reach that check, as
// with the directory visitor, and neither becomes a conversion candidate.
func TestWALPropertyEpochSealPlanVisitsEveryRecoveryFile(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	legacy := func(seq uint64) []byte {
		data := []byte(`{"tx_id":"legacy"}`)
		raw, err := json.Marshal(WALEntry{Sequence: seq, Timestamp: time.Now(), Operation: OpBufferedCommit, Data: data, Checksum: crc32Checksum(data)})
		require.NoError(t, err)
		return append(raw, '\n')
	}
	require.NoError(t, os.WriteFile(walActivePath(dir), append(legacy(0), legacy(9)...), 0600))
	require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0700))
	segment := filepath.Join(walSegmentsDir(dir), "seg-00000000000000000003-00000000000000000003.wal")
	entry := WALEntry{Sequence: 3, Timestamp: time.Now(), Operation: OpCreateNode, Data: []byte(`{}`)}
	entry.Checksum = crc32Checksum(entry.Data)
	var buffer bytes.Buffer
	payload, err := encodeWALEntryV3(&buffer, &entry)
	require.NoError(t, err)
	var record bytes.Buffer
	_, err = writeAtomicRecordBufio(&record, payload, walFormatVersionV3)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(segment, record.Bytes(), 0600))
	var seen []uint64
	wal.mu.Lock()
	plans, err := wal.planPropertyEpochSealLocked(func(entry WALEntry) error {
		seen = append(seen, entry.Sequence)
		return nil
	})
	wal.mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 9}, seen, "sequence zero is skipped, as by the directory visitor")
	require.Len(t, plans, 1, "only listed segments and the active file are converted")
	require.Empty(t, plans[0].controls)
	wal.mu.Lock()
	_, err = wal.planPropertyEpochSealLocked(func(WALEntry) error { return errors.New("veto") })
	wal.mu.Unlock()
	require.ErrorContains(t, err, "veto")
}

func TestWALPropertyEpochSealPlanRejectsChangedFile(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.NoError(t, wal.Close())
	path := walActivePath(wal.Config().Dir)
	var visited int
	plan, err := scanWALPropertyEpochFile(path, func(WALEntry) error { visited++; return nil })
	require.NoError(t, err)
	require.NotEmpty(t, plan.controls)
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Equal(t, len(entries)+len(plan.controls), visited, "controls and records arrive in the scanning read")
	_, err = scanWALPropertyEpochFile(path, func(WALEntry) error { return errors.New("veto") })
	require.ErrorContains(t, err, "veto")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(raw, 0), 0600))
	require.ErrorContains(t, plan.apply(), "changed between scan and sealing")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, append(raw, 0), after, "a rejected plan leaves the older-reader guard in place")
}

// An ordinary synchronous log never received a control, so clean close does
// no WAL work at all: even an unreadable segment directory is not touched.
func TestWALPropertyEpochOrdinarySynchronousCloseSkipsLog(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "store")}))
	require.NoError(t, err)
	w, err := NewWAL(filepath.Join(dir, "wal"), DefaultWALConfig())
	require.NoError(t, err)
	journal := NewWALEngine(b, w)
	_, err = journal.CreateNode(&Node{ID: "plain:a", Properties: map[string]any{"alpha": int64(1)}})
	require.NoError(t, err)
	require.NoFileExists(t, walPropertyEpochUnsealedPath(w.Config().Dir))
	require.NoError(t, os.RemoveAll(walSegmentsDir(w.Config().Dir)))
	require.NoError(t, os.WriteFile(walSegmentsDir(w.Config().Dir), []byte("x"), 0600))
	require.NoError(t, journal.Close())
	require.NoError(t, os.Remove(walSegmentsDir(w.Config().Dir)))
}

// The marker is durable before the first control. Crash recovery preserves it,
// and failed marker writes or removals cannot publish a clean epoch.
func TestWALPropertyEpochUnsealedMarker(t *testing.T) {
	for _, scenario := range []string{"survives crash", "marker I/O failures"} {
		t.Run(scenario, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			logDir := filepath.Join(dir, "wal")
			crash := scenario == "survives crash"
			var b *BadgerEngine
			if crash {
				var err error
				b, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "store")}))
				require.NoError(t, err)
			}
			w, err := NewWAL(logDir, DefaultWALConfig())
			require.NoError(t, err)
			fixtureowner.Own(t, w)
			marker := walPropertyEpochUnsealedPath(logDir)
			if crash {
				b.SetBufferedCommitWAL(w)
			} else {
				before, err := os.ReadFile(walActivePath(logDir))
				require.NoError(t, err)
				require.NoError(t, os.Mkdir(marker, 0700))
				require.Error(t, w.invalidateCleanPropertyEpoch())
				after, err := os.ReadFile(walActivePath(logDir))
				require.NoError(t, err)
				require.Equal(t, before, after, "no control may precede its marker")
				require.NoError(t, os.Remove(marker))
			}
			require.NoError(t, w.invalidateCleanPropertyEpoch())
			require.FileExists(t, marker)
			if !crash {
				require.NoError(t, os.Remove(marker))
				require.NoError(t, os.MkdirAll(filepath.Join(marker, "busy"), 0700))
				w.mu.Lock()
				err = w.sealPropertyEpochsLocked()
				w.mu.Unlock()
				require.Error(t, err)
				require.True(t, w.propertyEpochUnsealed, "a marker that was not removed stays pending")
				return
			}
			_, versions := walEpochRawBodies(t, walActivePath(logDir))
			require.Contains(t, versions, walFormatVersionPropertyEpoch)
			// Crash: neither the engine's clean close nor its conversion runs.
			crashClose(t, b, w)
			b, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "store")}))
			require.NoError(t, err)
			w, err = NewWAL(logDir, DefaultWALConfig())
			require.NoError(t, err)
			fixtureowner.Own(t, w)
			require.True(t, w.propertyEpochUnsealed, "a reopened log keeps the marker")
			require.NoError(t, NewWALEngine(b, w).Close())
			require.NoFileExists(t, marker)
			_, versions = walEpochRawBodies(t, walActivePath(logDir))
			require.NotEmpty(t, versions)
			for _, version := range versions {
				require.Equal(t, walFormatVersionV3, version)
			}
			// Compaction of a sealed log finds nothing to convert.
			w, err = NewWAL(logDir, DefaultWALConfig())
			require.NoError(t, err)
			fixtureowner.Own(t, w)
			require.False(t, w.propertyEpochUnsealed)
			require.NoError(t, writeWALManifest(logDir, &WALManifest{Segments: []WALSegment{{Path: "invalid.wal"}}}))
			w.mu.Lock()
			require.NoError(t, w.sealPropertyEpochsLocked())
			w.mu.Unlock()
		})
	}
}

// Every v4 frame must pass the marker owner; a writer that skipped it would
// leave a control that clean close never converts.
func TestWALPropertyEpochFramesUseMarkerOwner(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var callers []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if name.Name == "writeWALPropertyEpoch" {
					callers = append(callers, fn.Name.Name)
				}
				for _, arg := range call.Args {
					if id, ok := arg.(*ast.Ident); ok && name.Name == "writeAtomicRecordBufio" && id.Name == "walFormatVersionPropertyEpoch" {
						callers = append(callers, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	require.ElementsMatch(t, []string{"writePropertyEpochFrameLocked", "writeWALPropertyEpoch"}, callers)
}

// A foreign import still owes its clean proof even when this synchronous log
// never received a control: close publishes it after the same checks.
func TestWALPropertyEpochSynchronousForeignProofWithoutControl(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	require.NoError(t, markRecoveryPropertyForeign(b))
	require.False(t, b.walPropertySource.clean)
	w, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, NewWALEngine(b, w).Close())
	require.NoFileExists(t, walPropertyEpochUnsealedPath(w.Config().Dir))
	b, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	require.True(t, b.walPropertySource.foreign)
	require.True(t, b.walPropertySource.clean)
}
