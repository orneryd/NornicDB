package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

// walEpochRawBodies reports the physical version of every complete record, the
// raw bytes of each ordinary buffered-commit body and the number of authority
// controls. Current v5 frames carry both controls and ordinary entries, so the
// two are told apart by isWALPropertyEpoch(entry), never by the version byte.
// A record past the last complete boundary is skipped, so a deliberately
// damaged tail never has to be decoded.
func walEpochRawBodies(t *testing.T, path string) (bodies [][]byte, versions []uint8, controls int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	for offset := 0; offset < len(raw); {
		if offset+9 > len(raw) {
			break
		}
		size := int(binary.LittleEndian.Uint32(raw[offset+5 : offset+9]))
		end := offset + int(alignUp(int64(9+size+4+8)))
		if end > len(raw) {
			break
		}
		if binary.LittleEndian.Uint32(raw[offset:offset+4]) != walMagic {
			break
		}
		entry, err := WALEntry{}, error(nil)
		_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, raw[offset+9:offset+9+size], raw[offset+4])
		require.NoError(t, err)
		versions = append(versions, raw[offset+4])
		if isWALPropertyEpoch(entry) {
			controls++
		} else if entry.Operation == OpBufferedCommit {
			bodies = append(bodies, append([]byte(nil), raw[offset:end]...))
		}
		offset = end
	}
	return bodies, versions, controls
}

func TestWALPropertyEpochCleanStopClosesAuthority(t *testing.T) {
	for _, order := range []string{"engine first", "WAL first"} {
		t.Run(order, func(t *testing.T) { testWALPropertyEpochCleanStop(t, order == "WAL first") })
	}
}

// The whole log — authority controls and ordinary entries — stays in the
// current v5 framing. Nothing is downgraded for an older reader; the clean
// close only closes the authority span.
func TestWALPropertyEpochSynchronousRestoreCleanStop(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NORNICDB_WAL_COMPAT_DIR", dir)
	t.Setenv("NORNICDB_WAL_COMPAT_MODE", "write-clean-sync-restore")
	TestWALPublicDowngradeFixture(t)
	_, versions, controls := walEpochRawBodies(t, filepath.Join(dir, "wal", "wal.log"))
	require.NotEmpty(t, versions)
	require.NotZero(t, controls, "the restore attachment must have written its authority control")
	for _, version := range versions {
		require.Equal(t, walFormatVersionCurrent, version, "a clean restore publishes only the current framing")
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
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "close:a", Properties: map[string]any{"alpha": int64(2)}}, ClientNodeEdits))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	path := filepath.Join(logDir, "wal.log")
	before, versions, controls := walEpochRawBodies(t, path)
	require.Contains(t, versions, walFormatVersionCurrent)
	require.NotZero(t, controls, "the acknowledged commit carries its dictionary authority")
	if walFirst {
		require.NoError(t, w.Close())
		require.NoError(t, source.Close())
	} else {
		require.NoError(t, source.Close())
		require.NoError(t, w.Close())
	}
	after, versions, controls := walEpochRawBodies(t, path)
	require.Equal(t, before, after, "closing the authority must copy the ordinary bodies byte for byte")
	require.NotZero(t, controls)
	for _, version := range versions {
		require.Equal(t, walFormatVersionCurrent, version)
	}
	entries, err := ReadWALEntriesFromDir(logDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].PropertyEpoch, "foreign historical replay still has its complete names")
	// Append an unbound current frame directly after the closed authority to
	// verify that the sealed span cannot lend its dictionary to a later body.
	legacy := entries[0]
	legacy.Sequence++
	legacy.PropertyEpoch = nil
	legacy.Data = bytes.Clone(legacy.Data)
	legacy.Checksum = crc32Checksum(legacy.Data)
	var buffer bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &buffer, &legacy, nil, 0)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	writer := bufio.NewWriter(file)
	_, err = writeAtomicRecordBufio(writer, payload, walFormatVersionCurrent)
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, file.Close())
	entries, err = ReadWALEntriesFromDir(logDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Nil(t, entries[1].PropertyEpoch, "a body after the closed span cannot borrow its authority")
}

// Old v1-v4 records and a headerless historical JSON log are refused without
// mutation. Each case places the unsupported bytes after one complete current
// record, because a file with no decodable leading record is exactly the
// torn-tail case the repair owner is allowed to truncate.
func TestWALPropertyEpochOldFramingRefusedWithoutMutation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	data := []byte(`{"tx_id":"old"}`)
	var current bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &current, &WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpCheckpoint, Data: []byte(`{}`), Checksum: crc32Checksum([]byte(`{}`))}, nil, 0)
	require.NoError(t, err)
	lead, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
	records := map[string][]byte{}
	for version := uint8(1); version < walFormatVersionCurrent; version++ {
		entry := WALEntry{Sequence: 2, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: data}
		entry.Checksum = crc32Checksum(entry.Data)
		var payload []byte
		var err error
		if version < 3 {
			payload, err = json.Marshal(entry)
		} else {
			payload, err = processWALEntryPayload(walPayloadEncode, &bytes.Buffer{}, &entry, nil, 0)
		}
		require.NoError(t, err)
		record, _ := buildAtomicRecord(payload, version)
		records[fmt.Sprintf("version %d", version)] = append(bytes.Clone(lead), record...)
	}
	records["headerless json"] = append(bytes.Clone(lead), []byte(`{"seq":2,"op":"create_node"}`+"\n")...)
	for name, raw := range records {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := walActivePath(dir)
			require.NoError(t, os.WriteFile(path, raw, 0600))
			w, err := NewWAL(dir, DefaultWALConfig())
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			require.Nil(t, w)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after, "a refused WAL must not be repaired or converted")
			require.NoFileExists(t, walPropertyEpochUnsealedPath(dir))
			// A second open must refuse the same bytes again rather than
			// publish a converted copy next to them.
			_, err = NewWAL(dir, DefaultWALConfig())
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			after, err = os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after)
			// Recovery must also refuse before restoring any snapshot.
			destination := newTestMemoryEngine(t)
			_, _, err = RecoverIntoEngine(destination, dir, "")
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			count, err := destination.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
	// The producer routes of an already opened log must refuse the framing when
	// the file underneath them is replaced by a mixed old-format log.
	t.Run("producer routes", func(t *testing.T) {
		mixed := records[fmt.Sprintf("version %d", walFormatVersionV3)]
		source, _, commit := bindingCostFixture(t, 1)
		wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		require.NoError(t, os.WriteFile(walActivePath(wal.Config().Dir), mixed, 0600))
		wal.mu.Lock()
		wal.propertyEpochFormatChecked, wal.propertyEpochKnown = false, false
		wal.mu.Unlock()
		require.ErrorIs(t, wal.AppendBufferedCommit(commit, source.propKeyDict), ErrWALUnsupportedVersion)
		after, err := os.ReadFile(walActivePath(wal.Config().Dir))
		require.NoError(t, err)
		require.Equal(t, mixed, after, "a refused producer route writes nothing")
	})
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

// Truncation after a snapshot drops the control that carried authority. A new
// foreign restore writes the unsealed marker before publishing its own control,
// so a crash during restore cannot expose the previous clean span; the next
// clean close seals that control in place.
func TestWALPropertyEpochRestoreRearmsAuthority(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dest := reviewBindingEngine(t, t.TempDir())
	w, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	dest.SetBufferedCommitWAL(w)
	snapshot, err := w.CreateSnapshot(dest)
	require.NoError(t, err)
	require.NoError(t, SaveSnapshot(snapshot, filepath.Join(t.TempDir(), "empty.snapshot")))
	require.NoError(t, w.ManageSnapshot(snapshot.Sequence, WALSnapshotTruncate))
	_, versions, controls := walEpochRawBodies(t, walActivePath(w.Config().Dir))
	require.Zero(t, controls, "the truncated log retains no authority control")
	require.NoError(t, dest.beginSemanticPropertyRestore())
	require.FileExists(t, walPropertyEpochUnsealedPath(w.Config().Dir), "a crash during restore must not expose the old clean log")
	_, versions, controls = walEpochRawBodies(t, walActivePath(w.Config().Dir))
	require.NotZero(t, controls, "the restore publishes its new authority in the current framing")
	for _, version := range versions {
		require.Equal(t, walFormatVersionCurrent, version)
	}
	// The clean close seals that control instead of leaving an open span.
	require.NoError(t, dest.Close())
	require.NoError(t, w.Close())
	require.NoFileExists(t, walPropertyEpochUnsealedPath(w.Config().Dir))
	_, versions, controls = walEpochRawBodies(t, walActivePath(w.Config().Dir))
	require.NotZero(t, controls, "sealing keeps the control and closes its span")
	for _, version := range versions {
		require.Equal(t, walFormatVersionCurrent, version)
	}
}

func TestWALPropertyEpochFailedConversionKeepsOriginal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.NoError(t, wal.Close())
	path := walActivePath(wal.Config().Dir)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	// A truncated final ordinary record must not cause the preceding authority
	// control to be sealed in place before complete validation succeeds.
	raw = raw[:len(raw)-1]
	require.NoError(t, os.WriteFile(path, raw, 0600))
	require.Error(t, sealWALPropertyEpochFile(path))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}

// Verify current and unsupported recovery files under the same seal-plan contract.
func TestWALPropertyEpochSealPlanVisitsEveryRecoveryFile(t *testing.T) {
	t.Run("unsupported framing", func(t *testing.T) {
		t.Cleanup(config.WithWALEnabled())
		dir := t.TempDir()
		wal, err := NewWAL(dir, DefaultWALConfig())
		require.NoError(t, err)
		require.NoError(t, wal.Close())
		legacy := []byte(`{"tx_id":"legacy"}` + "\n")
		require.NoError(t, os.WriteFile(walActivePath(dir), legacy, 0600))
		require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0700))
		segment := filepath.Join(walSegmentsDir(dir), "seg-00000000000000000003-00000000000000000003.wal")
		entry := WALEntry{Sequence: 3, Timestamp: time.Now(), Operation: OpCreateNode, Data: []byte(`{}`)}
		entry.Checksum = crc32Checksum(entry.Data)
		var buffer bytes.Buffer
		payload, err := processWALEntryPayload(walPayloadEncode, &buffer, &entry, nil, 0)
		require.NoError(t, err)
		old, _ := buildAtomicRecord(payload, walFormatVersionV3)
		require.NoError(t, os.WriteFile(segment, old, 0600))
		require.NoError(t, writeWALManifest(dir, &WALManifest{Segments: []WALSegment{{Path: filepath.Base(segment), FirstSeq: 3, LastSeq: 3}}}))
		var seen []uint64
		wal.mu.Lock()
		plans, err := wal.planPropertyEpochSealLocked(func(entry WALEntry) error {
			seen = append(seen, entry.Sequence)
			return nil
		})
		wal.mu.Unlock()
		require.ErrorIs(t, err, ErrWALUnsupportedVersion, "old framing in a recovery file stops the plan")
		require.Nil(t, plans)
		require.Empty(t, seen, "no entry is visited before the framing check refuses the file")
		after, err := os.ReadFile(segment)
		require.NoError(t, err)
		require.Equal(t, old, after, "an old segment is never rewritten")
		after, err = os.ReadFile(walActivePath(dir))
		require.NoError(t, err)
		require.Equal(t, legacy, after, "a legacy active file is never rewritten")

	})
	t.Run("current framing", func(t *testing.T) {
		t.Cleanup(config.WithWALEnabled())
		dir := t.TempDir()
		wal, err := NewWAL(dir, DefaultWALConfig())
		require.NoError(t, err)
		require.NoError(t, wal.Close())
		require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0700))
		segment := filepath.Join(walSegmentsDir(dir), "seg-00000000000000000003-00000000000000000003.wal")
		segmentEntry := WALEntry{Sequence: 3, Timestamp: time.Now(), Operation: OpCreateNode, Data: []byte(`{}`)}
		segmentEntry.Checksum = crc32Checksum(segmentEntry.Data)
		var buffer bytes.Buffer
		payload, err := processWALEntryPayload(walPayloadEncode, &buffer, &segmentEntry, nil, 0)
		require.NoError(t, err)
		segmentRecord, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
		require.NoError(t, os.WriteFile(segment, segmentRecord, 0600))
		activeEntry := WALEntry{Sequence: 9, Timestamp: time.Now(), Operation: OpCreateNode, Data: []byte(`{}`)}
		activeEntry.Checksum = crc32Checksum(activeEntry.Data)
		payload, err = processWALEntryPayload(walPayloadEncode, &buffer, &activeEntry, nil, 0)
		require.NoError(t, err)
		activeRecord, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
		require.NoError(t, os.WriteFile(walActivePath(dir), activeRecord, 0600))
		require.NoError(t, writeWALManifest(dir, &WALManifest{Segments: []WALSegment{{Path: filepath.Base(segment), FirstSeq: 3, LastSeq: 3}}}))
		var seen []uint64
		wal.mu.Lock()
		plans, err := wal.planPropertyEpochSealLocked(func(entry WALEntry) error {
			seen = append(seen, entry.Sequence)
			return nil
		})
		wal.mu.Unlock()
		require.NoError(t, err)
		require.Equal(t, []uint64{3, 9}, seen, "the listed segment and the active file are visited once each")
		require.Len(t, plans, 2)
		for _, plan := range plans {
			require.Empty(t, plan.controls, "an ordinary current record is not an authority control")
		}
		after, err := os.ReadFile(walActivePath(dir))
		require.NoError(t, err)
		require.Equal(t, activeRecord, after, "a plan without controls rewrites nothing")
		wal.mu.Lock()
		_, err = wal.planPropertyEpochSealLocked(func(WALEntry) error { return errors.New("veto") })
		wal.mu.Unlock()
		require.ErrorContains(t, err, "veto")

	})
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
	_, _, controls := walEpochRawBodies(t, path)
	require.Equal(t, len(plan.controls), controls, "a control is recognized by payload, not by framing version")
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
	require.Equal(t, append(raw, 0), after, "a rejected plan leaves the original log in place")
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
			_, versions, controls := walEpochRawBodies(t, walActivePath(logDir))
			require.NotZero(t, controls)
			for _, version := range versions {
				require.Equal(t, walFormatVersionCurrent, version)
			}
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
			_, versions, controls = walEpochRawBodies(t, walActivePath(logDir))
			require.NotEmpty(t, versions)
			require.NotZero(t, controls, "sealing closes the control span instead of removing it")
			for _, version := range versions {
				require.Equal(t, walFormatVersionCurrent, version)
			}
			// Compaction of a sealed log finds nothing left to convert.
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

// Every authority control must pass the marker owner; a writer that skipped it
// would leave a control that clean close never seals.
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
