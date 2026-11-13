package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG 42: concurrent journal appenders break the segment roll-over
// ====================================================================================
// Discovered: 12 Nov 2026, real-disk measurements of a build with the product
// files of main 5b593da1e.
// Impact: with two or more concurrent appenders (client transactions, the
// embedding worker, write-behind commits, checkpoints), a roll-over refused the
// segment it had just sealed ("unlisted segment ... overlaps the manifest"),
// left the active file closed so every later append failed ("file already
// closed"), and the journal did not reopen.
// Root Cause: the append paths took a record's sequence number before w.mu, the
// lock that orders record writes and roll-overs. Records reached the file out of
// sequence order, and a roll-over between two of them sealed a segment whose
// range overlapped the one before it.
// ====================================================================================

// rollOverAck is what an appender was told about one record it appended.
type rollOverAck struct {
	seq   uint64
	value []byte
}

// rollOverAcks collects every acknowledged record by its identity.
type rollOverAcks struct {
	mu     sync.Mutex
	byID   map[string]rollOverAck
	errors []string
}

func (a *rollOverAcks) ack(id string, seq uint64, value []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, dup := a.byID[id]; dup {
		a.errors = append(a.errors, "identity acknowledged twice: "+id)
	}
	a.byID[id] = rollOverAck{seq: seq, value: value}
}

func (a *rollOverAcks) fail(format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.errors = append(a.errors, fmt.Sprintf(format, args...))
}

func rollOverValue(id string, size int) []byte {
	value := make([]byte, 0, len(id)+1+size)
	value = append(value, id...)
	value = append(value, ':')
	for i := 0; i < size; i++ {
		value = append(value, byte('a'+(len(id)+i)%26))
	}
	return value
}

// rollOverIdentity reads the identity and value one test appender gave a
// record, or ok=false for a record the test did not identify (checkpoints).
func rollOverIdentity(t *testing.T, entry WALEntry) (id string, value []byte, ok bool) {
	t.Helper()
	switch entry.Operation {
	case OpTxCommit:
		var data WALTxData
		require.NoError(t, json.Unmarshal(entry.Data, &data), "seq %d", entry.Sequence)
		return data.TxID, []byte(data.Metadata["v"]), true
	case OpBufferedCommit:
		data, err := decodeBufferedWALRecord(entry)
		require.NoError(t, err)
		return data.TxID, data.Writes["rollover:"+data.TxID], true
	}
	return "", nil, false
}

// rollOverJournalRangeErrors describes every way the sealed segments of the
// journal in dir disagree with the records they hold: a segment whose name or
// manifest entry does not state exactly its first and last record's numbers,
// a segment that does not begin after the one before it, records out of
// sequence order in file order, and a reopened journal that does not resume
// at its last record (or at the manifest's retired number, when that is
// higher). It returns the number the journal should resume at: the last
// record's number, or the retired number when that is higher.
func rollOverJournalRangeErrors(t *testing.T, dir string, reopened *WAL) (problems []string, resume uint64) {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	for i, entry := range entries {
		if i > 0 && entry.Sequence <= entries[i-1].Sequence {
			problems = append(problems, fmt.Sprintf("record %d (seq %d) does not follow seq %d in file order", i, entry.Sequence, entries[i-1].Sequence))
		}
		resume = max(resume, entry.Sequence)
	}
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	resume = max(resume, manifest.RetiredThrough)
	var previousLast uint64
	for _, segment := range manifest.Segments {
		if first, last, ok := parseSegmentFilename(segment.Path); !ok || first != segment.FirstSeq || last != segment.LastSeq {
			problems = append(problems, fmt.Sprintf("%s: its name and its manifest range %d..%d differ", segment.Path, segment.FirstSeq, segment.LastSeq))
		}
		if segment.FirstSeq <= previousLast {
			problems = append(problems, fmt.Sprintf("%s overlaps the segment before it (ending at %d)", segment.Path, previousLast))
		}
		path, err := resolveWALSegmentPath(dir, segment.Path)
		require.NoError(t, err)
		sealed, err := ReadWALEntries(t.Context(), path, nil)
		require.NoError(t, err)
		switch {
		case len(sealed) == 0:
			problems = append(problems, fmt.Sprintf("%s holds no record", segment.Path))
		case sealed[0].Sequence != segment.FirstSeq || sealed[len(sealed)-1].Sequence != segment.LastSeq:
			problems = append(problems, fmt.Sprintf("%s states %d..%d but holds records %d..%d", segment.Path, segment.FirstSeq, segment.LastSeq, sealed[0].Sequence, sealed[len(sealed)-1].Sequence))
		}
		previousLast = segment.LastSeq
	}
	if reopened.Sequence() != resume {
		problems = append(problems, fmt.Sprintf("the reopened journal resumes at %d, it should resume at %d", reopened.Sequence(), resume))
	}
	return problems, resume
}

// requireRollOverJournal reopens the journal in dir and requires that it
// opens, that its sealed ranges, segment names, manifest and resumed number
// agree exactly with the records written (rollOverJournalRangeErrors), and
// that every acknowledged record is present once with its value and, where
// the appender was told one, its sequence.
func requireRollOverJournal(t *testing.T, dir string, cfg *WALConfig, acks *rollOverAcks) {
	t.Helper()
	reopened, err := NewWAL(dir, cfg)
	require.NoError(t, err, "the journal must reopen after concurrent roll-overs")
	// Released if an assertion below fails; a repeated Close joins the one stop.
	t.Cleanup(func() { _ = reopened.Close() })
	problems, highest := rollOverJournalRangeErrors(t, dir, reopened)
	require.Empty(t, problems, "the journal's ranges must agree with the records written")
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)

	found := make(map[string]bool, len(acks.byID))
	for _, entry := range entries {
		id, value, ok := rollOverIdentity(t, entry)
		if !ok {
			continue
		}
		want, acked := acks.byID[id]
		if !acked {
			continue
		}
		require.False(t, found[id], "record %s is in the journal twice", id)
		found[id] = true
		if want.seq != 0 { // a buffered commit is not told its sequence
			require.Equal(t, want.seq, entry.Sequence, "record %s has the sequence its appender was given", id)
		}
		require.True(t, bytes.Equal(want.value, value), "record %s has the value its appender wrote", id)
	}
	for id := range acks.byID {
		require.True(t, found[id], "acknowledged record %s is missing after reopen", id)
	}

	next, err := reopened.AppendReturningSeq(OpCheckpoint, map[string]any{"after": "reopen"})
	require.NoError(t, err)
	require.Greater(t, next, highest, "the first append after reopen continues the numbering")
	require.NoError(t, reopened.Close())
}

// TestWALRollOverJournalAssertionRejectsAnOverstatedRange shows that two
// checks of rollOverJournalRangeErrors can fail: a segment's stated range
// against its records, and the resume number. "range": a segment named and
// listed 1..100 that holds only record 1 must be reported for its range.
// "resume": a correct one-segment journal whose reopened WAL reports a number
// other than its last record's must be reported for the resumed number.
func TestWALRollOverJournalAssertionRejectsAnOverstatedRange(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	cfg := DefaultWALConfig()
	cfg.MaxEntries, cfg.MaxFileSize = 0, 0
	sealOne := func(t *testing.T, last uint64) string {
		dir := t.TempDir()
		w, err := NewWAL(dir, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		_, err = w.AppendReturningSeq(OpCheckpoint, map[string]any{"only": "record"})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		name := fmt.Sprintf("seg-%020d-%020d.wal", 1, last)
		require.NoError(t, os.Rename(walActivePath(dir), filepath.Join(walSegmentsDir(dir), name)))
		_, err = accessWALManifest(dir, walPublishManifest, &WALManifest{Segments: []WALSegment{{Path: name, FirstSeq: 1, LastSeq: last}}})
		require.NoError(t, err)
		return dir
	}
	t.Run("range", func(t *testing.T) {
		dir := sealOne(t, 100)
		reopened, err := NewWAL(dir, cfg)
		require.NoError(t, err)
		defer func() { _ = reopened.Close() }()
		problems, _ := rollOverJournalRangeErrors(t, dir, reopened)
		require.Len(t, problems, 1, "%v", problems)
		require.Contains(t, problems[0], "states 1..100 but holds records 1..1")
	})
	t.Run("resume", func(t *testing.T) {
		dir := sealOne(t, 1)
		reopened, err := NewWAL(dir, cfg)
		require.NoError(t, err)
		defer func() { _ = reopened.Close() }()
		problems, _ := rollOverJournalRangeErrors(t, dir, reopened)
		require.Empty(t, problems, "the correct journal itself passes")
		reopened.sequence.Store(100) // as if the reopen had resumed at a number no record has
		problems, _ = rollOverJournalRangeErrors(t, dir, reopened)
		require.Len(t, problems, 1, "%v", problems)
		require.Contains(t, problems[0], "resumes at 100, it should resume at 1")
	})
}

// TestWALRollOverWithConcurrentAppenders runs eight appenders at once (three
// ordinary appenders, the path client transaction markers, the embedding
// worker and replication use; three BatchWriter committers; two write-behind
// buffered-commit appenders) beside checkpoints and the retention seal a
// snapshot performs. It requires that no append, checkpoint or snapshot call
// fails, that at least one checkpoint and one snapshot call complete while an
// appender is running from the call's start to its return (with retention, a
// snapshot call seals the active segment when the segment holds records), that
// the journal has more than four sealed segments, and that it passes
// requireRollOverJournal.
func TestWALRollOverWithConcurrentAppenders(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dictionary := newTestEngine(t).propKeyDict

	cases := []struct {
		name       string
		size       int
		iterations int
		maxBytes   int64
		maxEntries int64
	}{
		{name: "small_records_entry_limit", size: 200, iterations: 120, maxEntries: 32},
		{name: "large_records_byte_limit", size: 128 << 10, iterations: 12, maxBytes: 1 << 20},
	}
	const appenders = 8
	for _, tc := range cases {
		for _, syncMode := range []string{"immediate", "batch"} {
			t.Run(tc.name+"/"+syncMode, func(t *testing.T) {
				dir := t.TempDir()
				cfg := DefaultWALConfig()
				cfg.SyncMode = syncMode
				cfg.BatchSyncInterval = 5 * time.Millisecond
				cfg.MaxFileSize = tc.maxBytes
				cfg.MaxEntries = tc.maxEntries
				// With retention, a snapshot call seals the active segment with a
				// roll-over when the segment holds records.
				cfg.RetentionMaxSegments = 1 << 20
				wal, err := NewWAL(dir, cfg)
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })

				acks := &rollOverAcks{byID: make(map[string]rollOverAck)}
				var writers sync.WaitGroup
				// running counts the appenders that have not returned; it only
				// falls, so a positive count after a call means an appender ran
				// from the call's start to its return.
				var running atomic.Int64
				for g := 0; g < appenders; g++ {
					writers.Add(1)
					running.Add(1)
					go func(g int) {
						defer writers.Done()
						defer running.Add(-1)
						for i := 0; i < tc.iterations; i++ {
							id := fmt.Sprintf("w%d-%d", g, i)
							switch g % 3 {
							case 0: // ordinary append
								value := rollOverValue(id, tc.size)
								seq, err := wal.AppendWithDatabaseReturningSeq(OpTxCommit, WALTxData{TxID: id, Metadata: map[string]string{"v": string(value)}}, "test")
								if err != nil {
									acks.fail("append %s: %v", id, err)
									return
								}
								acks.ack(id, seq, value)
							case 1: // BatchWriter commit of three records
								batch := wal.NewBatch()
								values := make([][]byte, 3)
								for k := range values {
									part := fmt.Sprintf("%s.%d", id, k)
									values[k] = rollOverValue(part, tc.size/3)
									if err := batch.append(OpTxCommit, WALTxData{TxID: part, Metadata: map[string]string{"v": string(values[k])}}); err != nil {
										acks.fail("batch append %s: %v", part, err)
										return
									}
								}
								first, last, err := batch.CommitWithSeq()
								if err != nil {
									acks.fail("batch %s: %v", id, err)
									return
								}
								if last-first != 2 {
									acks.fail("batch %s numbered %d..%d", id, first, last)
								}
								for k := range values {
									acks.ack(fmt.Sprintf("%s.%d", id, k), first+uint64(k), values[k])
								}
							case 2: // write-behind buffered commit (forced flush)
								value := rollOverValue(id, tc.size)
								commit := bufferedCommit{txID: id, logicalTxID: id, namespace: "test", writes: map[string][]byte{"rollover:" + id: value}}
								if err := wal.AppendBufferedCommit(&commit, dictionary); err != nil {
									acks.fail("buffered commit %s: %v", id, err)
									return
								}
								acks.ack(id, 0, value) // a buffered commit is not told its sequence
							}
						}
					}(g)
				}

				stop := make(chan struct{})
				// Only checkpoints and snapshot calls that complete while an
				// appender runs from their start to their return are counted.
				var checkpointsDuring, snapshotsDuring atomic.Int64
				var background sync.WaitGroup
				background.Add(2)
				go func() { // checkpoints, as snapshots and compaction write them
					defer background.Done()
					ticker := time.NewTicker(2 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							before := running.Load() > 0
							if err := wal.Checkpoint(); err != nil {
								acks.fail("checkpoint: %v", err)
								return
							}
							if before && running.Load() > 0 {
								checkpointsDuring.Add(1)
							}
						}
					}
				}()
				go func() { // snapshot calls; with retention each seals the active segment at the current number when it holds records
					defer background.Done()
					ticker := time.NewTicker(15 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							before := running.Load() > 0
							if err := wal.ManageSnapshot(t.Context(), 0, WALSnapshotTruncate); err != nil {
								acks.fail("snapshot call: %v", err)
								return
							}
							if before && running.Load() > 0 {
								snapshotsDuring.Add(1)
							}
						}
					}
				}()
				writers.Wait()
				close(stop)
				background.Wait()
				require.Empty(t, acks.errors, "no journal write may fail because of a roll-over")
				t.Logf("checkpoints %d and snapshot calls %d completed with an appender running from their start to their return", checkpointsDuring.Load(), snapshotsDuring.Load())
				require.Positive(t, checkpointsDuring.Load(), "a checkpoint must complete while an appender runs")
				require.Positive(t, snapshotsDuring.Load(), "a snapshot call must complete while an appender runs")
				require.NoError(t, wal.Close())

				manifest, err := accessWALManifest(dir, walReadManifest, nil)
				require.NoError(t, err)
				require.Greater(t, len(manifest.Segments), 4, "the case must roll the journal over several times")

				requireRollOverJournal(t, dir, cfg, acks)
			})
		}
	}
}

// rollOverKillChildEnv names the journal directory of the killed child, and
// rollOverKillModeEnv its mode.
const (
	rollOverKillChildEnv = "NORNIC_WAL_ROLLOVER_KILL_DIR"
	rollOverKillModeEnv  = "NORNIC_WAL_ROLLOVER_KILL_MODE"
)

// TestWALRollOverSurvivesKill kills, twelve times per mode, a process that
// appends from four goroutines with a roll-over every 3 records, at a later
// moment each round: after a delay of 3 ms + 11 ms per round from its first
// acknowledgement, right after the first answer showing it has completed more
// appends than in its answer at that acknowledgement (killRollOverChild).
// After every kill the journal must pass requireRollOverJournal with every
// record the child acknowledged. In the durable mode ("immediate" sync) every
// append is acknowledged; in the write-behind mode (batch sync, deferred append
// flush) the buffered commits are, beside ordinary appends that the mode does
// not make durable. Where in a roll-over each kill landed is recorded in the
// test log, not required.
func TestWALRollOverSurvivesKill(t *testing.T) {
	if os.Getenv(rollOverKillChildEnv) != "" {
		t.Skip("child process")
	}
	t.Cleanup(config.WithWALEnabled())
	const rounds = 12
	for _, mode := range []string{"durable", "write_behind"} {
		t.Run(mode, func(t *testing.T) {
			states := map[string]int{}
			for round := 0; round < rounds; round++ {
				dir := filepath.Join(t.TempDir(), "wal")
				acks := runRollOverKillChild(t, dir, mode, time.Duration(3+round*11)*time.Millisecond)
				states[rollOverDiskState(dir)]++
				requireRollOverJournal(t, dir, rollOverKillConfig(mode), acks)
			}
			t.Logf("on-disk state at the kills: %v", states)
		})
	}
}

func rollOverKillConfig(mode string) *WALConfig {
	cfg := DefaultWALConfig()
	cfg.MaxEntries = 3
	if mode == "write_behind" {
		cfg.SyncMode = "batch"
		cfg.BatchSyncInterval = 2 * time.Millisecond
		cfg.DeferAppendFlush = true
		return cfg
	}
	cfg.SyncMode = "immediate"
	return cfg
}

// rollOverDiskState names where in a roll-over the kill left the journal.
func rollOverDiskState(dir string) string {
	if _, err := os.Stat(walManifestPath(dir) + ".tmp"); err == nil {
		return "manifest_publication"
	}
	if _, err := os.Stat(walActivePath(dir)); os.IsNotExist(err) {
		return "sealed_without_active_file"
	}
	manifest := WALManifest{}
	if raw, err := os.ReadFile(walManifestPath(dir)); err == nil {
		_ = json.Unmarshal(raw, &manifest)
	}
	scanned, _ := scanSegmentsFromDir(dir)
	if len(scanned) > len(manifest.Segments) {
		return "renamed_not_listed"
	}
	return "between_roll_overs"
}

// runRollOverKillChild starts this test binary as an appending child, reads
// its acknowledgements and kills it once, after waiting the given delay from
// its first acknowledgement, the child has reported more completed appends
// (killRollOverChild). The round fails unless the child's answers show it
// completed more appends before the kill.
func runRollOverKillChild(t *testing.T, dir, mode string, delay time.Duration) *rollOverAcks {
	t.Helper()
	acks, err := killRollOverChild(t, dir, mode, delay)
	require.NoError(t, err)
	return acks
}

// killRollOverChild is runRollOverKillChild returning, instead of failing, an
// error for a round that fails, with what the child wrote to stderr. The
// parent asks the child for its count of completed appends ("PING" on stdin,
// "ALIVE <count>" on stdout) at its first acknowledgement and again after the
// delay, repeating the second question until the count has grown, and kills
// at once after that answer. The round fails when an answer shows no growth
// more than ten seconds after the repeats began, or when any answer takes
// longer than ten seconds. A child that has exited, or has died from a signal,
// before a question is asked cannot answer it, and the round fails; a child
// that stops after its last answer and before the kill is not detected.
func killRollOverChild(t *testing.T, dir, mode string, delay time.Duration, env ...string) (*rollOverAcks, error) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestWALRollOverKillChild$", "-test.count=1")
	command.Env = append(append(os.Environ(), rollOverKillChildEnv+"="+dir, rollOverKillModeEnv+"="+mode), env...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	// The child is killed and reaped below on every path; this also ends it if
	// the round stops early. Kill of a reaped process and a second Wait only
	// return errors.
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	acks := &rollOverAcks{byID: make(map[string]rollOverAck)}
	alive := make(chan int64, 4)
	first := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var line []byte
		buf := make([]byte, 4096)
		signalled := false
		for {
			n, err := stdout.Read(buf)
			for _, b := range buf[:n] {
				if b != '\n' {
					line = append(line, b)
					continue
				}
				var id, value string
				var seq uint64
				var appended int64
				if _, scanErr := fmt.Sscanf(string(line), "ALIVE %d", &appended); scanErr == nil {
					alive <- appended
				} else if _, scanErr := fmt.Sscanf(string(line), "ACK %s %d %s", &id, &seq, &value); scanErr == nil {
					acks.ack(id, seq, []byte(value))
					if !signalled {
						signalled = true
						close(first)
					}
				}
				line = line[:0]
			}
			if err != nil {
				return
			}
		}
	}()
	ping := func() (int64, bool) {
		if _, err := io.WriteString(stdin, "PING\n"); err != nil {
			return 0, false
		}
		select {
		case n := <-alive:
			return n, true
		case <-done:
			return 0, false
		case <-time.After(10 * time.Second):
			return 0, false
		}
	}
	var stillAppending error
	select {
	case <-first:
		before, ok := ping()
		time.Sleep(delay)
		for deadline := time.Now().Add(10 * time.Second); ok; time.Sleep(time.Millisecond) {
			var after int64
			if after, ok = ping(); !ok || after > before {
				break
			}
			if time.Now().After(deadline) {
				stillAppending = fmt.Errorf("the child completed no append in ten seconds (count %d)", after)
				break
			}
		}
		if !ok {
			stillAppending = fmt.Errorf("the child did not answer before the kill")
		}
	case <-done:
		stillAppending = fmt.Errorf("the child closed its output before its first acknowledgement")
	case <-time.After(2 * time.Minute):
		stillAppending = fmt.Errorf("the child acknowledged nothing within two minutes")
	}
	killErr := command.Process.Kill()
	_ = stdin.Close()
	<-done
	_ = command.Wait()
	if stillAppending == nil && killErr != nil {
		stillAppending = fmt.Errorf("kill: %w", killErr)
	}
	if stillAppending != nil {
		return acks, fmt.Errorf("%w (exit %d): %s", stillAppending, command.ProcessState.ExitCode(), stderr.String())
	}
	require.NotEmpty(t, acks.byID, "the child acknowledged nothing before the kill")
	require.Empty(t, acks.errors)
	return acks, nil
}

// rollOverKillEndEarlyEnv makes the killed child end right after its first
// acknowledgement (TestWALRollOverKillRejectsAChildThatStopped): "exit" exits
// with status 3 as on an append error, "signal" kills the child's own process.
const rollOverKillEndEarlyEnv = "NORNIC_WAL_ROLLOVER_KILL_END_EARLY"

// TestWALRollOverKillRejectsAChildThatStopped: a child that acknowledges a
// record and ends right after it, by exiting or by dying from a signal, must
// fail the round.
func TestWALRollOverKillRejectsAChildThatStopped(t *testing.T) {
	if os.Getenv(rollOverKillChildEnv) != "" {
		t.Skip("child process")
	}
	t.Cleanup(config.WithWALEnabled())
	for _, how := range []string{"exit", "signal"} {
		t.Run(how, func(t *testing.T) {
			_, err := killRollOverChild(t, filepath.Join(t.TempDir(), "wal"), "durable", 2*time.Second, rollOverKillEndEarlyEnv+"="+how)
			require.Error(t, err, "a child that stopped before the kill must not pass")
			t.Logf("rejected: %v", err)
		})
	}
}

// TestWALRollOverKillChild is the killed child of TestWALRollOverSurvivesKill.
// It prints "ACK <id> <sequence> <value>" after each acknowledged append.
func TestWALRollOverKillChild(t *testing.T) {
	dir := os.Getenv(rollOverKillChildEnv)
	if dir == "" {
		t.Skip("run by TestWALRollOverSurvivesKill")
	}
	t.Cleanup(config.WithWALEnabled())
	mode := os.Getenv(rollOverKillModeEnv)
	dictionary := newTestEngine(t).propKeyDict
	wal, err := NewWAL(dir, rollOverKillConfig(mode))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	var out sync.Mutex
	var appended atomic.Int64
	go func() { // answers the parent's liveness question (killRollOverChild)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if scanner.Text() == "PING" {
				out.Lock()
				fmt.Printf("ALIVE %d\n", appended.Load())
				out.Unlock()
			}
		}
	}()
	var writers sync.WaitGroup
	for g := 0; g < 4; g++ {
		writers.Add(1)
		go func(g int) {
			defer writers.Done()
			buffered := mode == "write_behind" && g%2 == 0
			for i := 0; ; i++ {
				id := fmt.Sprintf("k%d-%d", g, i)
				value := strings.Repeat(string(rune('a'+g)), 64) + id
				var seq uint64
				var err error
				if buffered {
					commit := bufferedCommit{txID: id, logicalTxID: id, namespace: "test", writes: map[string][]byte{"rollover:" + id: []byte(value)}}
					err = wal.AppendBufferedCommit(&commit, dictionary)
				} else {
					seq, err = wal.AppendWithDatabaseReturningSeq(OpTxCommit, WALTxData{TxID: id, Metadata: map[string]string{"v": value}}, "test")
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "append %s: %v\n", id, err)
					os.Exit(3)
				}
				appended.Add(1)
				if mode == "write_behind" && !buffered {
					continue // not durable on return in this mode; not acknowledged
				}
				out.Lock()
				fmt.Printf("ACK %s %d %s\n", id, seq, value)
				switch os.Getenv(rollOverKillEndEarlyEnv) {
				case "exit":
					fmt.Fprintf(os.Stderr, "after %s: ends by exiting\n", id)
					os.Exit(3)
				case "signal":
					fmt.Fprintf(os.Stderr, "after %s: ends by killing itself\n", id)
					self, _ := os.FindProcess(os.Getpid())
					_ = self.Kill()
					select {} // the signal ends the process
				}
				out.Unlock()
			}
		}(g)
	}
	writers.Wait()
}
