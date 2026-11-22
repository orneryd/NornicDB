//go:build closeout_l2_e21

package storage

// Report entry 21: through the storage engine's entity-commit path
// (BeginEntityTransaction, PrepareEntityCommit, WALEngine.SubmitEntityCommit),
// a commit over the opt-in memory policy limit (case A, write-behind on) and a
// commit with a label longer than 65,000 bytes (case B, write-behind off) were
// written to the journal and then ended "committed-pending or unknown"; the
// nodes were not in the store after a flush or a reopen. Expected: such a
// commit is refused definitely before anything of it is journaled, or it is
// applied.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/memory"
)

type l2E21Case struct {
	name        string
	writeBehind bool
	policy      bool
	logicalID   string
	nodes       []*Node
	label       string
}

func l2E21MemoryCase(text int, logicalID string) l2E21Case {
	c := l2E21Case{name: "A-memory", writeBehind: true, policy: true, logicalID: logicalID, label: "R9"}
	payload := strings.Repeat("x", text)
	for i := 0; i < 10; i++ {
		c.nodes = append(c.nodes, &Node{ID: NodeID(fmt.Sprintf("test:m-%d", i)), Labels: []string{"R9"}, Properties: map[string]any{"text": payload}})
	}
	return c
}

func l2E21KeyCase(labelLen int, logicalID string) l2E21Case {
	label := strings.Repeat("L", labelLen)
	return l2E21Case{name: "B-key", logicalID: logicalID, label: label,
		nodes: []*Node{{ID: "test:k", Labels: []string{label}, Properties: map[string]any{"a": int64(1)}}}}
}

func l2E21Policy(t *testing.T) *memory.Policy {
	t.Helper()
	sampler := func() (memory.Sample, error) { return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil }
	fixed := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	policy := memory.NewPolicy(1<<20, 0, time.Hour, sampler, func() time.Time { return fixed })
	if err := policy.Refresh(); err != nil {
		t.Fatalf("policy refresh: %v", err)
	}
	return policy
}

func l2E21OpenEngine(t *testing.T, dir string, writeBehind bool, policy *memory.Policy, manualDrain bool) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: writeBehind, WriteBehindInterval: time.Hour, MemoryPolicy: policy})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if manualDrain && engine.writeBehind != nil {
		engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		engine.writeBehind.Close()
	}
	return engine
}

func l2E21OpenWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	cfg := DefaultWALConfig()
	cfg.Dir = filepath.Join(dir, "wal")
	cfg.BatchSyncInterval = time.Hour
	cfg.RecordPolicy = WALRecordsStandard
	wal, err := NewWAL(cfg.Dir, cfg)
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	return wal
}

func l2E21Records(t *testing.T, dir, logicalID string) int {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	if err != nil {
		t.Logf("read journal: %v", err)
		return -1
	}
	n := 0
	for _, entry := range entries {
		if entry.Operation != OpEntityCommit {
			continue
		}
		var data walEntityCommitData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			t.Logf("decode entity commit: %v", err)
			continue
		}
		envelope, _, err := DecodeEntityTransaction(data.Payload)
		if err != nil {
			t.Logf("decode envelope: %v", err)
			continue
		}
		if envelope.LogicalTxID == logicalID {
			n++
		}
	}
	return n
}

func l2E21Count(t *testing.T, engine *BadgerEngine, c l2E21Case) (byLabel, byID int) {
	t.Helper()
	ctx := context.Background()
	if nodes, err := engine.GetNodesByLabel(ctx, c.label); err == nil {
		byLabel = len(nodes)
	} else {
		t.Logf("count by label: %v", err)
	}
	for _, n := range c.nodes {
		if _, err := engine.GetNode(ctx, n.ID); err == nil {
			byID++
		}
	}
	return byLabel, byID
}

type l2E21Result struct {
	prepareErr  error
	submitErr   error
	outcome     CommitOutcome
	pending     bool // the outcome is an accepted, committed-pending/unknown one
	records     int
	afterFlush  int
	afterReopen int
	recovered   int
	recoverErr  error
	afterRecov  int
}

func (r l2E21Result) String() string {
	return fmt.Sprintf("prepareErr=%v submitErr=%v outcomeState=%d outcomeErr=%v pending=%v records=%d afterFlush=%d afterReopen=%d recovered=%d recoverErr=%v afterRecover=%d",
		l2E21Short(r.prepareErr), l2E21Short(r.submitErr), r.outcome.State, l2E21Short(r.outcome.Err), r.pending, r.records, r.afterFlush, r.afterReopen, r.recovered, l2E21Short(r.recoverErr), r.afterRecov)
}

func l2E21Short(err error) string {
	if err == nil {
		return "nil"
	}
	s := err.Error()
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return fmt.Sprintf("%q", s)
}

// defect: the commit was accepted (journaled or pending) but the store does
// not hold it, or the receipt ended committed-pending/unknown.
func (r l2E21Result) defect(want int) bool {
	if r.pending {
		return true
	}
	if r.records > 0 && r.afterFlush != want {
		return true
	}
	refused := r.prepareErr != nil || r.submitErr != nil || r.outcome.Err != nil
	if refused {
		return r.records != 0 || r.afterRecov != 0 || r.recoverErr != nil
	}
	return r.afterFlush != want || r.afterRecov != want
}

func l2E21Run(t *testing.T, c l2E21Case, mode EntityCommitMode) l2E21Result {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	var policy *memory.Policy
	if c.policy {
		policy = l2E21Policy(t)
	}
	engine := l2E21OpenEngine(t, dir, c.writeBehind, policy, c.writeBehind)
	wal := l2E21OpenWAL(t, dir)
	engine.SetBufferedCommitWAL(wal)
	var r l2E21Result

	tx, err := BeginEntityTransaction(ctx, engine, mode)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	for _, n := range c.nodes {
		if _, err := tx.CreateNode(ctx, copyNode(n)); err != nil {
			r.prepareErr = err
			break
		}
	}
	var prepared *PreparedEntityCommit
	if r.prepareErr == nil {
		prepared, r.prepareErr = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: c.logicalID})
	}
	if r.prepareErr != nil {
		_ = tx.Rollback()
	}
	if prepared != nil {
		receipt, err := NewWALEngine(engine, wal).SubmitEntityCommit(ctx, prepared, nil)
		r.submitErr = err
		// Abort only after the outcome is terminal. The accepted application
		// runs on its own goroutine and starts the parked commit through the
		// same once (PreparedEntityCommit.startCommit / Abort,
		// prepared_entity_commit_prototype.go:102-130): an Abort right after
		// submission races it, aborts the accepted commit and leaves it
		// "committed-pending or unknown" for recovery. The in-tree tests
		// defer it the same way (t.Cleanup(prepared.Abort)).
		if err == nil && receipt != nil {
			wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			r.outcome = receipt.WaitOutcome(wctx)
			cancel()
		}
		prepared.Abort() // no-op once the commit started; releases a frame the submission refused
	}
	var accepted *AcceptedCommitError
	r.pending = (r.outcome.Err != nil && r.outcome.State == CommitRecoveryPending) ||
		errors.As(r.outcome.Err, &accepted) || errors.As(r.submitErr, &accepted)
	r.records = l2E21Records(t, dir, c.logicalID)
	if err := engine.FlushWriteBehind(ctx); err != nil {
		t.Logf("flush: %v", err)
	}
	_, r.afterFlush = l2E21Count(t, engine, c)
	if err := engine.Close(); err != nil {
		t.Logf("engine close: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Logf("wal close: %v", err)
	}

	engine = l2E21OpenEngine(t, dir, c.writeBehind, policy, false)
	_, r.afterReopen = l2E21Count(t, engine, c)
	wal = l2E21OpenWAL(t, dir)
	engine.SetBufferedCommitWAL(wal)
	r.recovered, r.recoverErr = RecoverBufferedCommitsAtOpen(engine, wal)
	if err := engine.FlushWriteBehind(ctx); err != nil {
		t.Logf("flush after recovery: %v", err)
	}
	byLabel, byID := l2E21Count(t, engine, c)
	if byLabel != byID {
		t.Logf("after recovery: by label %d, by ID %d", byLabel, byID)
	}
	r.afterRecov = byID
	_ = engine.Close()
	_ = wal.Close()
	return r
}

func l2E21Modes() []struct {
	name string
	mode EntityCommitMode
} {
	return []struct {
		name string
		mode EntityCommitMode
	}{{"implicit", CommitModeImplicit}, {"explicit", CommitModeExplicit}}
}

func TestL2_e21_Repro(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	defects := 0
	var lines []string
	for _, c := range []l2E21Case{l2E21MemoryCase(300<<10, "r9-memory"), l2E21KeyCase(70000, "r9-key")} {
		for _, m := range l2E21Modes() {
			r := l2E21Run(t, c, m.mode)
			bad := r.defect(len(c.nodes))
			if bad {
				defects++
			}
			lines = append(lines, fmt.Sprintf("case=%s mode=%s defect=%v %s", c.name, m.name, bad, r))
		}
	}
	for _, l := range lines {
		t.Log(l)
	}
	if defects > 0 {
		t.Logf("L2RESULT E21 DEFECT %d of 4 runs accepted/journaled a commit the store does not hold", defects)
		t.Errorf("%d of 4 runs: commit accepted (journaled or pending) and not applied", defects)
		return
	}
	t.Logf("L2RESULT E21 OK 0 of 4 runs left an accepted unapplied commit")
}

// Control: the same path with commits within both limits (10 x 16 bytes under
// the 1 MiB policy; a one-letter label) is applied and survives the reopen.
func TestL2_e21_Control(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, c := range []l2E21Case{l2E21MemoryCase(16, "r9-memory-small"), l2E21KeyCase(1, "r9-key-short")} {
		for _, m := range l2E21Modes() {
			r := l2E21Run(t, c, m.mode)
			t.Logf("control case=%s mode=%s %s", c.name, m.name, r)
			want := len(c.nodes)
			if r.prepareErr != nil || r.submitErr != nil || r.outcome.Err != nil || r.afterFlush != want || r.afterRecov != want {
				t.Errorf("control case=%s mode=%s not applied: %s", c.name, m.name, r)
			}
		}
	}
}

// Cause: the entry names the order "journal first, checks after". At the pin
// both checks are meant to run before the journal: the key limit in
// storeKeyLimitErrorLocked (commit_admission.go:335, called from
// badger_transaction_publication.go:253) and the memory reservation in
// commitBufferedLocked before the prepared handoff (badger_write_behind.go:287).
// This logs, on the staged transactions, what each check decides before
// PrepareEntityCommit, and what the preparation then returns and reserves.
func TestL2_e21_Cause(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	ctx := context.Background()

	// Key limit: decide the staged transaction's keys directly.
	dir := t.TempDir()
	engine := l2E21OpenEngine(t, dir, false, nil, false)
	wal := l2E21OpenWAL(t, dir)
	engine.SetBufferedCommitWAL(wal)
	c := l2E21KeyCase(70000, "r9-key-cause")
	tx, err := BeginEntityTransaction(ctx, engine, CommitModeImplicit)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	_, createErr := tx.CreateNode(ctx, copyNode(c.nodes[0]))
	var keyErr error
	if createErr == nil {
		tx.mu.Lock()
		keyErr = tx.storeKeyLimitErrorLocked(ctx)
		tx.mu.Unlock()
	}
	prepared, prepErr := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: c.logicalID})
	if prepared != nil {
		prepared.Abort()
	} else {
		_ = tx.Rollback()
	}
	t.Logf("key case: create=%v storeKeyLimitErrorLocked=%v (isErrStoreKeyTooLong=%v) prepare=%v records=%d",
		createErr, keyErr, errors.Is(keyErr, ErrStoreKeyTooLong), prepErr, l2E21Records(t, dir, c.logicalID))
	_ = engine.Close()
	_ = wal.Close()

	// Memory policy: reservation at preparation.
	dir = t.TempDir()
	policy := l2E21Policy(t)
	engine = l2E21OpenEngine(t, dir, true, policy, true)
	wal = l2E21OpenWAL(t, dir)
	engine.SetBufferedCommitWAL(wal)
	mc := l2E21MemoryCase(300<<10, "r9-memory-cause")
	tx, err = BeginEntityTransaction(ctx, engine, CommitModeImplicit)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	for _, n := range mc.nodes {
		if _, err := tx.CreateNode(ctx, copyNode(n)); err != nil {
			t.Fatalf("stage: %v", err)
		}
	}
	prepared, prepErr = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: mc.logicalID})
	reserved := policy.Reserved()
	if prepared != nil {
		prepared.Abort()
	} else {
		_ = tx.Rollback()
	}
	t.Logf("memory case: prepare=%v isErrLimitExceeded=%v reservedAfterPrepare=%d records=%d",
		prepErr, errors.Is(prepErr, memory.ErrLimitExceeded), reserved, l2E21Records(t, dir, mc.logicalID))
	_ = engine.Close()
	_ = wal.Close()
}
