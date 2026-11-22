//go:build closeout_l2_e17

package storage

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// Test-side seam for E17 Cause2 (pkg/storage/zz_closeout_l2_e17_test.go,
// package storage_test, which drives the Cypher executor). It wraps the
// engine the way MemoryEngine does (embedded *BadgerEngine + GetInnerEngine)
// and records, for every autocommit schema statement's begin, the Badger read
// timestamp of its snapshot and the commit oracle's state at that moment.

// L2E17SchemaBegin is one captured AutocommitSchemaBegin.
type L2E17SchemaBegin struct {
	tx           *BadgerTransaction
	BadgerReadTs uint64 // the snapshot every read of the statement's Badger txn uses
	Published    uint64 // oracle.published right after the begin
	LastAssigned uint64 // oracle.nextTs-1 right after the begin
	Inflight     int    // assigned but unfinished commit timestamps at the begin
}

// L2E17CaptureEngine is a BadgerEngine whose schema-statement begins are recorded.
type L2E17CaptureEngine struct {
	*BadgerEngine
	l2mu    sync.Mutex
	l2last  *L2E17SchemaBegin
	l2count int
}

// NewL2E17CaptureEngine wraps b.
func NewL2E17CaptureEngine(b *BadgerEngine) *L2E17CaptureEngine {
	return &L2E17CaptureEngine{BadgerEngine: b}
}

// GetInnerEngine exposes the actual storage owner to capability discovery, as
// MemoryEngine does.
func (c *L2E17CaptureEngine) GetInnerEngine() Engine { return c.BadgerEngine }

// L2E17OracleState returns oracle.published, oracle.nextTs-1 and the number of
// in-flight timestamps.
func (c *L2E17CaptureEngine) L2E17OracleState() (published, lastAssigned uint64, inflight int) {
	o := c.BadgerEngine.db.oracle
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.published.Load(), o.nextTs - 1, len(o.inflight)
}

// BeginTransaction begins as the engine does and records schema begins.
func (c *L2E17CaptureEngine) BeginTransaction(ctx context.Context, request BeginRequest) (*BadgerTransaction, error) {
	tx, err := c.BadgerEngine.BeginTransaction(ctx, request)
	if err != nil || request != AutocommitSchemaBegin {
		return tx, err
	}
	published, lastAssigned, inflight := c.L2E17OracleState()
	begin := &L2E17SchemaBegin{tx: tx, BadgerReadTs: tx.badgerReadTs, Published: published, LastAssigned: lastAssigned, Inflight: inflight}
	c.l2mu.Lock()
	c.l2last = begin
	c.l2count++
	c.l2mu.Unlock()
	return tx, nil
}

// L2E17TakeSchemaBegin returns and clears the last captured schema begin and
// the number captured since the previous call.
func (c *L2E17CaptureEngine) L2E17TakeSchemaBegin() (*L2E17SchemaBegin, int) {
	c.l2mu.Lock()
	defer c.l2mu.Unlock()
	begin, count := c.l2last, c.l2count
	c.l2last, c.l2count = nil, 0
	return begin, count
}

func l2E17KeyClass(key []byte) string {
	if len(key) == 0 {
		return "<empty>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "0x%02x:", key[0])
	for i, ch := range key[1:] {
		if i >= 6 {
			break
		}
		if ch >= 0x20 && ch < 0x7f {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "\\x%02x", ch)
		}
	}
	return b.String()
}

// L2E17Describe reports, for a captured schema begin, the decisive values of
// the two commit checks:
//   - the schema snapshot check (badger_transaction_publication.go:109-111):
//     the statement's readTS.CommitSequence vs the namespace's current one;
//   - Badger's conflict check at the commit of the statement's Badger txn
//     (abortCommitLocked -> commitConflictError, badger_transaction_staging.go:246-247,
//     badger_transaction.go:2234-2236): which keys were committed at a Badger
//     version above the statement's read timestamp. Versions up to the
//     oracle's last assigned timestamp at the begin were assigned BEFORE the
//     statement took its exclusive barrier but were not in its snapshot
//     ("lag"); versions after it and up to endAssigned were assigned WHILE the
//     statement ran ("window"), which the exclusive barrier should exclude
//     for barriered writes (the statement's own pre-commit writes are in it).
func (c *L2E17CaptureEngine) L2E17Describe(begin *L2E17SchemaBegin, namespace string, endAssigned uint64) string {
	if begin == nil {
		return "no schema begin captured"
	}
	begin.tx.mu.Lock()
	txSeq := begin.tx.readTS.CommitSequence
	begin.tx.mu.Unlock()
	currentSeq := c.BadgerEngine.currentMVCCReadVersion(namespace).CommitSequence
	groups := map[string]int{}
	scanErr := func() error {
		raw := c.BadgerEngine.db.DB
		txn := raw.NewTransactionAt(math.MaxUint64, false)
		defer txn.Discard()
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			version := item.Version()
			if version <= begin.BadgerReadTs || version > endAssigned {
				continue
			}
			class := "window"
			if version <= begin.LastAssigned {
				class = "lag"
			}
			groups[fmt.Sprintf("%s %s", class, l2E17KeyClass(item.Key()))]++
		}
		return nil
	}()
	lines := make([]string, 0, len(groups))
	for k, n := range groups {
		lines = append(lines, fmt.Sprintf("%s x%d", k, n))
	}
	sort.Strings(lines)
	if len(lines) > 40 {
		lines = append(lines[:40], fmt.Sprintf("... %d more", len(lines)-40))
	}
	return fmt.Sprintf("badgerReadTs=%d published_at_begin=%d last_assigned_at_begin=%d lag_at_begin=%d inflight_at_begin=%d last_assigned_at_end=%d tx.readTS.CommitSequence=%d namespace_current_sequence=%d scan_err=%v versions_above_snapshot=[%s]",
		begin.BadgerReadTs, begin.Published, begin.LastAssigned, begin.LastAssigned-begin.Published, begin.Inflight, endAssigned, txSeq, currentSeq, scanErr, strings.Join(lines, "; "))
}
