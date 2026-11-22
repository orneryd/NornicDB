//go:build closeout_l2_e17

package storage

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
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

// ---------------------------------------------------------------------------
// E17 Cause4 seam: wraps the base engine nornicdb.Open builds (the WAL engine
// over the Badger engine) as the database manager's inner engine. It is a
// transparent EngineUnwrapper, so capability discovery still reaches the WAL
// and Badger engines, and it is the first TransactionCapableEngine the Cypher
// executor finds (cypher resolveImplicitTxEngines), so every schema
// statement's begin passes here and is recorded with its Badger transaction.

// L2E17C4Begin is one captured AutocommitSchemaBegin.
type L2E17C4Begin struct {
	Tx           *BadgerTransaction
	Txn          *badger.Txn
	ReadTs       uint64 // Txn.ReadTs()
	Published    uint64 // oracle.published right after the begin
	LastAssigned uint64 // oracle.nextTs-1 right after the begin
	Inflight     int
	MaxVersion   uint64 // badger.DB.MaxVersion() right after the begin
}

// L2E17Cause4Engine is the recording wrapper.
type L2E17Cause4Engine struct {
	Engine
	native *BadgerEngine
	l2mu   sync.Mutex
	l2last *L2E17C4Begin
}

// NewL2E17Cause4Engine wraps base and finds its Badger engine.
func NewL2E17Cause4Engine(base Engine) (*L2E17Cause4Engine, error) {
	engine := base
	for depth := 0; depth < 16 && engine != nil; depth++ {
		switch e := engine.(type) {
		case *BadgerEngine:
			return &L2E17Cause4Engine{Engine: base, native: e}, nil
		case *MemoryEngine:
			return &L2E17Cause4Engine{Engine: base, native: e.BadgerEngine}, nil
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, fmt.Errorf("no Badger engine under %T (stopped at %T)", base, engine)
		}
		engine = unwrapper.GetInnerEngine()
	}
	return nil, fmt.Errorf("no Badger engine under %T", base)
}

// GetInnerEngine keeps capability discovery transparent.
func (c *L2E17Cause4Engine) GetInnerEngine() Engine { return c.Engine }

// BeginTransaction is the native begin, recording schema begins.
func (c *L2E17Cause4Engine) BeginTransaction(ctx context.Context, request BeginRequest) (*BadgerTransaction, error) {
	tx, err := c.native.BeginTransaction(ctx, request)
	if err != nil || request != AutocommitSchemaBegin {
		return tx, err
	}
	o := c.native.db.oracle
	o.mu.Lock()
	published, lastAssigned, inflight := o.published.Load(), o.nextTs-1, len(o.inflight)
	o.mu.Unlock()
	begin := &L2E17C4Begin{Tx: tx, Txn: tx.badgerTx, Published: published, LastAssigned: lastAssigned, Inflight: inflight, MaxVersion: c.native.db.DB.MaxVersion()}
	if tx.badgerTx != nil {
		begin.ReadTs = tx.badgerTx.ReadTs()
	}
	c.l2mu.Lock()
	c.l2last = begin
	c.l2mu.Unlock()
	return tx, nil
}

// L2E17TakeBegin returns and clears the last captured schema begin.
func (c *L2E17Cause4Engine) L2E17TakeBegin() *L2E17C4Begin {
	c.l2mu.Lock()
	defer c.l2mu.Unlock()
	begin := c.l2last
	c.l2last = nil
	return begin
}

// l2E17TxnReads reads the Badger transaction's read-key fingerprints
// (badger.Txn.reads, unexported; z.MemHash of each key read, the set Badger's
// conflict check compares with later commits' written keys).
func l2E17TxnReads(txn *badger.Txn) (map[uint64]struct{}, bool) {
	if txn == nil {
		return nil, false
	}
	field := reflect.ValueOf(txn).Elem().FieldByName("reads")
	if !field.IsValid() || field.Kind() != reflect.Slice {
		return nil, false
	}
	reads := make(map[uint64]struct{}, field.Len())
	for i := 0; i < field.Len(); i++ {
		element := field.Index(i)
		if element.Kind() != reflect.Uint64 {
			return nil, false
		}
		reads[element.Uint()] = struct{}{}
	}
	return reads, true
}

// L2E17Cause4Describe reports a refused schema statement's decisive values:
// its Badger read timestamp and the oracle / Badger state right after its
// begin, and every key with a committed version above its read timestamp,
// marked when the statement read that key (fingerprint in its read set: the
// key Badger's conflict check fired on) and split by whether the version was
// already committed when the begin returned (<= MaxVersion at begin) or
// committed later.
func (c *L2E17Cause4Engine) L2E17Cause4Describe(begin *L2E17C4Begin) string {
	if begin == nil {
		return "no schema begin captured"
	}
	o := c.native.db.oracle
	o.mu.Lock()
	publishedNow, lastAssignedNow := o.published.Load(), o.nextTs-1
	o.mu.Unlock()
	raw := c.native.db.DB
	maxNow := raw.MaxVersion()
	reads, readsOK := l2E17TxnReads(begin.Txn)
	var conflicted, other []string
	txn := raw.NewTransactionAt(math.MaxUint64, false)
	opts := badger.DefaultIteratorOptions
	opts.AllVersions = true
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		v := item.Version()
		if v <= begin.ReadTs {
			continue
		}
		when := "after-begin"
		if v <= begin.MaxVersion {
			when = "before-begin"
		}
		line := fmt.Sprintf("v%d %s %s", v, when, l2E17KeyClass(item.Key()))
		if _, read := reads[z.MemHash(item.Key())]; read {
			conflicted = append(conflicted, line)
		} else if len(other) < 30 {
			other = append(other, line)
		}
	}
	it.Close()
	txn.Discard()
	sort.Strings(conflicted)
	// The begin takes the exclusive barrier, then its snapshot (readTs =
	// oracle.published), then returns; the values above are read on return.
	verdict := "readTs equals the newest committed version at begin"
	switch {
	case begin.Published < begin.LastAssigned:
		verdict = "published-vs-assigned lag at begin: an assigned commit was still unfinished, so the snapshot stopped below it"
	case begin.ReadTs < begin.Published:
		verdict = "oracle.published advanced between the snapshot and the begin's return: a commit finished inside the exclusive barrier"
	case begin.ReadTs < begin.MaxVersion:
		verdict = "Badger holds versions above oracle.published with nothing in flight: written outside the oracle's assign/finish"
	}
	return fmt.Sprintf("readTs=%d published_at_begin=%d last_assigned_at_begin=%d inflight_at_begin=%d maxVersion_at_begin=%d published_now=%d last_assigned_now=%d maxVersion_now=%d read_set_size=%d read_set_ok=%t begin_reading=%q conflicted_keys(read by the DDL, newer than its readTs)=[%s] other_newer_versions(first 30)=[%s]",
		begin.ReadTs, begin.Published, begin.LastAssigned, begin.Inflight, begin.MaxVersion, publishedNow, lastAssignedNow, maxNow, len(reads), readsOK, verdict, strings.Join(conflicted, "; "), strings.Join(other, "; "))
}
