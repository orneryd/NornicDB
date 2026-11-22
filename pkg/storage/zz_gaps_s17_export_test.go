//go:build report_gaps

package storage

// Report gap S17 (v1.5.2 verification campaign, test-side seam only): which
// caller writes the namespace schema record (0x09:<namespace>) that a
// refused autocommit CREATE INDEX / DROP INDEX read.
//
// GapS17Engine wraps the base engine nornicdb.Open builds (transparent
// EngineUnwrapper, first TransactionCapableEngine the Cypher executor finds)
// and records every AutocommitSchemaBegin with its Badger transaction.
// InstallPersisterRecorder replaces the namespace schema manager's persister
// (SchemaManager.SetPersister, the hook bindNamespaceSchema sets) with a
// wrapper that calls the same persistSchemaDefinition and records, per call,
// the Badger version it wrote to the schema record, the calling goroutine
// and its stack. Describe lists every version of the schema record above a
// refused statement's read timestamp with the recorded writer of each.

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

type GapS17Begin struct {
	Txn    *badger.Txn
	ReadTs uint64
	GID    uint64
}

type GapS17Write struct {
	Before, After uint64
	GID           uint64
	Stack         string
	Err           error
}

type GapS17Engine struct {
	Engine
	native *BadgerEngine
	mu     sync.Mutex
	last   *GapS17Begin
	writes []GapS17Write
}

func NewGapS17Engine(base Engine) (*GapS17Engine, error) {
	engine := base
	for depth := 0; depth < 16 && engine != nil; depth++ {
		switch e := engine.(type) {
		case *BadgerEngine:
			return &GapS17Engine{Engine: base, native: e}, nil
		case *MemoryEngine:
			return &GapS17Engine{Engine: base, native: e.BadgerEngine}, nil
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, fmt.Errorf("no Badger engine under %T (stopped at %T)", base, engine)
		}
		engine = unwrapper.GetInnerEngine()
	}
	return nil, fmt.Errorf("no Badger engine under %T", base)
}

func (c *GapS17Engine) GetInnerEngine() Engine { return c.Engine }

func GapS17GoroutineID() uint64 {
	buf := make([]byte, 64)
	s := string(buf[:runtime.Stack(buf, false)])
	s = strings.TrimPrefix(s, "goroutine ")
	if i := strings.IndexByte(s, ' '); i > 0 {
		id, _ := strconv.ParseUint(s[:i], 10, 64)
		return id
	}
	return 0
}

func (c *GapS17Engine) BeginTransaction(ctx context.Context, request BeginRequest) (*BadgerTransaction, error) {
	tx, err := c.native.BeginTransaction(ctx, request)
	if err != nil || request != AutocommitSchemaBegin {
		return tx, err
	}
	begin := &GapS17Begin{Txn: tx.badgerTx, GID: GapS17GoroutineID()}
	if tx.badgerTx != nil {
		begin.ReadTs = tx.badgerTx.ReadTs()
	}
	c.mu.Lock()
	c.last = begin
	c.mu.Unlock()
	return tx, nil
}

func (c *GapS17Engine) TakeBegin() *GapS17Begin {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.last
	c.last = nil
	return b
}

// schemaRecordVersion returns the newest committed Badger version of the
// namespace schema record (0 when absent).
func (c *GapS17Engine) schemaRecordVersion(namespace string) uint64 {
	var v uint64
	txn := c.native.db.DB.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	if item, err := txn.Get(schemaKey(namespace)); err == nil {
		v = item.Version()
	}
	return v
}

// l2GapS17Reads reads the Badger transaction's read-key fingerprints
// (badger.Txn.reads, unexported; z.MemHash of each key read).
func l2GapS17Reads(txn *badger.Txn) (map[uint64]struct{}, bool) {
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

func gapS17Stack() string {
	buf := make([]byte, 1<<16)
	s := string(buf[:runtime.Stack(buf, false)])
	lines := strings.Split(s, "\n")
	var out []string
	for i := 1; i+1 < len(lines); i += 2 {
		fn := strings.TrimSpace(lines[i])
		if j := strings.Index(fn, "("); j > 0 {
			fn = fn[:j]
		}
		if k := strings.LastIndex(fn, "/"); k >= 0 {
			fn = fn[k+1:]
		}
		loc := strings.TrimSpace(lines[i+1])
		if j := strings.Index(loc, " +0x"); j > 0 {
			loc = loc[:j]
		}
		if j := strings.Index(loc, "/pkg/"); j > 0 {
			loc = loc[j+1:]
		}
		if strings.HasPrefix(fn, "runtime.") || strings.Contains(fn, "gapS17Stack") {
			continue
		}
		out = append(out, fn+" "+loc)
		if len(out) >= 22 {
			break
		}
	}
	return strings.Join(out, " <- ")
}

// InstallPersisterRecorder wraps the schema manager persister of namespace.
func (c *GapS17Engine) InstallPersisterRecorder(namespace string) {
	sm := c.native.GetSchemaForNamespace(namespace)
	sm.SetPersister(func(def *SchemaDefinition) error {
		before := c.schemaRecordVersion(namespace)
		err := c.native.persistSchemaDefinition(namespace, def)
		after := c.schemaRecordVersion(namespace)
		w := GapS17Write{Before: before, After: after, GID: GapS17GoroutineID(), Stack: gapS17Stack(), Err: err}
		c.mu.Lock()
		c.writes = append(c.writes, w)
		c.mu.Unlock()
		return err
	})
}

func (c *GapS17Engine) PersisterWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

// Describe reports every version of the namespace schema record above the
// refused statement's read timestamp, whether the statement read that key,
// and the recorded persister call that wrote that version (goroutine, same
// goroutine as the statement or not, stack).
func (c *GapS17Engine) Describe(begin *GapS17Begin, namespace string, ddlGID uint64) string {
	if begin == nil {
		return "no schema begin captured"
	}
	key := schemaKey(namespace)
	readSet := map[uint64]struct{}{}
	if begin.Txn != nil {
		if reads, ok := l2GapS17Reads(begin.Txn); ok {
			readSet = reads
		}
	}
	_, keyRead := readSet[z.MemHash(key)]
	var versions []uint64
	txn := c.native.db.DB.NewTransactionAt(math.MaxUint64, false)
	opts := badger.DefaultIteratorOptions
	opts.AllVersions = true
	opts.PrefetchValues = false
	opts.Prefix = key
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if !bytes.Equal(item.Key(), key) {
			continue
		}
		if item.Version() > begin.ReadTs {
			versions = append(versions, item.Version())
		}
	}
	it.Close()
	txn.Discard()
	c.mu.Lock()
	writes := append([]GapS17Write(nil), c.writes...)
	c.mu.Unlock()
	var parts []string
	for _, v := range versions {
		found := false
		for _, w := range writes {
			if w.After == v {
				found = true
				parts = append(parts, fmt.Sprintf("v%d writer=persistSchemaDefinition goroutine=%d same_goroutine_as_refused_statement=%t prev_version=%d err=%v stack=[%s]", v, w.GID, w.GID == ddlGID, w.Before, w.Err, w.Stack))
				break
			}
		}
		if !found {
			parts = append(parts, fmt.Sprintf("v%d writer=NOT persistSchemaDefinition (no recorded persister call wrote this version)", v))
		}
	}
	return fmt.Sprintf("readTs=%d statement_goroutine=%d schema_key=%q read_by_statement=%t read_set_size=%d schema_record_versions_above_readTs=%d [%s]",
		begin.ReadTs, ddlGID, string(key), keyRead, len(readSet), len(versions), strings.Join(parts, " || "))
}

// RecentWrites summarizes the last n persister calls (for accepted baselines).
func (c *GapS17Engine) RecentWrites(n int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	start := len(c.writes) - n
	if start < 0 {
		start = 0
	}
	var parts []string
	for _, w := range c.writes[start:] {
		parts = append(parts, fmt.Sprintf("v%d->v%d goroutine=%d stack=[%s]", w.Before, w.After, w.GID, w.Stack))
	}
	return strings.Join(parts, " || ")
}
