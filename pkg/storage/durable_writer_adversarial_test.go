package storage

import (
	"os"
	"strings"
	"testing"
)

// Regression coverage (): adversarial shapes against the single durable
// cache-owner guards. Each subtest fails when NO guard reports the shape.
func propertyWriteEntryGuardHits(files map[string]string) int {
	hits := 0
	for name, src := range files {
		hits += len(nodeCacheScopeOwnerViolations(name, []byte(src)))
		hits += len(writerScopeCallViolations(name, []byte(src)))
	}
	return hits
}

func propertyWriteEntryMutate(t *testing.T, file, old, replacement string) string {
	t.Helper()
	source, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ReplaceAll(string(source), "\r\n", "\n")
	if !strings.Contains(text, old) {
		t.Fatalf("mutation anchor missing in %s", file)
	}
	return strings.Replace(text, old, replacement, 1)
}

func TestPropertyWriteEntryGuardAdversarialShapes(t *testing.T) {
	const pkg = "package storage\n"
	cases := map[string]func(t *testing.T) map[string]string{
		"control-pr-copied-fence": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { b.nodeCacheInvalidationTs = ts }\n"}
		},
		"control-address-of-fence": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { p := &b.nodeCacheInvalidationTs; *p = ts }\n"}
		},
		"helper-split-fence-in-owner-file": func(*testing.T) map[string]string {
			return map[string]string{
				"badger_cache_scope.go": pkg + "func (b *BadgerEngine) fenceForWriter(ts uint64) { b.fenceUnknownNodeCacheWritesLocked(ts) }\n",
				"new_writer.go":         pkg + "func (b *BadgerEngine) writer(ts uint64) { b.nodeCacheMu.Lock(); b.fenceForWriter(ts); b.nodeCacheMu.Unlock() }\n",
			}
		},
		"helper-split-durable-terminal-named-commitBatch": func(*testing.T) map[string]string {
			return map[string]string{"badger_managed.go": pkg + "type rogueBatches struct{}\nfunc (rogueBatches) commitBatch(txn *badger.Txn) error { return txn.CommitAt(1, nil) }\n"}
		},
		"owner-call-after-publication-real-commitShared": func(t *testing.T) map[string]string {
			return map[string]string{"badger_managed.go": propertyWriteEntryMutate(t, "badger_managed.go",
				"\treturn ts, nil",
				"\tm.oracle.finish(ts)\n\t_ = m.oracle.waitPublished(ts)\n\treturn ts, nil")}
		},
		"never-called-owner-reference-real-issuer": func(t *testing.T) map[string]string {
			return map[string]string{"badger_cache_commit.go": propertyWriteEntryMutate(t, "badger_cache_commit.go",
				"generation := engine.cacheCommitNodeWrites(ts, scope)",
				"_ = engine.cacheCommitNodeWrites\n\tgeneration := uint64(0)")}
		},
		"method-value-issuer-bypass": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { issue := b.cacheCommitNodeWrites; _ = issue(ts, noNodeCacheWrites()) }\n"}
		},
		"aliased-fence-increment": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer() { b.nodeCacheInvalidationTs++ }\n"}
		},
		"aliased-commit-ts-store-method-value": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { store := b.nodeCacheCommitTs.Store; store(ts) }\n"}
		},
		"aliased-fence-method-value": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { fence := b.fenceUnknownNodeCacheWritesLocked; fence(ts) }\n"}
		},
		"zero-scope-through-struct": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "type carrier struct{ scope nodeCacheWriteScope }\nfunc (b *BadgerEngine) writer() error { var c carrier; return b.withUpdate(c.scope, func(txn *badger.Txn) error { return txn.Set(nodeKey(\"x\"), nil) }) }\n"}
		},
		"wrong-scope-through-struct": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "type carrier struct{ scope nodeCacheWriteScope }\nfunc (b *BadgerEngine) writer(id NodeID) error { c := carrier{scope: noNodeCacheWrites()}; return b.withUpdate(c.scope, func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }) }\n"}
		},
		"zero-scope-via-new": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer() error { return b.withUpdate(*new(nodeCacheWriteScope), func(txn *badger.Txn) error { return nil }) }\n"}
		},
		// r73 35f7 additions.
		"zero-scope-composite-literal": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(id NodeID) error { return b.withUpdate(nodeCacheWriteScope{}, func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }) }\n"}
		},
		"raw-write-batch-terminal": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(id NodeID) error { wb := b.db.DB.NewWriteBatchAt(1); _ = wb.Set(nodeKey(id), nil); return wb.Flush() }\n"}
		},
		"closure-fence-other-file": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(ts uint64) { f := func() { b.fenceUnknownNodeCacheWritesLocked(ts) }; f() }\n"}
		},
		"package-alias-fence": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "var fenceAlias = (*BadgerEngine).fenceUnknownNodeCacheWritesLocked\n"}
		},
		"package-alias-update": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "var updateAlias = (*managedBadgerDB).Update\n"}
		},
		"interface-update-call": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(id NodeID) error { var kv badgerKV = b.db; return kv.Update(func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }) }\n"}
		},
		"method-value-commitat": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func writer(txn *badger.Txn) error { commit := txn.CommitAt; return commit(1, nil) }\n"}
		},
		"metadata-route-writes-node-key": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(id NodeID) error { return b.withMetadataUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }) }\n"}
		},
		"metadata-helper-writes-node-key": func(*testing.T) map[string]string {
			return map[string]string{"new_writer.go": pkg + "func (b *BadgerEngine) writer(id NodeID) error { return updateBadgerMetadata(b.db, func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }) }\n"}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			if hits := propertyWriteEntryGuardHits(build(t)); hits == 0 {
				t.Errorf("ESCAPES: no guard reported shape %q", name)
			} else {
				t.Logf("CAUGHT: %q (%d reports)", name, hits)
			}
		})
	}
}
