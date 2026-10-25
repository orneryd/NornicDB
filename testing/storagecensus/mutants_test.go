package storagecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mutant is a change to the module, applied as an overlay: a new file, or a
// rewrite of an existing one. want is the violation it must produce: the
// census must reject it for the intended reason, not merely change.
type mutant struct {
	name string
	// file is a module-relative path; src replaces it (a new file), or
	// edits rewrite it (old -> new, each old occurring exactly once).
	file  string
	src   string
	edits [][2]string
	// want are substrings one violation must contain, all of them; entry,
	// when set, is an entry point that violation must list.
	want  []string
	entry string
}

const storageHeader = "package storage\n\nimport badger \"github.com/dgraph-io/badger/v4\"\n\nvar _ = badger.NewEntry\n\n"

var mutants = []mutant{
	{
		name: "selected count mutex uses wrong selector", file: "pkg/storage/badger_label_count.go",
		edits: [][2]string{{"mu := b.namedCountMutex(kind)\n\tmu.Lock()", "mu := b.namedCountMutex(countDeltaKindLabel)\n\tmu.Lock()"}},
		want:  []string{"unordered write:", "prefixMVCCMetaEdgeTypeCount"},
	},
	{
		name: "selected count mutex swaps family mapping", file: "pkg/storage/badger_label_count.go",
		edits: [][2]string{{"return &b.edgeTypeCountWriteMu\n\t}\n\treturn &b.labelCountWriteMu", "return &b.labelCountWriteMu\n\t}\n\treturn &b.edgeTypeCountWriteMu"}},
		want:  []string{"unordered write:", "prefixMVCCMetaEdgeTypeCount"},
	},
	{
		name: "phi count key selects wrong family", file: "pkg/storage/badger_label_count.go",
		edits: [][2]string{{"baseKey := labelCountKey\n\tif kind == countDeltaKindEdgeType {\n\t\tbaseKey = edgeTypeCountKey", "baseKey := edgeTypeCountKey\n\tif kind == countDeltaKindEdgeType {\n\t\tbaseKey = labelCountKey"}},
		want:  []string{"unordered write:", "prefixMVCCMetaEdgeTypeCount"},
	},
	{
		name: "explicit empty entities do not acquire order", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantEmptyEntities() error {
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) }, []writtenEntities{}...)
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantEmptyEntities", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "ordered callback alias cannot cover later unordered invocation", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantCallbackAlias() error {
	callback := func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) }
	alias := callback
	if err := b.withUpdate(alias, writtenEntities{nodes: []NodeID{"census-mutant"}}); err != nil { return err }
	return b.withUpdate(callback)
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantCallbackAlias", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "same-spelled bound lock helper on another receiver grants nothing", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `type censusMutantLockReceiver struct{}
func (*censusMutantLockReceiver) acquireCommitPublicationLocked() {}
func (b *BadgerEngine) CensusMutantWrongBoundReceiver() error {
	take := (&censusMutantLockReceiver{}).acquireCommitPublicationLocked
	take()
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(edgeTypeLabelCountKey(prefixMVCCMetaEdgeTypeStartLabelCount, "test", "Census", "LINK"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantWrongBoundReceiver", want: []string{"unordered write:", "prefixMVCCMetaEdgeTypeStartLabelCount"},
	},
	{
		// A closure is covered by the wrapper it is handed to only when the
		// wrapper holds the order where it runs the closure.
		name: "closure handed to a wrapper that takes no order", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) censusMutantRunner(fn func(txn *badger.Txn) error) error {
	return b.withUpdate(fn)
}

func (b *BadgerEngine) CensusMutantWrapped() error {
	return b.censusMutantRunner(func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantWrapped", want: []string{"unordered write:", "prefixNode"},
	},
	{
		// A closure handed to a function value runs when the function the
		// value can be runs it: here one that keeps it for later.
		name: "closure handed to a runner that keeps it", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `var censusMutantQueue []func()

func censusMutantKeep(fn func()) { censusMutantQueue = append(censusMutantQueue, fn) }

func (b *BadgerEngine) censusMutantRunWith(run func(func()), id NodeID) {
	key := nodeKey(id)
	run(func() { _ = b.db.Update(func(txn *badger.Txn) error { return txn.Delete(key) }) })
}

func (b *BadgerEngine) CensusMutantRunnerKeeps(id NodeID) { b.censusMutantRunWith(censusMutantKeep, id) }
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantRunnerKeeps", want: []string{"unordered write:", "prefixNode"},
	},
	{
		// A count lock covers only when held exclusively.
		name: "count rewritten under the read lock", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantCountUnderRLock() error {
	b.labelCountWriteMu.RLock()
	defer b.labelCountWriteMu.RUnlock()
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(labelCountKey("census", "Mutant"), nil) })
}
`,
		want: []string{"unordered write: pkg/storage.BadgerEngine.CensusMutantCountUnderRLock", "prefixMVCCMetaLabelCount (derived-count)"},
	},
	{
		// The commit's positional counts are covered by the lock its
		// helper takes: without the helper's call they are not. The commit
		// itself lives in pkg/storage/badger_transaction_publication.go
		// (CommitContextWithPublication); the edit is anchored where the
		// helper runs, just before the commit's own Badger commit.
		name:  "commit count lock helper not called",
		file:  "pkg/storage/badger_transaction_publication.go",
		edits: [][2]string{{"\ttx.acquireCommitPublicationLocked()\n\n\t// Commit Badger transaction (atomic!)", "\n\t// Commit Badger transaction (atomic!)"}},
		entry: "pkg/storage.BadgerTransaction.Commit", want: []string{"unordered write:", "prefixMVCCMetaEdgeTypeStartLabelCount"},
	},
	{
		// A migration is ordered inside the constructor, before the buffer
		// exists; the same migration called on an open engine is not.
		name: "migration run on an open engine", file: "pkg/storage/zz_census_mutant.go",
		src:   storageHeader + "func (b *BadgerEngine) CensusMutantMigrate() error { return b.migrateV2ToV3() }\n",
		entry: "pkg/storage.BadgerEngine.CensusMutantMigrate", want: []string{"unordered write:", "Adj"},
	},
	{
		// A count lock covers its own counts only.
		name: "count rewritten under another count's lock", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantWrongCountLock() error {
	b.labelCountWriteMu.Lock()
	defer b.labelCountWriteMu.Unlock()
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(edgeTypeCountKey("census", "MUTANT"), nil) })
}
`,
		want: []string{"unordered write: pkg/storage.BadgerEngine.CensusMutantWrongCountLock", "prefixMVCCMetaEdgeTypeCount (derived-count)"},
	},
	{
		name: "captured key written after its maker returned", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantLater(id NodeID) {
	key := nodeKey(id)
	go func() { _ = b.db.Update(func(txn *badger.Txn) error { return txn.Delete(key) }) }()
}
`,
		want: []string{"unordered write: pkg/storage.BadgerEngine.CensusMutantLater", "prefixNode (entity-body)"},
	},
	{
		name: "raw total read", file: "pkg/storage/zz_census_mutant.go",
		src:  storageHeader + "func (b *BadgerEngine) CensusMutantRawTotal() int64 { return b.nodeCount.Load() }\n",
		want: []string{"unowned entity-count read: pkg/storage.BadgerEngine.nodeCount", "in pkg/storage.BadgerEngine.CensusMutantRawTotal"},
	},
	{
		name: "hidden decision split into a helper", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) censusMutantHidden(id NodeID) bool {
	_, err := b.GetStoredNode(id)
	return err == nil
}

func (b *BadgerEngine) CensusMutantHiddenReplace(n *Node) error {
	if b.censusMutantHidden(n.ID) {
		return b.UpdateNode(n)
	}
	return nil
}
`,
		want: []string{"hidden decision outside the disposition owner: pkg/storage.BadgerEngine.CensusMutantHiddenReplace"},
	},
	{
		name: "acquisition after the write", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantLateOwner() error {
	err := b.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
	var order orderHold
	if _, oerr := b.orderAfterBufferedCommits(&order, writtenEntities{}); oerr != nil {
		return oerr
	}
	defer order.release()
	return err
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantLateOwner", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "acquisition referenced, never called", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantUnusedOwner() error {
	_ = b.beginLifecycleWrite
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(edgeKey("census-mutant"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantUnusedOwner", want: []string{"unordered write:", "prefixEdge"},
	},
	{
		name: "acquisition whose error is ignored", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantIgnoredOwner() error {
	l, _ := b.beginLifecycleWrite("")
	defer l.finish()
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantIgnoredOwner", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "cardinality field changed outside its owner", file: "pkg/storage/zz_census_mutant.go",
		src:  storageHeader + "func (d *bufferedCardinalityDeltas) CensusMutantUnownedCount() { d.nodeTotal += 1 }\n",
		want: []string{"unowned entity-count change: pkg/storage.bufferedCardinalityDeltas.nodeTotal", "in pkg/storage.bufferedCardinalityDeltas.CensusMutantUnownedCount"},
	},
	{
		name: "extra count inside an existing writer", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"func (b *BadgerEngine) CreateNode(node *Node) (NodeID, error) {", "func (b *BadgerEngine) CreateNode(node *Node) (NodeID, error) {\n\tb.nodeCount.Add(1)"}},
		want:  []string{"unowned entity-count change: pkg/storage.BadgerEngine.nodeCount", "in pkg/storage.BadgerEngine.CreateNode"},
	},
	{
		name: "acquisition removed from an existing writer", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"if err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {", "if err := error(nil); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "hold released before the write", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\t\tdefer order.release()\n\t}\n\terr := b.withUpdate(func(txn *badger.Txn) error {\n\t\tversion, allocErr := b.allocateMVCCVersion(txn, namespaceForNodeID(id), time.Now())",
			"\t\torder.release()\n\t}\n\terr := b.withUpdate(func(txn *badger.Txn) error {\n\t\tversion, allocErr := b.allocateMVCCVersion(txn, namespaceForNodeID(id), time.Now())"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "branch around the acquisition", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\tvar order orderHold\n\tif b.writeBehind != nil {\n\t\tif err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {",
			"\tvar order orderHold\n\tif b.writeBehind != nil && id != \"census-mutant\" {\n\t\tif err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		// The buffer test through a predicate counts, but only its branch
		// without a buffer: an acquisition taken only there covers nothing.
		name: "acquisition only when there is no buffer", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\tvar order orderHold\n\tif b.writeBehind != nil {\n\t\tif err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {",
			"\tvar order orderHold\n\tif !b.writeBehindEnabled() {\n\t\tif err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "write extracted into a helper passed as a method value", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) censusMutantWrite(txn *badger.Txn) error {
	return txn.Set(labelIndexKey("Census", 1), nil)
}

func (b *BadgerEngine) CensusMutantHelper() error { return b.withUpdate(b.censusMutantWrite) }
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantHelper", want: []string{"unordered write:", "prefixLabelIndex"},
	},
	{
		name: "counter changed through an alias", file: "pkg/storage/zz_census_mutant.go",
		src:  storageHeader + "func (d *bufferedCardinalityDeltas) CensusMutantAlias() {\n\tp := &d.nodeTotal\n\t*p++\n}\n",
		want: []string{"unowned entity-count change: pkg/storage.bufferedCardinalityDeltas.nodeTotal", "in pkg/storage.bufferedCardinalityDeltas.CensusMutantAlias"},
	},
	{
		name: "count map changed through an alias", file: "pkg/storage/zz_census_mutant.go",
		src:  storageHeader + "func (b *BadgerEngine) CensusMutantMapAlias() {\n\tm := b.namespaceNodeCounts\n\tm[\"census\"]++\n}\n",
		want: []string{"unowned entity-count change: pkg/storage.BadgerEngine.namespaceNodeCounts", "in pkg/storage.BadgerEngine.CensusMutantMapAlias"},
	},
	{
		name: "unscoped total read for a quota", file: "pkg/multidb/zz_census_mutant.go",
		src:  "package multidb\n\nfunc (m *DatabaseManager) CensusMutantQuota() int64 {\n\tn, _ := m.inner.NodeCount()\n\treturn n\n}\n",
		want: []string{"unscoped total: pkg/multidb.DatabaseManager.CensusMutantQuota"},
	},
	{
		name: "visible prior sized for quota", file: "pkg/multidb/storage_transaction.go",
		edits: [][2]string{{"return storage.GetStoredNode(t.Engine, storage.NodeID(strings.TrimPrefix(string(id), t.dbName+\":\")))", "return t.Engine.GetNode(storage.NodeID(strings.TrimPrefix(string(id), t.dbName+\":\")))"}},
		want:  []string{"visible read in quota accounting: pkg/multidb.sizeTrackingEngine.CommitTransaction"},
	},
	{
		name: "write through an interface with no acquisition", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `type censusMutantWriter interface{ write(b *BadgerEngine) error }

type censusMutantImpl struct{}

func (censusMutantImpl) write(b *BadgerEngine) error {
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
}

func (b *BadgerEngine) CensusMutantInterface() error {
	var w censusMutantWriter = censusMutantImpl{}
	return w.write(b)
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantInterface", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "unresolved write callback", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `type censusMutantHook struct{ fn func(*badger.Txn) error }

var censusMutantHooks censusMutantHook

func (b *BadgerEngine) CensusMutantUnresolved() error {
	return b.withUpdate(func(txn *badger.Txn) error { return censusMutantHooks.fn(txn) })
}
`,
		want: []string{"unresolved write callback: pkg/storage.BadgerEngine.CensusMutantUnresolved$1"},
	},
}

// TestCensusRejectsMutants applies each mutant to the module as an overlay
// and requires the census to report the violation it introduces, for the
// intended reason. It runs the full census once per mutant.
func TestCensusRejectsMutants(t *testing.T) {
	if testing.Short() {
		t.Skip("one full census per mutant")
	}
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			path := filepath.Join(abs, filepath.FromSlash(m.file))
			src := []byte(m.src)
			if m.src == "" {
				orig, err := os.ReadFile(path)
				require.NoError(t, err)
				text := strings.ReplaceAll(string(orig), "\r\n", "\n")
				for _, e := range m.edits {
					require.Equal(t, 1, strings.Count(text, e[0]), "mutation anchor in %s", m.file)
					text = strings.Replace(text, e[0], e[1], 1)
				}
				src = []byte(text)
			}
			r, err := Census(root, map[string][]byte{path: src})
			require.NoError(t, err)
			// The violation must be the mutant's: absent from the census of
			// the unchanged module.
			base := census(t)
			inBase := map[string]bool{}
			for _, v := range base.Violations {
				inBase[v] = true
			}
			baseRow := func(u UnorderedRow) bool {
				for _, b := range base.Unordered {
					if b.Origin == u.Origin && b.Space == u.Space && contains(b.Entries, m.entry) {
						return true
					}
				}
				return false
			}
			found := false
			if m.entry != "" {
				// An unordered write the entry point reaches, of the space wanted.
				for _, u := range r.Unordered {
					found = found || (contains(u.Entries, m.entry) && strings.Contains(u.Space, m.want[len(m.want)-1]) && !baseRow(u))
				}
			} else {
				for _, v := range r.Violations {
					all := !inBase[v]
					for _, w := range m.want {
						all = all && strings.Contains(v, w)
					}
					found = found || all
				}
			}
			require.True(t, found, "the census did not reject the mutant %q for its reason %q; violations:\n  %s", m.name, m.want, strings.Join(r.Violations, "\n  "))
		})
	}
}
