// Package mutanttest holds the storage census mutant matrix and the runner
// that applies it.
//
// The matrix is one literal: each mutant rewrites or adds one module file and
// names the violation the census must report for it. run.go executes those
// cases one partition at a time, so the four test packages under
// testing/storagecensus/mutants share this only copy of them.

package mutanttest

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

const storageHeader = "package storage\n\nimport (\n\t\"context\"\n\n\tbadger \"github.com/dgraph-io/badger/v4\"\n)\n\nvar _ = badger.NewEntry\nvar _ context.Context\n\n"

const commitCountLockBinding = "\tcw.beforeLarge = tx.acquireCommitPublicationLocked\n"
const commitCountLockInline = "\tif err := tx.acquireCommitPublicationLocked(ctx); err != nil {\n\t\treturn tx.abortCommitLocked(err)\n\t}\n"

var mutants = []mutant{
	{
		name: "unreviewed prepared prefix field stays unclassified", file: "pkg/storage/schema_composite_index_entries.go",
		edits: [][2]string{
			{"\tprefixCount    int\n", "\tprefixCount    int\n\tcensusUnknownPrefixProgress int\n"},
			{"\tkeys.prefixCount++\n", "\tkeys.prefixCount++\n\tkeys.censusUnknownPrefixProgress++\n"},
		},
		want: []string{"unclassified field: pkg/storage.preparedCompositeNodeKeys.censusUnknownPrefixProgress"},
	},
	{
		name: "unreviewed snapshot boundary field stays unclassified", file: "pkg/storage/schema_property_index_snapshot.go",
		edits: [][2]string{
			{"\tgroupEnds []int\n", "\tgroupEnds []int\n\tcensusUnknownBoundaryEnds []int\n"},
			{"\t\t\tsnapshot.groupEnds = append(snapshot.groupEnds, len(snapshot.ids))\n", "\t\t\tsnapshot.groupEnds = append(snapshot.groupEnds, len(snapshot.ids))\n\t\t\tsnapshot.censusUnknownBoundaryEnds = append(snapshot.censusUnknownBoundaryEnds, len(snapshot.ids))\n"},
		},
		want: []string{"unclassified field: pkg/storage.propertyIndexSnapshot.censusUnknownBoundaryEnds"},
	},
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) }, []writtenEntities{}...)
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantEmptyEntities", want: []string{"unordered write:", "prefixNode"},
	},
	{
		name: "ordered callback alias cannot cover later unordered invocation", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantCallbackAlias() error {
	callback := func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) }
	alias := callback
	if err := b.withUpdate(context.Background(), updateRequest{}, alias, writtenEntities{nodes: []NodeID{"census-mutant"}}); err != nil { return err }
	return b.withUpdate(context.Background(), updateRequest{}, callback)
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(edgeTypeLabelCountKey(prefixMVCCMetaEdgeTypeStartLabelCount, "test", "Census", "LINK"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantWrongBoundReceiver", want: []string{"unordered write:", "prefixMVCCMetaEdgeTypeStartLabelCount"},
	},
	{
		// A closure is covered by the wrapper it is handed to only when the
		// wrapper holds the order where it runs the closure.
		name: "closure handed to a wrapper that takes no order", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) censusMutantRunner(fn func(txn *badger.Txn) error) error {
	return b.withUpdate(context.Background(), updateRequest{}, fn)
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(labelCountKey("census", "Mutant"), nil) })
}
`,
		want: []string{"unordered write: pkg/storage.BadgerEngine.CensusMutantCountUnderRLock", "prefixMVCCMetaLabelCount (derived-count)"},
	},
	{
		// The commit's positional counts are covered by the lock its
		// helper takes: without the helper's call they are not. The commit
		// itself lives in pkg/storage/badger_transaction_publication.go
		// (PublishCommit). Remove both its large-batch callback binding and
		// its inline completion call to inject the complete missing-owner fault.
		name:  "commit count lock helper not called",
		file:  "pkg/storage/badger_transaction_publication.go",
		edits: [][2]string{{commitCountLockBinding, ""}, {commitCountLockInline, ""}},
		entry: "pkg/storage.BadgerTransaction.Commit", want: []string{"unordered write:", "prefixMVCCMetaEdgeTypeStartLabelCount"},
	},
	{
		// Keeping the large-batch binding must not bless ordinary commits
		// whose inline acquisition was removed.
		name:  "ordinary commit count lock helper not called",
		file:  "pkg/storage/badger_transaction_publication.go",
		edits: [][2]string{{commitCountLockInline, ""}},
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(edgeTypeCountKey("census", "MUTANT"), nil) })
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
	_, err := b.GetStoredNode(context.Background(), id)
	return err == nil
}

func (b *BadgerEngine) CensusMutantHiddenReplace(n *Node) error {
	if b.censusMutantHidden(n.ID) {
		return b.UpdateNode(context.Background(), NodeUpdateRequest{After: n})
	}
	return nil
}
`,
		want: []string{"hidden decision outside the disposition owner: pkg/storage.BadgerEngine.CensusMutantHiddenReplace"},
	},
	{
		name: "acquisition after the write", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantLateOwner() error {
	err := b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
	var order orderHold
	if _, oerr := b.orderAfterBufferedCommits(context.Background(), &order, writtenEntities{}); oerr != nil {
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(edgeKey("census-mutant"), nil) })
}
`,
		entry: "pkg/storage.BadgerEngine.CensusMutantUnusedOwner", want: []string{"unordered write:", "prefixEdge"},
	},
	{
		name: "acquisition whose error is ignored", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) CensusMutantIgnoredOwner() error {
	l, _ := b.beginLifecycleWrite(context.Background(), "")
	defer l.finish()
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
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
		edits: [][2]string{{"func (b *BadgerEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {", "func (b *BadgerEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {\n\tb.nodeCount.Add(1)"}},
		want:  []string{"unowned entity-count change: pkg/storage.BadgerEngine.nodeCount", "in pkg/storage.BadgerEngine.CreateNode"},
	},
	{
		name: "acquisition removed from an existing writer", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"if err := b.orderCovering(write.ctx, &order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {", "if err := error(nil); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "hold released before the write", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\t\tdefer order.release()\n\t}\n\tvar publicationOps []Operation\n\tschema := b.schemaForNodeID(id)",
			"\t\torder.release()\n\t}\n\tvar publicationOps []Operation\n\tschema := b.schemaForNodeID(id)"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "branch around the acquisition", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\tvar order orderHold\n\tif b.writeBehind != nil {\n\t\tif err := b.orderCovering(write.ctx, &order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {",
			"\tvar order orderHold\n\tif b.writeBehind != nil && id != \"census-mutant\" {\n\t\tif err := b.orderCovering(write.ctx, &order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		// The buffer test through a predicate counts, but only its branch
		// without a buffer: an acquisition taken only there covers nothing.
		name: "acquisition only when there is no buffer", file: "pkg/storage/badger_nodes.go",
		edits: [][2]string{{"\tvar order orderHold\n\tif b.writeBehind != nil {\n\t\tif err := b.orderCovering(write.ctx, &order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {",
			"\tvar order orderHold\n\tif !b.writeBehindEnabled() {\n\t\tif err := b.orderCovering(write.ctx, &order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {"}},
		entry: "pkg/storage.BadgerEngine.DeleteNode", want: []string{"unordered write:", ""},
	},
	{
		name: "write extracted into a helper passed as a method value", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `func (b *BadgerEngine) censusMutantWrite(txn *badger.Txn) error {
	return txn.Set(labelIndexKey("Census", 1), nil)
}

func (b *BadgerEngine) CensusMutantHelper() error { return b.withUpdate(context.Background(), updateRequest{}, b.censusMutantWrite) }
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
		src:  "package multidb\n\nimport \"context\"\n\nfunc (m *DatabaseManager) CensusMutantQuota() int64 {\n\tn, _ := m.inner.NodeCount(context.Background())\n\treturn n\n}\n",
		want: []string{"unscoped total: pkg/multidb.DatabaseManager.CensusMutantQuota"},
	},
	{
		name: "accepted allowance map changed through alias", file: "pkg/multidb/zz_census_mutant.go",
		src:  "package multidb\nfunc(i *DatabaseInfo)CensusPendingAlias(){alias:=i.pending;alias[\"rogue\"]=reservedUsage{nodes:1}}\n",
		want: []string{"unowned quota change: pkg/multidb.DatabaseInfo.pending", "in pkg/multidb.DatabaseInfo.CensusPendingAlias"},
	},

	{
		name: "new quota field stays unclassified", file: "pkg/multidb/manager.go",
		edits: [][2]string{{"type DatabaseInfo struct {", "func(i *DatabaseInfo) CensusUnknownQuota(){i.censusUnknownQuota++}\ntype DatabaseInfo struct {\ncensusUnknownQuota int64"}},
		want:  []string{"unclassified field: pkg/multidb.DatabaseInfo.censusUnknownQuota"},
	},

	{
		name: "static visible read in quota accounting", file: "pkg/multidb/zz_census_mutant.go",
		src: `package multidb
import (
    "context"
    "github.com/orneryd/nornicdb/pkg/storage"
)
func (m *DatabaseManager) CensusMutantStaticVisible() error {
 engine := storage.NewNamespacedEngine(m.inner,m.config.DefaultDatabase)
 _,err := engine.GetNode(context.Background(), storage.NodeID("census-mutant"))
 return err
}
`,
		want: []string{"visible read in quota accounting: pkg/multidb.DatabaseManager.CensusMutantStaticVisible", "reads through GetNode"},
	},
	{
		name: "visible prior sized for quota", file: "pkg/multidb/storage_transaction.go",
		edits: [][2]string{{"if trackBytes {\n\t\tchanges = prepared.Changes()", "if trackBytes {\n\t\tchanges = prepared.Changes()\nfor i := range changes { if changes[i].BeforeNode != nil { changes[i].BeforeNode, _ = t.Engine.GetNode(context.Background(), changes[i].BeforeNode.ID) } }"}},
		want:  []string{"visible read in quota accounting: pkg/multidb.sizeTrackingEngine.admitPrepared"},
	},
	{
		name: "write through an interface with no acquisition", file: "pkg/storage/zz_census_mutant.go",
		src: storageHeader + `type censusMutantWriter interface{ write(b *BadgerEngine) error }

type censusMutantImpl struct{}

func (censusMutantImpl) write(b *BadgerEngine) error {
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(nodeKey("census-mutant"), nil) })
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
	return b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return censusMutantHooks.fn(txn) })
}
`,
		want: []string{"unresolved write callback: pkg/storage.BadgerEngine.CensusMutantUnresolved$1"},
	},
}
