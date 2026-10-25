package storagecensus

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/collection"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// statePackages hold the module's stored-entity state in memory.
var statePackages = []string{"pkg/storage", "pkg/multidb"}

// fieldRow classifies one field. owners are the only functions that may
// change a field whose class is in ownedFieldClasses.
type fieldRow struct {
	class  string
	owners []string
	// readers may also read an owned field: it is a total across every
	// database (or unscoped), so a reader outside this list could use it
	// where a database's own count is meant.
	readers []string
}

// ownedFieldClasses are the classes of fields that hold entity counts, quota
// usage or suppression state: only their owners change them.
var ownedFieldClasses = map[string]bool{"entity-count": true, "derived-count": true, "quota": true, "suppression": true}

// fieldClasses classifies, by field, every numeric field of a state package
// that the module writes (a field of integer, float or atomic integer type,
// or a map, slice or array of them), and the non-numeric fields that hold
// suppression state. A written numeric field with no row is a violation.
var fieldClasses = map[string]fieldRow{
	// Entity counts: applied by the engine's count publication (after a
	// direct write, a transaction commit and a write-behind replay), the
	// prefix delete, the recount at open, and construction and close.
	"pkg/storage.BadgerEngine.nodeCount": {class: "entity-count", owners: []string{
		"pkg/storage.BadgerEngine.DeleteByPrefix", "pkg/storage.BadgerEngine.cacheOnNodeCreatedWithCounts", "pkg/storage.BadgerEngine.cacheOnNodeDeletedWithCounts",
		"pkg/storage.BadgerEngine.cacheOnNodesDeleted", "pkg/storage.BadgerEngine.initializeCounts", "pkg/storage.BadgerEngine.publishBufferedTotalCounts"}, readers: []string{"pkg/storage.BadgerEngine.NodeCount", "pkg/storage.BadgerEngine.labelCoversScope"}},
	"pkg/storage.BadgerEngine.edgeCount": {class: "entity-count", owners: []string{
		"pkg/storage.BadgerEngine.DeleteByPrefix", "pkg/storage.BadgerEngine.cacheOnEdgeCreatedWithCounts", "pkg/storage.BadgerEngine.cacheOnEdgeDeletedWithCounts",
		"pkg/storage.BadgerEngine.cacheOnEdgesDeleted", "pkg/storage.BadgerEngine.cacheOnNodeDeletedWithCounts", "pkg/storage.BadgerEngine.cacheOnNodesDeleted",
		"pkg/storage.BadgerEngine.initializeCounts", "pkg/storage.BadgerEngine.publishBufferedTotalCounts"}, readers: []string{"pkg/storage.BadgerEngine.EdgeCount"}},
	"pkg/storage.BadgerEngine.namespaceNodeCounts": {class: "entity-count", owners: []string{
		"pkg/storage.BadgerEngine.DeleteByPrefix", "pkg/storage.BadgerEngine.addNamespaceNodeCount", "pkg/storage.BadgerEngine.cacheOnNodesDeleted",
		"pkg/storage.BadgerEngine.initializeCounts", "pkg/storage.BadgerEngine.releaseClosedStateLocked", "pkg/storage.NewBadgerEngineWithOptions"}, readers: []string{"pkg/storage.BadgerEngine.entityCountByPrefix", "pkg/storage.BadgerEngine.ListNamespaces"}},
	"pkg/storage.BadgerEngine.namespaceEdgeCounts": {class: "entity-count", owners: []string{
		"pkg/storage.BadgerEngine.DeleteByPrefix", "pkg/storage.BadgerEngine.addNamespaceEdgeCount", "pkg/storage.BadgerEngine.addNamespaceEdgeCountFromNode",
		"pkg/storage.BadgerEngine.cacheOnEdgesDeleted", "pkg/storage.BadgerEngine.cacheOnNodesDeleted", "pkg/storage.BadgerEngine.initializeCounts",
		"pkg/storage.BadgerEngine.releaseClosedStateLocked", "pkg/storage.NewBadgerEngineWithOptions"}, readers: []string{"pkg/storage.BadgerEngine.entityCountByPrefix", "pkg/storage.BadgerEngine.ListNamespaces"}},
	// Counts of acknowledged write-behind commits not yet applied.
	"pkg/storage.bufferedCardinalityDeltas.nodes":     {class: "entity-count", owners: []string{"pkg/storage.bufferedCardinalityDeltas.add"}, readers: []string{"pkg/storage.bufferedCardinalityDeltas.count"}},
	"pkg/storage.bufferedCardinalityDeltas.edges":     {class: "entity-count", owners: []string{"pkg/storage.bufferedCardinalityDeltas.add"}, readers: []string{"pkg/storage.bufferedCardinalityDeltas.count"}},
	"pkg/storage.bufferedCardinalityDeltas.nodeTotal": {class: "entity-count", owners: []string{"pkg/storage.bufferedCardinalityDeltas.add"}, readers: []string{"pkg/storage.bufferedCardinalityDeltas.count"}},
	"pkg/storage.bufferedCardinalityDeltas.edgeTotal": {class: "entity-count", owners: []string{"pkg/storage.bufferedCardinalityDeltas.add"}, readers: []string{"pkg/storage.bufferedCardinalityDeltas.count"}},
	"pkg/storage.CommitBuffer.edgeCreateCount":        {class: "entity-count", owners: []string{"pkg/storage.CommitBuffer.AddCommit", "pkg/storage.CommitBuffer.addEdge"}},
	"pkg/storage.CommitBuffer.edgeDeleteCount":        {class: "entity-count", owners: []string{"pkg/storage.CommitBuffer.AddCommit"}},
	// Derived counts a transaction stages until it commits.
	"pkg/storage.BadgerTransaction.pendingLabelCountDeltas":         {class: "derived-count", owners: []string{"pkg/storage.BadgerTransaction.bufferAdjustLabelCount", "pkg/storage.BadgerTransaction.closeLocked"}, readers: []string{"pkg/storage.BadgerTransaction.writeScalarCountDeltasInCommitLocked", "pkg/storage.BadgerTransaction.commitBufferedLocked", "pkg/storage.BadgerTransaction.CommitContextWithPublication", "pkg/storage.BadgerTransaction.PendingNodeLabelCountDelta"}},
	"pkg/storage.BadgerTransaction.pendingEdgeTypeCountDeltas":      {class: "derived-count", owners: []string{"pkg/storage.BadgerTransaction.bufferAdjustEdgeTypeCount", "pkg/storage.BadgerTransaction.closeLocked"}, readers: []string{"pkg/storage.BadgerTransaction.writeScalarCountDeltasInCommitLocked", "pkg/storage.BadgerTransaction.commitBufferedLocked", "pkg/storage.BadgerTransaction.CommitContextWithPublication", "pkg/storage.BadgerTransaction.PendingEdgeTypeCountDelta"}},
	"pkg/storage.BadgerTransaction.pendingEdgeTypeLabelCountDeltas": {class: "derived-count", owners: []string{"pkg/storage.BadgerTransaction.bufferAdjustEdgeTypeLabelCount", "pkg/storage.BadgerTransaction.closeLocked"}, readers: []string{"pkg/storage.BadgerTransaction.writeScalarCountDeltasInCommitLocked", "pkg/storage.BadgerTransaction.commitBufferedLocked", "pkg/storage.BadgerTransaction.CommitContextWithPublication", "pkg/storage.BadgerTransaction.acquireCommitPublicationLocked"}},
	// Suppression: whether reads must consult manual suppression at all.
	"pkg/storage.BadgerEngine.manualSuppressionCount": {class: "suppression", owners: []string{
		"pkg/storage.BadgerEngine.dropManualSuppressions", "pkg/storage.BadgerEngine.forgetManualSuppression",
		"pkg/storage.BadgerEngine.loadManualSuppressions", "pkg/storage.BadgerEngine.markManualSuppression"}, readers: []string{"pkg/storage.BadgerEngine.manuallySuppressed", "pkg/storage.BadgerEngine.visibilityFiltered", "pkg/storage.BadgerEngine.HasManualSuppressions"}},
	// Quota usage: the cached stored bytes of each database.
	"pkg/multidb.DatabaseInfo.nodeSize":         {class: "quota", owners: []string{"pkg/multidb.DatabaseManager.applyStorageSizeDelta", "pkg/multidb.DatabaseManager.ensureStorageSizeInitialized", "pkg/multidb.databaseLimitChecker.getCurrentStorageSize"}, readers: []string{"pkg/multidb.DatabaseManager.GetStorageSize", "pkg/multidb.cloneDatabaseInfo"}},
	"pkg/multidb.DatabaseInfo.edgeSize":         {class: "quota", owners: []string{"pkg/multidb.DatabaseManager.applyStorageSizeDelta", "pkg/multidb.DatabaseManager.ensureStorageSizeInitialized", "pkg/multidb.databaseLimitChecker.getCurrentStorageSize"}, readers: []string{"pkg/multidb.DatabaseManager.GetStorageSize", "pkg/multidb.cloneDatabaseInfo"}},
	"pkg/multidb.DatabaseInfo.totalSize":        {class: "quota", owners: []string{"pkg/multidb.DatabaseManager.applyStorageSizeDelta", "pkg/multidb.DatabaseManager.ensureStorageSizeInitialized", "pkg/multidb.databaseLimitChecker.getCurrentStorageSize"}, readers: []string{"pkg/multidb.DatabaseManager.GetStorageSize", "pkg/multidb.cloneDatabaseInfo"}},
	"pkg/multidb.databaseLimitChecker.nodeSize": {class: "quota", owners: []string{"pkg/multidb.databaseLimitChecker.getCurrentStorageSize"}},
	"pkg/multidb.databaseLimitChecker.edgeSize": {class: "quota", owners: []string{"pkg/multidb.databaseLimitChecker.getCurrentStorageSize"}},

	// These are explicit per-field classifications, not type exemptions.
	// MarshalJSON owns the request buffer; it never publishes stored entities.
	"pkg/storage.remoteHTTPWriter.buf": {class: "request serialization scratch"},
	// QueryCypher owns the resolved mode; Fabric consumes it to claim writes.
	"pkg/storage.remoteCypherTx.resolvedMode": {class: "resolved remote query mode"},

	// Not entity state.
	"pkg/storage.BadgerEngine.activeMVCCSnapshotReaders":         {class: "snapshot readers"},
	"pkg/storage.BadgerEngine.adjCacheMaxNodes":                  {class: "configuration"},
	"pkg/storage.BadgerEngine.edgeCacheMaxItems":                 {class: "configuration"},
	"pkg/storage.BadgerEngine.edgeTypeCacheMaxTypes":             {class: "configuration"},
	"pkg/storage.BadgerEngine.labelFirstCacheMax":                {class: "configuration"},
	"pkg/storage.BadgerEngine.nodeBodyCacheMaxBytes":             {class: "configuration"},
	"pkg/storage.BadgerEngine.nodeCacheMaxEntries":               {class: "configuration"},
	"pkg/storage.BadgerEngine.cacheHits":                         {class: "metrics"},
	"pkg/storage.BadgerEngine.cacheMisses":                       {class: "metrics"},
	"pkg/storage.BadgerEngine.countDeltaPending":                 {class: "count-delta fold scheduling"},
	"pkg/storage.BadgerEngine.edgeCacheVersions":                 {class: "read cache"},
	"pkg/storage.BadgerEngine.nodeBodyCacheBytes":                {class: "read cache"},
	"pkg/storage.BadgerEngine.mvccLegacyGlobalSeed":              {class: "MVCC sequence"},
	"pkg/storage.BadgerEngine.storageVersion":                    {class: "store metadata"},
	"pkg/storage.BadgerTransaction.badgerReadTs":                 {class: "transaction snapshot"},
	"pkg/storage.BadgerTransaction.bufferEpoch":                  {class: "transaction snapshot"},
	"pkg/storage.BadgerTransaction.lifecycleSeq":                 {class: "transaction snapshot"},
	"pkg/storage.BadgerTransaction.pendingWrites":                {class: "transaction staging size"},
	"pkg/storage.BadgerTransaction.snapshotEdgeBytes":            {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotEdgeBytesByID":        {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotLabelPrefixBytes":     {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotLabelPrefixNodeBytes": {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotNodeLabelBytes":       {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotPrefixNodeBytes":      {class: "transaction read cache"},
	"pkg/storage.BadgerTransaction.snapshotPrefixNodeBytesByID":  {class: "transaction read cache"},
	"pkg/storage.CommitBuffer.deletes":                           {class: "write-behind generation contents"},
	"pkg/storage.CommitBuffer.ops":                               {class: "write-behind generation size"},
	"pkg/storage.WriteBehindBuffer.curInterval":                  {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.drainDur":                     {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.drainRate":                    {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.maxDelay":                     {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.minDelay":                     {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.opCount":                      {class: "write-behind generation size"},
	"pkg/storage.WriteBehindBuffer.targetOps":                    {class: "write-behind scheduling"},
	"pkg/storage.WriteBehindBuffer.warnState":                    {class: "write-behind scheduling"},
	"pkg/multidb.DatabaseManager.statementCacheGeneration":       {class: "statement cache"},
	"pkg/multidb.DatabaseManager.statementCacheIndexID":          {class: "statement cache"},
	"pkg/multidb.databaseLimitChecker.activeQueries":             {class: "query concurrency"},
}

// typeClasses classify, as a whole, struct types that hold no stored-entity
// state: entity values passed by value, configuration, limits, reports,
// WAL and dictionary bookkeeping, statistics. A type in stateTypes may not
// have a row here: its fields are classified one by one.
var typeClasses = map[string]string{
	"pkg/storage.Node":                         "entity value (a copy, not stored state)",
	"pkg/storage.Edge":                         "entity value (a copy, not stored state)",
	"pkg/storage.SchemaDefinition":             "schema value",
	"pkg/storage.RangeIndex":                   "schema index structure",
	"pkg/storage.SchemaManager":                "schema constraint locks",
	"pkg/storage.uniqueConstraintCommitLock":   "schema constraint locks",
	"pkg/storage.NodeConfig":                   "per-node edge caps and trust (node configuration)",
	"pkg/storage.NodeConfigStore":              "per-node edge caps and trust (node configuration)",
	"pkg/storage.EdgeMetaStore":                "edge metadata statistics",
	"pkg/storage.WAL":                          "WAL file bookkeeping",
	"pkg/storage.WALConfig":                    "configuration",
	"pkg/storage.WALEngine":                    "WAL snapshot bookkeeping",
	"pkg/storage.WALIntegrityReport":           "report",
	"pkg/storage.WALManifest":                  "WAL file bookkeeping",
	"pkg/storage.RecoveryStreamStatus":         "report",
	"pkg/storage.ReplayResult":                 "report",
	"pkg/storage.changeCapture":                "configuration",
	"pkg/storage.changeLog":                    "change capture sequence",
	"pkg/storage.commitOracle":                 "commit timestamps and readers",
	"pkg/storage.deleteCleanup":                "background cleanup scheduling",
	"pkg/storage.edgeDictShard":                "id dictionary",
	"pkg/storage.nodeDictShard":                "id dictionary",
	"pkg/storage.idDictionary":                 "id dictionary",
	"pkg/storage.propertyKeyDictionary":        "property key dictionary",
	"pkg/storage.txnCounterState":              "id dictionary",
	"pkg/storage.edgeTypeCountSnapshot":        "a count snapshot under construction (local value)",
	"pkg/storage.embeddingRangeManifestRecord": "embedding sidecar layout",
	"pkg/storage.largeCommit":                  "large-commit batching",
	"pkg/storage.namespaceMVCCState":           "MVCC timestamps",
	"pkg/storage.nodeIndexReader":              "a scan cursor",
	"pkg/storage.orderHold":                    "write-order stripes (the acquisition itself)",
	"pkg/storage.overlayScopeEpoch":            "write-behind read scopes",
	"pkg/storage.pendingEmbeddingCandidates":   "pending-embedding index cache",
	"pkg/storage.prefixDrops":                  "prefix-delete sequence",
	"pkg/storage.projectedNodeDecoder":         "a decoder's scratch state",
	"pkg/storage.temporalCreationGroups":       "a temporal index build's scratch state",
	"pkg/storage.v1ToV2IndexStats":             "migration statistics",
	"pkg/storage.v1ToV2PassStats":              "migration statistics",
	"pkg/multidb.ConnectionLimits":             "limits configuration",
	"pkg/multidb.QueryLimits":                  "limits configuration",
	"pkg/multidb.RateLimits":                   "limits configuration",
	"pkg/multidb.StorageLimits":                "limits configuration",
	"pkg/multidb.ConnectionTracker":            "connection counts (not stored state)",
	"pkg/multidb.rateLimiter":                  "rate-limit tokens",
}

// stateTypes hold stored-entity counts, quota usage or suppression state
// among other fields: each of their written numeric fields needs its own row.
var stateTypes = map[string]bool{
	"pkg/storage.BadgerEngine": true, "pkg/storage.BadgerTransaction": true, "pkg/storage.WriteBehindBuffer": true,
	"pkg/storage.CommitBuffer": true, "pkg/storage.bufferedCardinalityDeltas": true,
	"pkg/multidb.DatabaseInfo": true, "pkg/multidb.DatabaseManager": true, "pkg/multidb.databaseLimitChecker": true,
}

// typeOf is the struct type part of a field name.
func typeOf(field string) string {
	return field[:strings.LastIndex(field, ".")]
}

// classOf is a field's row, from its own row or its type's.
func classOf(field string) (fieldRow, bool) {
	if row, ok := fieldClasses[field]; ok {
		return row, true
	}
	if c, ok := typeClasses[typeOf(field)]; ok && !stateTypes[typeOf(field)] {
		return fieldRow{class: c}, true
	}
	return fieldRow{}, false
}

// numeric reports whether t holds a count, a size or another number.
func numeric(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Info()&(types.IsInteger|types.IsFloat) != 0
	case *types.Map:
		return numeric(u.Elem())
	case *types.Slice:
		return numeric(u.Elem())
	case *types.Array:
		return numeric(u.Elem())
	case *types.Struct:
		if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "sync/atomic" {
			return strings.Contains(n.Obj().Name(), "Int")
		}
	}
	return false
}

// fieldWrite is one change of a field.
type fieldWrite struct {
	field, fn, pos string
}

// field names a struct field as "pkg/storage.Type.field" when its struct is
// a named type of a state package.
func (p *program) field(st types.Type, index int) (string, *types.Var) {
	if ptr, ok := st.Underlying().(*types.Pointer); ok {
		st = ptr.Elem()
	}
	named, ok := st.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return "", nil
	}
	path := strings.TrimPrefix(named.Obj().Pkg().Path(), Module)
	state := false
	for _, s := range statePackages {
		state = state || path == s
	}
	if !state {
		return "", nil
	}
	s, ok := named.Underlying().(*types.Struct)
	if !ok || index >= s.NumFields() {
		return "", nil
	}
	f := s.Field(index)
	return path + "." + named.Obj().Name() + "." + f.Name(), f
}

// rootFields are the state fields an address or map value v designates.
func (a *analysis) rootFields(v ssa.Value, seen map[ssa.Value]bool) []string {
	if seen[v] {
		return nil
	}
	seen[v] = true
	switch v := v.(type) {
	case *ssa.FieldAddr:
		if fresh(v.X) {
			return nil // initializing a value this function allocates
		}
		if name, f := a.p.field(v.X.Type().Underlying().(*types.Pointer).Elem(), v.Field); name != "" && f != nil {
			return []string{name}
		}
		return a.rootFields(v.X, seen)
	case *ssa.Field:
		if name, f := a.p.field(v.X.Type(), v.Field); name != "" && f != nil {
			return []string{name}
		}
	case *ssa.IndexAddr:
		return a.rootFields(v.X, seen)
	case *ssa.UnOp:
		return a.rootFields(v.X, seen)
	case *ssa.Phi:
		var out []string
		for _, e := range v.Edges {
			out = append(out, a.rootFields(e, seen)...)
		}
		return out
	}
	return nil
}

// fresh reports whether v is, through field and element addresses, a value
// its function allocates.
func fresh(v ssa.Value) bool {
	for i := 0; i < 16; i++ {
		switch x := v.(type) {
		case *ssa.Alloc:
			return true
		case *ssa.FieldAddr:
			v = x.X
		case *ssa.IndexAddr:
			v = x.X
		default:
			return false
		}
	}
	return false
}

// fieldWrites finds every change of a state field: a store, a map update or
// delete, an atomic method, and an address of a field passed on (from then
// on the census cannot follow it, so passing it counts as changing it).
func (a *analysis) fieldWrites() []fieldWrite {
	var out []fieldWrite
	add := func(fields []string, fn *ssa.Function, ins ssa.Instruction) {
		for _, f := range fields {
			out = append(out, fieldWrite{field: f, fn: a.p.funcName(fn), pos: a.p.pos(ins.Pos())})
		}
	}
	for _, fn := range a.p.funcs {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch ins := ins.(type) {
				case *ssa.Store:
					add(a.rootFields(ins.Addr, map[ssa.Value]bool{}), fn, ins)
				case *ssa.MapUpdate:
					add(a.rootFields(ins.Map, map[ssa.Value]bool{}), fn, ins)
				case ssa.CallInstruction:
					c := ins.Common()
					if b, ok := c.Value.(*ssa.Builtin); ok && b.Name() == "delete" && len(c.Args) > 0 {
						add(a.rootFields(c.Args[0], map[ssa.Value]bool{}), fn, ins)
						continue
					}
					callee := c.StaticCallee()
					for i, arg := range c.Args {
						fa, ok := arg.(*ssa.FieldAddr)
						if !ok {
							continue
						}
						if i == 0 && callee != nil && callee.Signature.Recv() != nil {
							// A method of the field's own type changes it
							// only through its own fields, except atomics.
							if pkg := callee.Pkg; pkg == nil || pkg.Pkg.Path() != "sync/atomic" || !mutating(callee.Name()) {
								continue
							}
						}
						add(a.rootFields(fa, map[ssa.Value]bool{}), fn, ins)
					}
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].field != out[j].field {
			return out[i].field < out[j].field
		}
		return out[i].pos < out[j].pos
	})
	return out
}

// fieldReads finds every read of a state field: a load through its
// address and an atomic Load.
func (a *analysis) fieldReads() []fieldWrite {
	var out []fieldWrite
	for _, fn := range a.p.funcs {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				var fields []string
				switch ins := ins.(type) {
				case *ssa.UnOp:
					if ins.Op == token.MUL {
						fields = a.rootFields(ins.X, map[ssa.Value]bool{})
					}
				case *ssa.Call:
					c := ins.Common()
					if callee := c.StaticCallee(); callee != nil && callee.Pkg != nil && callee.Pkg.Pkg.Path() == "sync/atomic" && !mutating(callee.Name()) && len(c.Args) > 0 {
						fields = a.rootFields(c.Args[0], map[ssa.Value]bool{})
					}
				}
				for _, f := range fields {
					out = append(out, fieldWrite{field: f, fn: a.p.funcName(fn), pos: a.p.pos(ins.Pos())})
				}
			}
		}
	}
	return out
}

func mutating(name string) bool {
	switch name {
	case "Load":
		return false
	}
	return true
}

// checkFields reports written numeric fields with no row, changes of owned
// fields outside their owners, and rows that no longer match the code.
func (a *analysis) checkFields(r *Report) {
	written := map[string]bool{}
	for _, w := range a.fieldWrites() {
		_, f := a.lookupField(w.field)
		numericField := f != nil && numeric(f.Type())
		row, ok := classOf(w.field)
		written[w.field] = true
		switch {
		case !ok && numericField:
			r.Violations = append(r.Violations, fmt.Sprintf("unclassified field: %s is changed at %s in %s", w.field, w.pos, w.fn))
		case ok && ownedFieldClasses[row.class] && !contains(row.owners, w.fn) && !contains(row.owners, outer(w.fn)):
			r.Violations = append(r.Violations, fmt.Sprintf("unowned %s change: %s at %s in %s (owners: %s)", row.class, w.field, w.pos, w.fn, strings.Join(row.owners, ", ")))
		}
		if _, own := fieldClasses[w.field]; own {
			r.FieldWrites = append(r.FieldWrites, FieldWriteRow{Field: w.field, Class: row.class, Func: w.fn, Pos: w.pos})
		}
	}
	for _, rd := range a.fieldReads() {
		row, ok := classOf(rd.field)
		if !ok || !ownedFieldClasses[row.class] {
			continue
		}
		if contains(row.owners, rd.fn) || contains(row.owners, outer(rd.fn)) || contains(row.readers, rd.fn) || contains(row.readers, outer(rd.fn)) {
			continue
		}
		r.Violations = append(r.Violations, fmt.Sprintf("unowned %s read: %s at %s in %s (readers: %s)", row.class, rd.field, rd.pos, rd.fn, strings.Join(append(append([]string(nil), row.owners...), row.readers...), ", ")))
	}
	for _, name := range collection.SortedKeys(typeClasses) {
		if stateTypes[name] {
			r.Violations = append(r.Violations, "type row for a state type: "+name+" holds entity state; classify its fields one by one")
		}
		if named, _ := a.lookupField(name + ".-"); named == nil {
			r.Violations = append(r.Violations, "stale: type row "+name+" names no type")
		}
	}
	funcs := map[string]bool{}
	for _, fn := range a.p.funcs {
		funcs[a.p.funcName(fn)] = true
	}
	for _, name := range collection.SortedKeys(fieldClasses) {
		if !written[name] {
			r.Violations = append(r.Violations, "stale: field row "+name+" names no written field")
		}
		row := fieldClasses[name]
		for _, fn := range append(append([]string(nil), row.owners...), row.readers...) {
			if !funcs[fn] {
				r.Violations = append(r.Violations, "stale: field row "+name+" lists "+fn+", which is no function")
			}
		}
	}
}

// outer is a closure's outermost enclosing function.
func outer(fn string) string {
	if i := strings.Index(fn, "$"); i >= 0 {
		return fn[:i]
	}
	return fn
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// lookupField finds a field by its census name.
func (a *analysis) lookupField(name string) (*types.Named, *types.Var) {
	dot := strings.LastIndex(name, ".")
	typeDot := strings.LastIndex(name[:dot], ".")
	pkgPath, typeName, fieldName := name[:typeDot], name[typeDot+1:dot], name[dot+1:]
	pkg := a.p.ssa.ImportedPackage(Module + pkgPath)
	if pkg == nil {
		return nil, nil
	}
	tn, ok := pkg.Pkg.Scope().Lookup(typeName).(*types.TypeName)
	if !ok {
		return nil, nil
	}
	named, ok := tn.Type().(*types.Named)
	if !ok {
		return nil, nil
	}
	s, ok := named.Underlying().(*types.Struct)
	if !ok {
		return named, nil
	}
	for i := 0; i < s.NumFields(); i++ {
		if s.Field(i).Name() == fieldName {
			return named, s.Field(i)
		}
	}
	return named, nil
}

// FieldWriteRow is a classified field change.
type FieldWriteRow struct {
	Field string `json:"field"`
	Class string `json:"class"`
	Func  string `json:"func"`
	Pos   string `json:"pos"`
}
