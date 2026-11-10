package storagecensus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// importableFieldFixture builds the one-package fixture the field classifier
// can resolve: ssaFixture's primary package is deliberately not importable, so
// lookupField cannot read a fixture field's type from it and the
// unclassified-field rule never fires. This fixture registers its package as
// importable, so an unreviewed numeric field and a lookalike type reach that
// rule.
func importableFieldFixture(t *testing.T, source string) (*program, *ssa.Package) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "coverage.go", source, 0)
	require.NoError(t, err)
	checked, info, err := ssautil.BuildPackage(&types.Config{}, fset, types.NewPackage(Module+"pkg/storage", "storage"), []*ast.File{file}, ssa.SanityCheckFunctions)
	require.NoError(t, err)
	prog := ssa.NewProgram(fset, ssa.SanityCheckFunctions)
	pkg := prog.CreatePackage(checked.Pkg, []*ast.File{file}, info, true)
	pkg.Build()
	p := &program{fset: fset, ssa: prog}
	require.Same(t, pkg, prog.ImportedPackage(Module+"pkg/storage"))
	for fn := range ssautil.AllFunctions(prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	return p, pkg
}

// entityWriteLocks.latest counts the held locks whose entity a commit the
// transaction's snapshot does not hold already committed: only
// lockEntityForWrite changes it, and readsLatestLocked and
// enrollCommittedWriteLocked read it. The index selects the lock whose
// latest flag guards conflict validation, so its writer is constrained too.
// A rogue writer or reader of latest, an unreviewed field and a lookalike type
// still fail.
func TestEntityWriteLockFieldsRetainExactPublicationOwners(t *testing.T) {
	p, _ := importableFieldFixture(t, `package storage
 type entityWriteLock struct{latest bool}
 type entityWriteLocks struct { held []entityWriteLock; index map[string]int; latest int; unreviewed int }
 type entityWriteLocksLookalike struct { held []entityWriteLock; index map[string]int; latest int }
 type BadgerTransaction struct{entityLocks entityWriteLocks}
 type lookalikeHolder struct{locks entityWriteLocksLookalike}
 func(l *entityWriteLocks)add(key string)*entityWriteLock{l.held=append(l.held,entityWriteLock{});if l.index!=nil{l.index[key]=len(l.held)-1};return &l.held[len(l.held)-1]}
 func(l *entityWriteLocks)find(key string)*entityWriteLock{if l.index!=nil{if at,found:=l.index[key];found{return &l.held[at]}};for at:=range l.held{if at==0{return &l.held[at]}};return nil}
 func(tx *BadgerTransaction)lockEntityForWrite(){held:=tx.entityLocks.add("k");held.latest=true;tx.entityLocks.latest++}
 func(tx *BadgerTransaction)readsLatestLocked()bool{return tx.entityLocks.latest!=0}
 func(tx *BadgerTransaction)enrollCommittedWriteLocked()bool{return tx.entityLocks.latest!=0}
 func rogueWrite(tx *BadgerTransaction){tx.entityLocks.latest++}
 func rogueRead(tx *BadgerTransaction)int{return tx.entityLocks.latest}
 func rogueIndex(tx *BadgerTransaction){tx.entityLocks.index["k"]=0}
 func unreviewedWrite(tx *BadgerTransaction){tx.entityLocks.unreviewed++}
 func lookalikeWrite(h *lookalikeHolder){h.locks.latest++;h.locks.index["k"]=0}
 `)
	r := &Report{}
	(&analysis{p: p}).checkFields(r)
	text := strings.Join(r.Violations, "\n")
	name := "pkg/storage.entityWriteLocks.latest"
	row, ok := classOf(name)
	require.True(t, ok)
	require.Equal(t, "publication-order", row.class)
	require.NotEmpty(t, row.owners)
	for _, misuse := range []struct{ kind, caller string }{{"change", "rogueWrite"}, {"read", "rogueRead"}} {
		found := false
		for _, violation := range r.Violations {
			found = found || strings.Contains(violation, "unowned publication-order "+misuse.kind+": "+name) && strings.Contains(violation, "in pkg/storage."+misuse.caller)
		}
		require.True(t, found, "%s must reject %s in %s", name, misuse.kind, misuse.caller)
	}
	for _, owner := range []string{"lockEntityForWrite", "readsLatestLocked", "enrollCommittedWriteLocked"} {
		require.NotContains(t, text, "in pkg/storage.BadgerTransaction."+owner)
	}
	index, ok := classOf("pkg/storage.entityWriteLocks.index")
	require.True(t, ok, "the lock lookup index is reviewed, so it is not an unclassified field")
	require.Equal(t, "publication-order", index.class)
	require.Equal(t, []string{"pkg/storage.entityWriteLocks.add"}, index.owners)
	require.Contains(t, text, "unowned publication-order change: pkg/storage.entityWriteLocks.index")
	require.NotContains(t, text, "in pkg/storage.entityWriteLocks.add")
	require.NotContains(t, text, "in pkg/storage.entityWriteLocks.find")
	require.Contains(t, text, "unclassified field: pkg/storage.entityWriteLocks.unreviewed is changed")
	_, ok = classOf("pkg/storage.entityWriteLocksLookalike.latest")
	require.False(t, ok)
	_, ok = classOf("pkg/storage.entityWriteLocksLookalike.index")
	require.False(t, ok)
	require.Contains(t, text, "unclassified field: pkg/storage.entityWriteLocksLookalike.latest is changed")
	require.Contains(t, text, "unclassified field: pkg/storage.entityWriteLocksLookalike.index is changed")
}
