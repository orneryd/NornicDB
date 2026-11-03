package capturecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestInstalledRollbackPhaseRecord(t *testing.T) {
	config := storageNilContextConfig(t, "./...")
	program, err := workcensus.LoadProgram(config)
	require.NoError(t, err)
	const pkg = "github.com/orneryd/nornicdb/pkg/storage"
	const owner = "(*" + pkg + ".managedBadgerDB).rollbackSince"
	findOwner := func(program *workcensus.Program) *ssa.Function {
		t.Helper()
		for _, fn := range program.Functions {
			if fn.Synthetic == "" && symbolOf(fn) == owner {
				return fn
			}
		}
		t.Fatal("missing rollback owner")
		return nil
	}
	fn := findOwner(program)
	var origin ssa.Value
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(*ssa.Call)
			if ok && call.Common().StaticCallee() != nil && call.Common().StaticCallee().String() == "context.Background" {
				require.Nil(t, origin, "one compensation lifetime selection")
				origin = call
			}
		}
	}
	require.NotNil(t, origin)
	record, ok := StoragePolicy().ReviewedHeldValues[owner]
	require.True(t, ok)
	require.Len(t, record.EndsEdges, 2)
	require.Equal(t, "(*"+pkg+".largeCommit).abort", record.EndsEdges[0].Caller)
	require.Equal(t, "(*"+pkg+".managedBadgerDB).recoverInterruptedLargeCommit", record.EndsEdges[1].Caller)
	require.Contains(t, record.Approver, "explicit approval")
	held := heldValues(fn, nil, creationRoots(origin, fn, func(string) {}))
	require.Empty(t, reviewedHeldStale(program, fn, record, held, newSourceDigests(program)))
	require.Len(t, record.Callers[owner], 2)

	policy := Policy{Package: pkg, Native: "BadgerEngine", ContextOwners: []string{"(*" + pkg + ".managedBadgerDB).commitDurableWrite"}}
	seen := map[[3]string]bool{}
	chain, others, _, _ := requestChain(program, fn, record, policy, newPublicBoundary(program, policy), map[string]bool{}, map[string]bool{}, seen, map[string]string{}, newSourceDigests(program), &acceptanceLog{}, owner)
	require.Empty(t, chain)
	require.Empty(t, others)
	require.Len(t, seen, 2, "both exact compensation routes are exercised")
	chain, others, _, _ = requestChain(program, fn, ReviewedHeldValue{}, policy, newPublicBoundary(program, policy), map[string]bool{}, map[string]bool{}, map[[3]string]bool{}, map[string]string{}, newSourceDigests(program), &acceptanceLog{}, owner)
	require.NotZero(t, len(chain)+len(others), "unreviewed ancestry must remain rejected")

	for _, mutation := range []struct {
		name, file, from, to, reason string
	}{
		{"site-body", "badger_managed.go", "ctx := context.Background()", "ctx := context.Background(); _ = ctx.Err()", "source of"},
		{"abort-body", "badger_managed.go", "func (lc *largeCommit) abort() error {", "func (lc *largeCommit) abort() error { _ = lc.ctx;", "source of"},
		{"recovery-body", "badger_managed.go", "func (m *managedBadgerDB) recoverInterruptedLargeCommit() error {", "func (m *managedBadgerDB) recoverInterruptedLargeCommit() error { _ = m.DB;", "source of"},
		{"third-caller", "badger_managed.go", "", "\nfunc (m *managedBadgerDB) extraRepairCaller(ctx context.Context) error { _ = ctx.Err(); return m.rollbackSince(1) }\n", "callers of"},
		{"helper-body", "durable_cache_scope.go", "func (m *managedBadgerDB) commitDurableWrite(txn *badger.Txn, ts uint64, scope cacheWriteScope, ctx context.Context, journal *commitWriter) error {", "func (m *managedBadgerDB) commitDurableWrite(txn *badger.Txn, ts uint64, scope cacheWriteScope, ctx context.Context, journal *commitWriter) error { _ = ts;", "source of"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			path := filepath.Join(config.Dir, "pkg/storage", mutation.file)
			source, err := os.ReadFile(path)
			require.NoError(t, err)
			changed := string(source)
			if mutation.from == "" {
				changed += mutation.to
			} else {
				require.Equal(t, 1, strings.Count(changed, mutation.from))
				changed = strings.Replace(changed, mutation.from, mutation.to, 1)
			}
			overlay := config
			overlay.Overlay = map[string][]byte{path: []byte(changed)}
			mutant, err := workcensus.LoadProgram(overlay)
			require.NoError(t, err)
			mutantOwner := findOwner(mutant)
			stale := reviewedHeldStale(mutant, mutantOwner, record, heldValues(mutantOwner, nil, nil), newSourceDigests(mutant))
			require.Contains(t, stale, mutation.reason, "changed proof dependencies cannot inherit acceptance")
		})
	}
}
