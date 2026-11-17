package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// procedureRegistrationTable names the one registry table a scoped fixture
// registers into and whether that table also owns a case-fold index. A user
// registration resolves before a built-in one, so a cleanup restores exactly
// the table the fixture wrote.
type procedureRegistrationTable struct {
	entries func(*ProcedureRegistry) map[string]registeredProcedure
	folded  bool
}

var (
	userProcedureTable = procedureRegistrationTable{
		entries: func(r *ProcedureRegistry) map[string]registeredProcedure { return r.user },
		folded:  true,
	}
	builtInProcedureTable = procedureRegistrationTable{
		entries: func(r *ProcedureRegistry) map[string]registeredProcedure { return r.builtins },
	}
)

// scopeProcedureRegistration restores only the registration a fixture owns in
// table. A fixture calls it before it registers, so the exact entry it captured
// is the state to put back: a previous registration is restored unchanged (a
// contract another fixture registered under that name survives), while an entry
// this fixture created is removed together with its case fold when no remaining
// entry shares the name. Every other registration is left untouched.
//
// Cleanup advances registry.generation whenever it changes a table, so a cached
// lookup cannot keep serving the registration the fixture replaced or removed.
func scopeProcedureRegistration(t *testing.T, name string, table procedureRegistrationTable) {
	t.Helper()
	r := globalProcedureRegistry
	r.mu.RLock()
	previous, existed := table.entries(r)[name]
	r.mu.RUnlock()
	t.Cleanup(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		entries := table.entries(r)
		if existed {
			entries[name] = previous
			r.generation.Add(1)
			return
		}
		if _, owned := entries[name]; !owned {
			return
		}
		delete(entries, name)
		r.generation.Add(1)
		if !table.folded {
			return
		}
		fold := lowerASCII(name)
		for other := range entries {
			if lowerASCII(other) == fold {
				return
			}
		}
		delete(r.userFold, fold)
	})
}

const (
	scopedNeighbourProcedureName = "u5scope.neighbour"
	scopedRestoredProcedureName  = "u5scope.restored"
	scopedBuiltInProcedureName   = "u5scope.builtin"
	scopedRemovedProcedureName   = "u5scope.removed"
)

// scopedProcedureSpec distinguishes registrations under one name by the output
// they declare, so a restored entry is identifiable.
func scopedProcedureSpec(name, declared string) ProcedureSpec {
	return ProcedureSpec{
		Name:    name,
		Mode:    ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: declared, Type: "INTEGER"}},
	}
}

func scopedProcedureHandler(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
	return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
}

func scopedUserFold(name string) bool {
	globalProcedureRegistry.mu.RLock()
	defer globalProcedureRegistry.mu.RUnlock()
	_, folded := globalProcedureRegistry.userFold[lowerASCII(name)]
	return folded
}

// Scoped cleanup is the one owner that restores a test's registration, so it
// must restore or remove exactly the entry that test owns: a pre-existing exact
// registration comes back unchanged, an entry the fixture created disappears
// with its now-unused case fold, neighbouring registrations survive both, and
// registry.generation advances so a cached lookup cannot keep serving the
// registration the cleanup replaced or removed. A built-in fixture is scoped in
// the built-in table, where the same owner restores its previous registration.
func TestScopeProcedureRegistrationOwnsOnlyItsEntry(t *testing.T) {
	registerProcedureForTest(t, scopedProcedureSpec(scopedNeighbourProcedureName, "neighbour"), scopedProcedureHandler)
	// Other fixtures' contracts under the restored names: the scoped cleanup
	// puts them back instead of deleting them.
	registerProcedureForTest(t, scopedProcedureSpec(scopedRestoredProcedureName, "previous"), scopedProcedureHandler)
	registerBuiltInProcedureForTest(t, scopedProcedureSpec(scopedBuiltInProcedureName, "previousBuiltIn"), scopedProcedureHandler)

	declared := func(t *testing.T, name string) string {
		t.Helper()
		registered, found := globalProcedureRegistry.Get(name)
		require.True(t, found, "registration %s must exist", name)
		return registered.Spec.Returns[0].Name
	}

	var restoredGeneration uint64
	t.Run("restores a pre-existing registration", func(t *testing.T) {
		registerProcedureForTest(t, scopedProcedureSpec(scopedRestoredProcedureName, "replacement"), scopedProcedureHandler)
		restoredGeneration = globalProcedureRegistry.generation.Load()
		require.Equal(t, "replacement", declared(t, scopedRestoredProcedureName))
	})
	require.Greater(t, globalProcedureRegistry.generation.Load(), restoredGeneration,
		"cleanup advances the registry generation")
	require.Equal(t, "previous", declared(t, scopedRestoredProcedureName),
		"the pre-existing registration is restored, not deleted")
	require.True(t, scopedUserFold(scopedRestoredProcedureName),
		"the restored registration keeps its case fold")

	var builtInGeneration uint64
	t.Run("restores a pre-existing built-in registration", func(t *testing.T) {
		registerBuiltInProcedureForTest(t, scopedProcedureSpec(scopedBuiltInProcedureName, "replacementBuiltIn"), scopedProcedureHandler)
		builtInGeneration = globalProcedureRegistry.generation.Load()
		require.Equal(t, "replacementBuiltIn", declared(t, scopedBuiltInProcedureName))
	})
	require.Greater(t, globalProcedureRegistry.generation.Load(), builtInGeneration,
		"cleanup advances the registry generation")
	require.Equal(t, "previousBuiltIn", declared(t, scopedBuiltInProcedureName),
		"the pre-existing built-in registration is restored, not deleted")

	var removedGeneration uint64
	t.Run("removes the registration it created", func(t *testing.T) {
		registerProcedureForTest(t, scopedProcedureSpec(scopedRemovedProcedureName, "removed"), scopedProcedureHandler)
		removedGeneration = globalProcedureRegistry.generation.Load()
	})
	require.Greater(t, globalProcedureRegistry.generation.Load(), removedGeneration,
		"cleanup advances the registry generation")
	if _, found := globalProcedureRegistry.Get(scopedRemovedProcedureName); found {
		t.Fatal("the registration this fixture created must be removed")
	}
	require.False(t, scopedUserFold(scopedRemovedProcedureName),
		"the unused case fold is removed with its entry")
	require.Equal(t, "neighbour", declared(t, scopedNeighbourProcedureName),
		"a neighbouring registration survives the scoped cleanup")
}
