package cypher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every function allowed to read a raw database source (the participant
// storage's underlying engine, the composite executor's base executor, the
// participant handle's source executor and raw engine) is still refused a
// write through it: a write inserted into each one is reported by the
// write-access guard, and the result metadata readers of the base executor
// may only call their metadata method. Each mutation type-checks.
func TestWriteAccessGuardReportsRawWritesInEveryAllowedReader(t *testing.T) {
	loader := newWriteAccessLoader(t)
	const probe = `_, _ = %s.CreateNode(&storage.Node{ID: storage.NodeID("guard-probe")})` + "\n\t"
	inject := func(receiver string) string { return strings.Replace(probe, "%s", receiver, 1) }
	cases := []struct {
		name, file, anchor, injected, want string
	}{
		{"metadata_columns", "executor_fabric.go", "return e.base.CachedResultColumns(", inject("e.base.storage"), "cypherFabricExecutor.base written through (CreateNode, in " + cypherPath + ".cypherFabricExecutor.CachedResultColumns)"},
		{"metadata_columns_other_call", "executor_fabric.go", "return e.base.CachedResultColumns(", "_ = e.base.currentDatabaseName()\n\t", "cypherFabricExecutor.base read other than to call CachedResultColumns (in " + cypherPath + ".cypherFabricExecutor.CachedResultColumns)"},
		{"metadata_projection", "executor_fabric.go", "return e.base.ResultProjection(", inject("e.base.storage"), "cypherFabricExecutor.base written through (CreateNode, in " + cypherPath + ".cypherFabricExecutor.ResultProjection)"},
		{"fragment_executor", "executor_fabric.go", "composite := c.statementFor(ctx, tx)", inject("c.base.storage"), "cypherFabricExecutor.base written through (CreateNode, in " + cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord)"},
		{"record_query", "executor_fabric.go", "executor := c.base.cloneForStorage(c.base.storage)", "_, _ = c.base.Execute(ctx, \"CREATE (n)\", nil)\n\t", "cypherFabricExecutor.base written through (Execute, in " + cypherPath + ".cypherFabricExecutor.ExecuteRecordQuery)"},
		{"participants_map", "fabric_participants.go", "if !c.autoCommit && c.base != nil && c.base.txContext != nil && c.base.txContext.active {", inject("c.base.storage"), "cypherFabricExecutor.base written through (CreateNode, in " + cypherPath + ".cypherFabricExecutor.participants)"},
		{"participant_source", "fabric_participants.go", "source := c.base.cloneForStorage(engine)", inject("c.base.storage"), "cypherFabricExecutor.base written through (CreateNode, in " + cypherPath + ".compositeStatement.participant)"},
		{"handle_open_source", "fabric_participants.go", "terminal := context.WithoutCancel(h.begin)", "_, _ = h.source.Execute(h.begin, \"CREATE (n)\", nil)\n\t", "participantHandle.source written through (Execute, in " + cypherPath + ".participantHandle.open)"},
		{"handle_open_engine", "fabric_participants.go", "terminal := context.WithoutCancel(h.begin)", inject("h.engine"), "participantHandle.engine written through (CreateNode, in " + cypherPath + ".participantHandle.open)"},
		{"handle_run_source", "fabric_participants.go", "exec := h.source.cloneWithStorage(h.storage)", inject("(*h.source).storage"), "participantHandle.source written through (CreateNode, in " + cypherPath + ".participantHandle.run)"},
		{"wrapper_read", "transaction_storage_wrapper.go", "func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {\n\t", "func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {\n\t" + inject("w.underlying"), "transactionStorageWrapper.underlying written through (CreateNode, in " + cypherPath + ".transactionStorageWrapper.GetNode)"},
		{"labelless_index", "transaction_storage_wrapper.go", "engine = wrapper.underlying", inject("wrapper.underlying"), "transactionStorageWrapper.underlying written through (CreateNode, in " + cypherPath + ".labellessPropertyIndexUsable)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := readWriteAccessSource(t, loader.cypher, tc.file)
			require.Equal(t, 1, strings.Count(src, tc.anchor), "anchor moved in %s: %q", tc.file, tc.anchor)
			replacement := tc.injected + tc.anchor
			if strings.HasPrefix(tc.injected, tc.anchor) {
				replacement = tc.injected
			}
			mutated := strings.Replace(src, tc.anchor, replacement, 1)
			pkg, errs := loader.check(loader.cypher, map[string]string{tc.file: mutated})
			require.Empty(t, errs, "the mutation type-checks")
			violations := writeAccessViolations(loader.typed(loader.fabric, nil), pkg, loader.parsed(loader.storage, nil))
			require.Contains(t, strings.Join(violations, "\n"), tc.want)
		})
	}
}

// readWriteAccessSource is file name of the package listed, with LF line ends.
func readWriteAccessSource(t *testing.T, listed *writeAccessListed, name string) string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(listed.Dir, name))
	require.NoError(t, err)
	return strings.ReplaceAll(string(src), "\r\n", "\n")
}
