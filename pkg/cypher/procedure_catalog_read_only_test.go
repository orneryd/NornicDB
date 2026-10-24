package cypher

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// An executor built over a composite database, or over a composite
// statement's participant storage, reads the procedure catalog without
// writing it: a legacy lowercase record is registered as it is, not
// migrated; one whose exact-name copy exists is skipped. A catalog read
// error is the load's.
func TestProcedureCatalogLoadsReadOnlyOverACompositeDatabase(t *testing.T) {
	defer ClearUserProcedures()
	base := newTestMemoryEngine(t)
	one := storage.NewNamespacedEngine(base, "one")
	record := persistedProcedureRecord{Name: "custom.ReadOnlyLegacy", Mode: "READ", Body: "RETURN 7 AS n", Signature: "custom.ReadOnlyLegacy() :: (n :: INTEGER)"}
	blob, err := msgpack.Marshal(record)
	require.NoError(t, err)
	legacy := storage.NodeID(procedureCatalogPrefix + "custom.readonlylegacy")
	_, err = one.CreateNode(&storage.Node{ID: legacy, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(blob)}})
	require.NoError(t, err)
	root := storage.NewCompositeEngine(map[string]storage.Engine{"one": one}, map[string]string{"one": "one"}, map[string]string{"one": "read_write"})
	root.SetCompositeName("comp")

	ClearUserProcedures()
	exec := NewStorageExecutor(root)
	_, err = one.GetNode(legacy)
	require.NoError(t, err, "the legacy record is not migrated")
	_, err = one.GetNode(procedureCatalogNodeID(record.Name))
	require.ErrorIs(t, err, storage.ErrNotFound, "nor copied to its exact name")
	_, found := lookupUserProcedureForTest(record.Name)
	require.True(t, found, "the legacy record is registered as it is")

	// With its exact-name copy present, the legacy record is skipped.
	exact := record
	exact.Body = "RETURN 8 AS n"
	exactBlob, err := msgpack.Marshal(exact)
	require.NoError(t, err)
	_, err = one.CreateNode(&storage.Node{ID: procedureCatalogNodeID(record.Name), Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(exactBlob)}})
	require.NoError(t, err)
	superseded, err := procedureCatalogSuperseded(one, must(one.GetNode(legacy)), record)
	require.NoError(t, err)
	require.True(t, superseded)
	require.NoError(t, exec.loadPersistedProceduresFrom(root, false))
	_, err = one.GetNode(legacy)
	require.NoError(t, err, "still nothing written")

	// The exact-name record itself is never superseded.
	superseded, err = procedureCatalogSuperseded(one, must(one.GetNode(procedureCatalogNodeID(record.Name))), record)
	require.NoError(t, err)
	require.False(t, superseded)

	// A read error of the exact-name lookup is the load's error.
	unavailable := errors.New("catalog read unavailable")
	_, err = procedureCatalogSuperseded(&procedureIdentityFaultEngine{Engine: one, readErr: unavailable}, must(one.GetNode(legacy)), record)
	require.ErrorIs(t, err, unavailable)
	require.ErrorIs(t, exec.loadPersistedProceduresFrom(&procedureIdentityFaultEngine{Engine: one, readErr: unavailable}, false), unavailable)
}

// lookupUserProcedureForTest reports whether name is a registered
// procedure.
func lookupUserProcedureForTest(name string) (ProcedureSpec, bool) {
	for _, spec := range ListRegisteredProcedures() {
		if spec.Name == name {
			return spec, true
		}
	}
	return ProcedureSpec{}, false
}
