package cypher

import (
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// constituentCredentialProof observes the actual manager factory boundary and
// authoritative catalog, rather than treating public display fields as storage.
type constituentCredentialProof struct {
	t       *testing.T
	mu      sync.Mutex
	runtime map[string]multidb.ConstituentRef
}

func newConstituentCredentialProof(t *testing.T) *constituentCredentialProof {
	return &constituentCredentialProof{t: t, runtime: make(map[string]multidb.ConstituentRef)}
}

func (proof *constituentCredentialProof) factory(ref multidb.ConstituentRef, _ string) (storage.Engine, error) {
	proof.mu.Lock()
	proof.runtime[ref.Alias] = ref
	proof.mu.Unlock()
	return newTestMemoryEngine(proof.t), nil
}

func (proof *constituentCredentialProof) assertConnection(manager *multidb.DatabaseManager, composite string, expected multidb.ConstituentRef) {
	t := proof.t
	t.Helper()
	refs, err := manager.GetCompositeConstituents(composite)
	require.NoError(t, err)
	var stored *multidb.ConstituentRef
	for i := range refs {
		if refs[i].Alias == expected.Alias {
			stored = &refs[i]
			break
		}
	}
	require.NotNil(t, stored)
	require.NotEmpty(t, stored.ConnectionV1)
	require.Empty(t, stored.URI)
	require.Empty(t, stored.AuthMode)
	require.Empty(t, stored.User)
	require.Empty(t, stored.Password)
	system, err := manager.GetStorage("system")
	require.NoError(t, err)
	node, err := system.GetNode("databases:metadata")
	require.NoError(t, err)
	data, ok := node.Properties["data"].(string)
	require.True(t, ok)
	var disk map[string]struct {
		Constituents []multidb.ConstituentRef `json:"constituents"`
	}
	require.NoError(t, jsontext.Unmarshal([]byte(data), &disk))
	var persisted *multidb.ConstituentRef
	for i := range disk[composite].Constituents {
		if disk[composite].Constituents[i].Alias == expected.Alias {
			value := disk[composite].Constituents[i]
			persisted = &value
			break
		}
	}
	require.NotNil(t, persisted)
	require.Equal(t, *stored, *persisted)
	require.NotContains(t, data, expected.Password)
	require.NotContains(t, data, expected.User)
	display, err := manager.RemoteConstituentForDisplay(*stored)
	require.NoError(t, err)
	require.Equal(t, expected.URI, display.URI)
	require.Equal(t, expected.User, display.User)
	require.Equal(t, expected.AuthMode, display.AuthMode)
	require.Empty(t, display.Password)
	require.Empty(t, display.ConnectionV1)
	_, err = manager.GetStorageWithAuth(composite+"."+expected.Alias, "Bearer fixture-control")
	require.NoError(t, err)
	proof.mu.Lock()
	actual, present := proof.runtime[expected.Alias]
	proof.mu.Unlock()
	require.True(t, present, "actual manager factory must receive the selected connection")
	require.Equal(t, expected, actual)
}
