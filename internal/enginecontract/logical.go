package enginecontract

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// PublicObligation is executable: Observe invokes the real consumer and Check
// judges its product result. The expected disposition is independent fixture
// data. Registering a test name, or merely comparing owner tokens, is insufficient.
type PublicObligation struct {
	Disposition storage.BindingDisposition
	Consumers   map[string]func(*testing.T)
}

// LogicalConsumers is the finite consumer grammar for a boundary declaration.
// Each new logical field must acquire a consumer here; each new Engine needs
// its own complete adapter. Source-population gates independently check issuers
// and consumer sites, including calls through helpers and interface values.
var LogicalConsumers = map[string][]string{
	"ReadState":            {"rows", "visualization"},
	"Membership":           {"membership", "periodic"},
	"Controller":           {"control"},
	"PendingActions":       {"actions"},
	"SchemaPublication":    {"schema"},
	"ReadLease":            {"lease"},
	"StatementMode":        {"mode"},
	"Identity":             {"identity"},
	"Enforcement":          {"limits"},
	"CompoundDependencies": {"rows"},
	"SameVersionFallback":  {"membership"},
	"Delivery":             {"rows"},
	"ExactEffects":         {"mutation"},
	"FullRead":             {"visualization"},
	"ObservedNode":         {"observed-node"},
	"ObservedEdge":         {"observed-edge"},
}

type PublicProof struct {
	engine    string
	consumers int
}

func RequirePublicProof(t *testing.T, name string, proof *PublicProof) {
	t.Helper()
	require.NotNil(t, proof, "adapter did not execute its public obligations")
	require.Equal(t, name, proof.engine)
	require.NotZero(t, proof.consumers)
}

func CheckPublicObligations(t *testing.T, engine storage.Engine, adapters map[string]PublicObligation) *PublicProof {
	t.Helper()
	declaration, ok := engine.(storage.EngineBoundaryDeclaration)
	require.True(t, ok)
	policy := declaration.EngineCapabilityPolicy()
	require.Empty(t, PolicyProblems(policy))
	value := reflect.ValueOf(policy)
	used := map[string]bool{}
	for i := 1; i < value.NumField(); i++ {
		field := value.Type().Field(i).Name
		required, known := LogicalConsumers[field]
		require.True(t, known, "new field %s has no public consumer obligations", field)
		adapter, ok := adapters[field]
		require.True(t, ok, "missing executable obligation for %s", field)
		require.Equal(t, storage.BindingDisposition(value.Field(i).Uint()), adapter.Disposition, "%s: fixture disposition disagrees with declaration", field)
		require.Len(t, adapter.Consumers, len(required), "%s: missing/stale consumers", field)
		for _, consumer := range required {
			run := adapter.Consumers[consumer]
			require.NotNil(t, run, "%s/%s needs executable adapter", field, consumer)
			if !used[consumer] {
				t.Run(consumer, run)
				used[consumer] = true
			}
		}
	}
	require.Len(t, adapters, value.NumField()-1, "stale obligation fields")
	typ := reflect.TypeOf(engine)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return &PublicProof{engine: typ.PkgPath() + "." + typ.Name(), consumers: len(used)}
}
