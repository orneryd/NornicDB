package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

type typedAccessTestManager struct{ *accessTestManager }

func (m *typedAccessTestManager) GetCompositeConstituents(name string) ([]multidb.ConstituentRef, error) {
	items, err := m.accessTestManager.GetCompositeConstituents(name)
	if err != nil {
		return nil, err
	}
	var refs []multidb.ConstituentRef
	for _, item := range items {
		if ref, ok := toConstituentRef(item); ok {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

func TestTypedGraphSelectionMatchesLegacyAdapter(t *testing.T) {
	for _, lookupErr := range []error{nil, errors.New("unavailable")} {
		legacy := &accessTestManager{constituentsErr: lookupErr}
		typed := &typedAccessTestManager{legacy}
		for _, selection := range []string{"cmp.a", "cmp.b", "cmp.missing", "cmp", "plain", "al", "nosuch"} {
			want, wantErr := AccessDatabases(legacy, selection)
			got, gotErr := AccessDatabases(typed, selection)
			require.Equal(t, want, got, selection)
			require.Equal(t, wantErr, gotErr, selection)
			for _, refused := range []string{"", "cmp", "dba", "dbb", "al", "target", "plain"} {
				allows := func(database string) bool { return database != refused }
				wantTarget, wantAllowed := GraphAccess(legacy, selection, allows)
				gotTarget, remote, gotAllowed := GraphAccessTarget(typed, selection, allows)
				require.False(t, remote)
				require.Equal(t, wantTarget, gotTarget, selection)
				require.Equal(t, wantAllowed, gotAllowed, selection)
			}
		}
	}
}
