package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxProcedureDemandAllocations(t *testing.T) {
	query := strings.Repeat("/* CALL fake.proc() ' */ ", 900) + "RETURN date('2020-01-01'), coalesce(null,1)"
	require.Greater(t, len(query), 16384)
	require.Empty(t, registeredProceduresForCalls(query))
	require.Zero(t, testing.AllocsPerRun(10, func() {
		if len(registeredProceduresForCalls(query)) != 0 {
			t.Fatal("comment became a procedure")
		}
	}))
}

func TestFunctionSyntaxProcedureDemand(t *testing.T) {
	query := "CALL `db` /* note */ . labels() YIELD label CALL (label) { CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey } RETURN 'CALL fake.proc()'"
	for _, prefix := range []string{"", "/* " + strings.Repeat("padding ", 2400) + " */ "} {
		text := prefix + query
		for _, retained := range []bool{false, true} {
			if retained {
				readFunctionSyntax(text)
			}
			var names []string
			err := lexicalProcedureInvocations(text, func(name, _ string, _ int) error {
				names = append(names, name)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, []string{"db.labels", "db.propertyKeys"}, names)
		}
	}
}
