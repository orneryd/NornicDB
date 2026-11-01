package antlr

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatementEffectsRetainGrammarScope(t *testing.T) {
	for _, tc := range []struct {
		source       string
		writes, read bool
		calls        []string
	}{
		{"RETURN 'CREATE CALL remote.unknown()' AS value", false, true, nil},
		{"MATCH(n) SET/*gap*/n.x=1 RETURN n", true, false, nil},
		{"EXPLAIN CALL unknown.remote()", false, true, []string{"unknown.remote"}},
		{"CALL { RETURN 1 AS x } RETURN x", false, true, nil},
		{"CALL { CREATE (n) RETURN n } RETURN n", true, false, nil},
		{"CALL `remote`.`odd``name`()", false, false, []string{"remote.odd`name"}},
		{"EXPLAIN CREATE (n); MATCH(n) SET n.x=1 RETURN n", true, false, nil},
	} {
		t.Run(tc.source, func(t *testing.T) {
			for _, dialect := range []bool{false, true} {
				syntax, err := ParseSyntax(t.Context(), tc.source, StatementEntry, dialect, RawSource)
				require.NoError(t, err)
				effects := syntax.Effects()
				require.Equal(t, tc.writes, effects.Writes())
				require.Equal(t, tc.read, effects.RemoteReadOnly())
				require.Equal(t, len(tc.calls), effects.CallCount())
				for i, name := range tc.calls {
					require.Equal(t, name, effects.Call(i).Name())
					require.NotEmpty(t, effects.Call(i).Text())
				}
				cached, err := ParseSyntax(t.Context(), tc.source, StatementEntry, dialect, RawSource)
				require.NoError(t, err)
				require.Same(t, syntax.data, cached.data, "effects reuse the existing dialect/entry syntax cache")
			}
		})
	}
}

func TestReadOnlyEffectsUseLiveCallbacksAndRetainedScope(t *testing.T) {
	syntax, err := ParseSyntax(t.Context(), "EXPLAIN CALL ignored.opaque(); CALL retained.read()", StatementEntry, false, RawSource)
	require.NoError(t, err)
	effects := syntax.Effects()
	for _, allow := range []bool{true, false, true} {
		calls := 0
		require.Equal(t, allow, effects.ReadOnly(func(call StatementCall) bool { calls++; require.Equal(t, "retained.read", call.Name()); return allow }))
		require.Equal(t, 1, calls, "only executable CALL invokes live policy")
	}
	var scopes []string
	require.True(t, effects.AllowsExecutableQueries(func(source string) bool { scopes = append(scopes, source); return true }))
	require.Equal(t, []string{"CALL retained.read()"}, scopes)
	require.False(t, effects.RemoteReadOnly(), "local callback must never alter remote conservative proof")
}

func TestStatementEffectsRequireCompleteQueryBodies(t *testing.T) {
	for _, source := range []string{"", "/* no query */", ";", "EXPLAIN", "PROFILE", "CYPHER 25", "RETURN 1; EXPLAIN", "RETURN 1; PROFILE", "RETURN 1; CYPHER 25"} {
		t.Run(source, func(t *testing.T) {
			for _, dialect := range []bool{false, true} {
				for _, entry := range []SyntaxEntry{StatementEntry, FragmentEntry} {
					syntax, err := ParseSyntax(t.Context(), source, entry, dialect, RawSource)
					if err != nil {
						continue // A syntax rejection is already a refusal, not a proof of read-only effects.
					}
					effects := syntax.Effects()
					require.False(t, effects.ReadOnly(func(StatementCall) bool { return true }), source)
					require.False(t, effects.RemoteReadOnly(), source)
					visits := 0
					require.False(t, effects.AllowsExecutableQueries(func(string) bool { visits++; return true }), source)
					require.Zero(t, visits, "no partial script may reach live admission policy")
				}
			}
		})
	}
}
