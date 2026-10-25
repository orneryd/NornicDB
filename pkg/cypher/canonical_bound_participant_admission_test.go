package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

// Author: #1640 lane. Participant-owned CALL admission is required even when
// the authored YIELD makes its header spellings syntactically available.
func TestCanonicalBoundParticipantAdmission(t *testing.T) {
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, tail := range []string{"", " YIELD value", " YIELD value AS kept"} {
				t.Run(mode+input+tail, func(t *testing.T) {
					exec, _ := newUnitExecutor(t)
					query := mode + "USE comp.remote " + input + "CALL custom.participantOnly(1)" + tail
					failure := errors.New("participant admission rejected this invocation")
					calls := 0
					facts := exec.StatementFactsWith(query, resultmetadata.KnownScope(nil), false, func(obligation resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
						calls++
						require.Equal(t, "custom.participantOnly", obligation.Procedure)
						require.Equal(t, "CALL custom.participantOnly(1)"+tail, query[obligation.Start:obligation.End])
						return ParticipantProcedure{}, failure
					}, RawSource)
					facts.Visit(func(resultmetadata.Statement) { t.Fatal("unadmitted participant invocation published facts") }, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { require.ErrorIs(t, reason.Err, failure) })
					require.Equal(t, 1, calls)
				})
			}
		}
	}
}

func TestCanonicalBoundParticipantDeclaredProjection(t *testing.T) {
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, projection := range []struct{ tail, column string }{{"", "value"}, {" YIELD value", "value"}, {" YIELD value AS kept", "kept"}} {
				t.Run(mode+input+projection.tail, func(t *testing.T) {
					exec, _ := newUnitExecutor(t)
					query := mode + "USE comp.remote " + input + "CALL custom.participantOnly(1)" + projection.tail
					calls := 0
					facts := exec.StatementFactsWith(query, resultmetadata.KnownScope(nil), false, func(obligation resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
						calls++
						require.Equal(t, "CALL custom.participantOnly(1)"+projection.tail, query[obligation.Start:obligation.End])
						return ParticipantProcedure{Spec: &ProcedureSpec{MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "argument", Type: "INTEGER"}}, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}}}, nil
					}, RawSource)
					requiresYield := input != "" && projection.tail == ""
					facts.Visit(func(statement resultmetadata.Statement) {
						require.False(t, requiresYield, "a non-standalone value procedure requires YIELD")
						statement.Columns.Visit(func(columns []string) { require.Equal(t, []string{projection.column}, columns) }, func() { t.Fatal("unknown admitted header") })
					}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) {
						require.True(t, requiresYield, "%v", reason.Err)
						require.ErrorContains(t, reason.Err, "YIELD")
					})
					require.Equal(t, 1, calls)
				})
			}
		}
	}
}

func TestCanonicalBoundParticipantSignatureAdmission(t *testing.T) {
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, sample := range []struct {
				name, call, detail string
				missing            bool
			}{
				{"valid", "CALL db.labels(7) YIELD other AS kept RETURN kept", "", false},
				{"arity", "CALL db.labels() YIELD other AS kept RETURN kept", "InvalidNumberOfArguments", false},
				{"yield", "CALL db.labels(7) YIELD label RETURN label", "", false},
				{"unresolved", "CALL db.labels(7) YIELD other AS kept RETURN kept", "", true},
			} {
				t.Run(mode+input+sample.name, func(t *testing.T) {
					exec, _ := newUnitExecutor(t)
					calls := 0
					facts := exec.StatementFactsWith(mode+"USE comp.remote "+input+sample.call, resultmetadata.KnownScope(nil), false, func(resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
						calls++
						if sample.missing {
							return ParticipantProcedure{}, nil
						}
						// This participant's declaration deliberately differs from
						// the same-named local built-in in both inputs and outputs.
						return ParticipantProcedure{Spec: &ProcedureSpec{MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "argument", Type: "INTEGER"}}, Returns: []ProcedureColumn{{Name: "other", Type: "INTEGER"}}}}, nil
					}, RawSource)
					facts.Visit(func(statement resultmetadata.Statement) {
						require.Equal(t, "valid", sample.name)
						statement.Columns.Visit(func(columns []string) { require.Equal(t, []string{"kept"}, columns) }, func() { t.Fatal("unknown header") })
					}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) {
						require.NotEqual(t, "valid", sample.name)
						require.Error(t, reason.Err)
						if sample.detail != "" {
							code, detail, _ := boundRuleError(reason.Err)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
							require.Equal(t, sample.detail, detail)
						}
					})
					require.Equal(t, 1, calls)
				})
			}
		}
	}
}

func TestCanonicalBoundParticipantFrozenTypes(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	query := "USE comp.remote CALL db.labels(7) YIELD other AS kept RETURN kept + 1 AS answer"
	spec := ProcedureSpec{MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "argument", Type: "INTEGER"}}, Returns: []ProcedureColumn{{Name: "other", Type: "INTEGER"}}}
	program := exec.statementProjectionProgram(ctx, query, resultmetadata.KnownScope(nil), false, func(resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
		return ParticipantProcedure{Spec: &spec}, nil
	})
	require.True(t, program.complete, "%v", program.err)
	// A provider retaining and changing its metadata must not change the
	// admitted declaration or make typing fall back to the local db.labels.
	spec.Params[0].Type = "STRING"
	spec.Returns[0].Name = "changed"
	_, _, err := exec.enrichBoundTypes(withBoundStatementProgram(ctx, program.bound), query, nil, nil, "", false)
	require.NoError(t, err)
	for _, clause := range program.bound.clauses {
		if clause.source.kind == pipelineClauseCall {
			clause.procedureSpec = nil
		}
	}
	_, _, err = exec.enrichBoundTypes(withBoundStatementProgram(ctx, program.bound), query, nil, nil, "", false)
	require.Error(t, err, "a missing admitted declaration must fail closed")
}

func TestCanonicalBoundParticipantRoutingIdentity(t *testing.T) {
	exec, _ := newUnitExecutor(t)
	query := "WITH 'comp.one' AS target USE graph.byName(target) WITH 'comp.two' AS target CALL custom.participantOnly(target) YIELD value RETURN value"
	calls := 0
	exec.StatementFactsWith(query, resultmetadata.KnownScope(nil), false, func(obligation resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
		calls++
		routing, routingKnown := obligation.Routing.Binding("target")
		current, currentKnown := obligation.Incoming.Binding("target")
		require.True(t, routingKnown)
		require.True(t, currentKnown)
		require.NotEqual(t, routing, current, "the USE selector retains its entry declaration across rebinding")
		require.Equal(t, "graph.byName", obligation.Selection.Function)
		require.Equal(t, []string{"target"}, obligation.Selection.Arguments)
		return ParticipantProcedure{Spec: &ProcedureSpec{MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "argument", Type: "ANY"}}, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}}}, nil
	}, RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
	require.Equal(t, 1, calls)
}
