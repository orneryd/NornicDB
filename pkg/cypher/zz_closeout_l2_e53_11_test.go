//go:build closeout_l2_e53_11

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	l2e5311antlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E53.11: CREATE CONSTRAINT ... REQUIRE { {x: 1} } (a map literal as a
// constraint-contract entry) is refused by both parsers but with different
// errors (ANTLR: SyntaxError; default: invalid predicate). The comparison
// needs both parsers in one process, so this test switches the parser itself
// (config.SetParserType) and restores the process's parser afterwards; under
// the lab's "--parser both" it runs the same comparison twice.

const (
	l2E53_11MapStatement     = "CREATE CONSTRAINT pfmap FOR (n:MapNode) REQUIRE { {x: 1} }"
	l2E53_11ControlStatement = "CREATE CONSTRAINT pfmap FOR (n:MapNode) REQUIRE { n.age > 0 }"
)

type l2E53_11Outcome struct {
	err     error
	code    string
	message string
}

func l2E53_11Run(t *testing.T, parser, statement string) l2E53_11Outcome {
	t.Helper()
	prior := config.GetParserType()
	config.SetParserType(parser)
	defer config.SetParserType(prior)
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, execErr := exec.Execute(context.Background(), statement, nil)
	out := l2E53_11Outcome{err: execErr}
	if execErr != nil {
		out.code, out.message = nornicerrors.Neo4jStatus(execErr)
	}
	return out
}

func TestL2_e53_11_Repro(t *testing.T) {
	nornic := l2E53_11Run(t, config.ParserTypeNornic, l2E53_11MapStatement)
	antlr := l2E53_11Run(t, config.ParserTypeANTLR, l2E53_11MapStatement)
	// "The same error" is decided on the Neo4j status code and the message
	// together: Neo4jStatus classifies an unrecognised error as SyntaxError,
	// so two different refusals can share a code; one refusal reached by both
	// parsers carries the same text.
	same := nornic.err != nil && antlr.err != nil && nornic.code == antlr.code && nornic.message == antlr.message
	verdict := "OK"
	if !same {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E53.11 %s process_parser=%s nornic=[%s %q] antlr=[%s %q]",
		verdict, config.GetParserType(), nornic.code, nornic.message, antlr.code, antlr.message)
	if nornic.err == nil || antlr.err == nil {
		t.Errorf("the map-literal requirement must be refused by both parsers: nornic err=%v antlr err=%v", nornic.err, antlr.err)
	}
	if nornic.err != nil && antlr.err != nil && (nornic.code != antlr.code || nornic.message != antlr.message) {
		t.Errorf("parsers disagree: nornic %s (%v) vs antlr %s (%v)", nornic.code, nornic.err, antlr.code, antlr.err)
	}
}

// Control: a predicate requirement is accepted by both parsers.
func TestL2_e53_11_Control(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		out := l2E53_11Run(t, parser, l2E53_11ControlStatement)
		t.Logf("control parser=%s err=%v", parser, out.err)
		if out.err != nil {
			t.Errorf("control parser=%s: %s refused: %v", parser, l2E53_11ControlStatement, out.err)
		}
	}
}

// Cause: where each parser's refusal comes from. The statement is handed to
// the shared typed preparation (prepareCreateConstraint,
// schema_constraint_preparation.go:22) directly, bypassing the executor's
// parser-specific front end; if preparation gives one code for both parsers
// while Execute differs, the difference is in the ANTLR front end that runs
// before it.
func TestL2_e53_11_Cause(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		func() {
			prior := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(prior)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
			_, prepErr := exec.prepareCreateConstraint(context.Background(), l2E53_11MapStatement)
			prepCode := ""
			if prepErr != nil {
				prepCode, _ = nornicerrors.Neo4jStatus(prepErr)
			}
			_, execErr := exec.Execute(context.Background(), l2E53_11MapStatement, nil)
			execCode := ""
			if execErr != nil {
				execCode, _ = nornicerrors.Neo4jStatus(execErr)
			}
			t.Logf("cause parser=%s prepare=[%s %v] execute=[%s %v]", parser, prepCode, prepErr, execCode, execErr)
		}()
	}
}

// Cause2: the two refusals come from two layers, each called directly here on
// the same text, without the executor.
//   - ANTLR grammar (only the ANTLR validator runs it, executor_query_routing.go:510-516):
//     constraintRequirement's constraintBlock alternative (CypherParser.g4:195,
//     block = balanced braces :211-213) takes the map literal {x: 1} as a
//     nested contract block, whose entry `x: 1` is then read as a
//     constraintBlockEntry (:219-221) and fails; readContractBlocks reports it
//     (pkg/cypher/antlr/contract_block.go:69, "line L:C unexpected input").
//   - Storage contract compiler (both parsers reach it when ANTLR is not run):
//     compileNodeConstraintContractExpression has no map-literal form and
//     returns "unsupported node predicate" (pkg/storage/constraint_contracts.go:526),
//     wrapped by ValidateConstraintContractExpressions (:586-598) as
//     'constraint contract pfmap invalid: predicate "{x: 1}": ...'.
// The control text { n.age > 0 } passes both.
func TestL2_e53_11_Cause2(t *testing.T) {
	for _, c := range []struct {
		name, statement, expression string
	}{
		{"map-literal", l2E53_11MapStatement, "{x: 1}"},
		{"control", l2E53_11ControlStatement, "n.age > 0"},
	} {
		validateErr := l2e5311antlr.Validate(c.statement)
		parsed, parseErr := l2e5311antlr.Parse(c.statement)
		var parseDiagnostics []string
		if parsed != nil {
			parseDiagnostics = parsed.Errors
		}
		storageErr := storage.ValidateConstraintContractExpressions(storage.ConstraintContract{
			Name:              "pfmap",
			TargetEntityType:  "node",
			TargetLabelOrType: "MapNode",
			Entries:           []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindBooleanNode, Expression: c.expression}},
		})
		t.Logf("L2CAUSE E53.11 %s antlr.Validate=%v antlr.Parse=(err %v, diagnostics %q) storage.ValidateConstraintContractExpressions(%q)=%v",
			c.name, validateErr, parseErr, parseDiagnostics, c.expression, storageErr)
	}
}
