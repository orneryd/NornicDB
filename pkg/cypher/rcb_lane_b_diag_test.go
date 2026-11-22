package cypher

// Root-cause diagnostics, v1.5.2 verification campaign, lane B (test-only; no
// product code is changed). Each test logs the decisive value of one step and
// a variant that bypasses or corrects that step in a test-side copy.
//
//   TestRCBCaseOperandSplitStep   G-arithmetic-case-operand (NOR-440)
//   TestRCBClosedStoreStatusStep  G-http-autocommit-runner (NOR-358)
//   TestRCBUseAfterImportingWith  G-use-with-import (NOR-85)

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// rcbSplitRowArithmeticTierCaseAware is a test-side copy of
// splitRowArithmeticTier (row_expression.go:544) with one change: a CASE …
// END is tracked like a bracket pair, so an operator inside it is never the
// expression's top-level operator.
func rcbSplitRowArithmeticTierCaseAware(expr, operators string) (left, right string, operator byte, ok bool) {
	parenDepth, bracketDepth, braceDepth, caseDepth := 0, 0, 0, 0
	isIdent := func(b byte) bool {
		return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}
	operatorIndex := -1
	for index := 0; index < len(expr); index++ {
		current := expr[index]
		if current == '/' {
			if end := queryCommentEnd(expr, index); end >= 0 {
				index = end - 1
				continue
			}
		}
		switch cypherLexicalByte(current) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expr, index, current) - 1
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			parenDepth--
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			bracketDepth--
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			braceDepth--
			continue
		}
		if isIdent(current) && (index == 0 || !isIdent(expr[index-1])) {
			end := index
			for end < len(expr) && isIdent(expr[end]) {
				end++
			}
			if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				switch strings.ToUpper(expr[index:end]) {
				case "CASE":
					caseDepth++
				case "END":
					if caseDepth > 0 {
						caseDepth--
					}
				}
			}
			index = end - 1
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || caseDepth != 0 || !strings.ContainsRune(operators, rune(current)) {
			continue
		}
		if (current == '+' || current == '-') && rowArithmeticSignIsUnary(expr, index) {
			continue
		}
		operatorIndex = index
		operator = current
	}
	if operatorIndex < 0 {
		return "", "", 0, false
	}
	return strings.TrimSpace(expr[:operatorIndex]), strings.TrimSpace(expr[operatorIndex+1:]), operator, true
}

func TestRCBCaseOperandSplitStep(t *testing.T) {
	cases := []struct{ expr, operators, wantLeft, wantRight string }{
		{"1 + CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END", "+-", "1", "CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END"},
		{"x + CASE WHEN x > 1 THEN -1 ELSE 0 END", "+-", "x", "CASE WHEN x > 1 THEN -1 ELSE 0 END"},
		{"1 + CASE WHEN true THEN 2 + CASE WHEN true THEN 1 ELSE 0 END ELSE 0 END", "+-", "1", "CASE WHEN true THEN 2 + CASE WHEN true THEN 1 ELSE 0 END ELSE 0 END"},
		{"2 * CASE WHEN true THEN 3 * 2 ELSE 1 END", "*/%", "2", "CASE WHEN true THEN 3 * 2 ELSE 1 END"},
		{"12 / CASE WHEN true THEN 6 / 3 ELSE 1 END", "*/%", "12", "CASE WHEN true THEN 6 / 3 ELSE 1 END"},
		{"2 ^ CASE WHEN true THEN 2 ^ 2 ELSE 1 END", "^", "2", "CASE WHEN true THEN 2 ^ 2 ELSE 1 END"},
		{"10 * CASE WHEN true THEN 5 - 2 ELSE 0 END", "+-", "", ""},
		{"10 * CASE WHEN true THEN 5 - 2 ELSE 0 END", "*/%", "10", "CASE WHEN true THEN 5 - 2 ELSE 0 END"},
		{"1 + CASE WHEN true THEN toInteger('2' + '3') ELSE 0 END", "+-", "1", "CASE WHEN true THEN toInteger('2' + '3') ELSE 0 END"},
		{"1 + (CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END)", "+-", "1", "(CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END)"},
		{"CASE WHEN true THEN 5 - 2 ELSE 0 END + 10", "+-", "CASE WHEN true THEN 5 - 2 ELSE 0 END", "10"},
	}
	productWrong, copyWrong := 0, 0
	for _, tc := range cases {
		pl, pr, pop, pok := splitRowArithmeticTier(tc.expr, tc.operators)
		cl, cr, cop, cok := rcbSplitRowArithmeticTierCaseAware(tc.expr, tc.operators)
		wantOK := tc.wantLeft != "" || tc.wantRight != ""
		productOK := pok == wantOK && (!wantOK || (pl == tc.wantLeft && pr == tc.wantRight))
		copyOK := cok == wantOK && (!wantOK || (cl == tc.wantLeft && cr == tc.wantRight))
		if !productOK {
			productWrong++
		}
		if !copyOK {
			copyWrong++
		}
		t.Logf("RCB440 SPLIT expr=%q tier=%q product=(%q %q %q ok=%v correct=%v) caseAwareCopy=(%q %q %q ok=%v correct=%v)",
			tc.expr, tc.operators, pl, string(pop), pr, pok, productOK, cl, string(cop), cr, cok, copyOK)
	}
	t.Logf("RCB440 SPLIT SUMMARY cases=%d product_wrong=%d case_aware_copy_wrong=%d", len(cases), productWrong, copyWrong)

	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, q := range []string{
		"RETURN 10 + CASE WHEN true THEN 5 - 2 ELSE 0 END AS r",
		"RETURN 10 * CASE WHEN true THEN 5 - 2 ELSE 0 END AS r",
		"RETURN 10 + (CASE WHEN true THEN 5 - 2 ELSE 0 END) AS r",
		"RETURN 10 * (CASE WHEN true THEN 5 - 2 ELSE 0 END) AS r",
		"RETURN CASE WHEN true THEN 5 - 2 ELSE 0 END AS r",
		"RETURN CASE WHEN true THEN 5 - 2 ELSE 0 END + 10 AS r",
	} {
		result, err := exec.Execute(t.Context(), q, nil)
		var rows interface{}
		if result != nil {
			rows = result.Rows
		}
		t.Logf("RCB440 EXEC %q rows=%v err=%v", q, rows, err)
	}
	if productWrong == 0 || copyWrong != 0 {
		t.Errorf("RCB440 expected the product splitter to be wrong and the CASE-aware copy right: product_wrong=%d copy_wrong=%d", productWrong, copyWrong)
	}
}

func rcbErrorChain(err error) string {
	var parts []string
	for e := err; e != nil; e = errors.Unwrap(e) {
		parts = append(parts, fmt.Sprintf("%T", e))
		if len(parts) > 12 {
			break
		}
	}
	return strings.Join(parts, " > ")
}

func TestRCBClosedStoreStatusStep(t *testing.T) {
	for _, bare := range []error{storage.ErrStorageClosed, storage.ErrStorageRestoring} {
		code, message := nornicerrors.Neo4jStatus(bare)
		t.Logf("RCB358 BARE err=%q code=%s message=%q ownStatus=%v", bare.Error(), code, message, nornicerrors.HasNeo4jStatus(bare))
	}
	base, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	if _, err := exec.Execute(t.Context(), "CREATE (:Doc {id:'a'})", nil); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"MATCH (n:Doc) RETURN count(n) AS c",
		"MATCH (n:Doc {id:'a'}) RETURN n.id AS id",
		"MATCH (n:Doc {id:'a'}) SET n.v=1 RETURN n.v AS v",
		"CALL db.labels() YIELD label RETURN label",
	} {
		_, err := exec.Execute(t.Context(), q, nil)
		code, message := nornicerrors.Neo4jStatus(err)
		corrected := code
		if !nornicerrors.HasNeo4jStatus(err) && errors.Is(err, storage.ErrStorageClosed) {
			corrected = nornicerrors.TransientDatabaseUnavailable
		}
		t.Logf("RCB358 EXEC %q isStorageClosed=%v ownStatus=%v code=%s message=%q chain=[%s] corrected_classifier_variant_code=%s",
			q, errors.Is(err, storage.ErrStorageClosed), nornicerrors.HasNeo4jStatus(err), code, message, rcbErrorChain(err), corrected)
	}
}

func TestRCBUseAfterImportingWith(t *testing.T) {
	ctx := t.Context()
	for _, body := range []string{
		"WITH v USE neo4j RETURN v AS x",
		"USE neo4j WITH v RETURN v AS x",
		"WITH v RETURN v AS x",
	} {
		qb := readQueryBody(ctx, body, 0, len(body))
		useStart, declined := -2, false
		var kinds []string
		if len(qb.branches) > 0 {
			branch := qb.branches[0]
			useStart, declined = branch.useStart, branch.declined
			for _, clause := range branch.clauses {
				kinds = append(kinds, fmt.Sprintf("%v:%q", clause.kind, clause.text))
			}
		}
		_, planned := pipelineClausesForBody(ctx, qb)
		use, remaining, hasUse, useErr := parseUseClause(ctx, body, true, cyphertext.AcceptedSource)
		remainingPlanned := false
		if hasUse {
			_, remainingPlanned = pipelineClausesFor(ctx, remaining)
		}
		t.Logf("RCB85 BODY %q status=%v branches=%d useStart=%d declined=%v clauses=%v bodyPipelinePlanned=%v leadingUSEParsed=%v use=%+v remaining=%q remainingPlanned=%v err=%v",
			body, qb.status, len(qb.branches), useStart, declined, kinds, planned, hasUse, use, remaining, remainingPlanned, useErr)
	}
	exec := newNOR85NativeSession(t)
	for _, q := range []string{
		nor85Reproduction,
		"WITH 1 AS v CALL { USE neo4j WITH v RETURN v AS x } RETURN x",
		"WITH 1 AS v CALL { WITH v RETURN v AS x } RETURN x",
	} {
		result, err := exec.Execute(ctx, q, nil)
		var rows interface{}
		if result != nil {
			rows = result.Rows
		}
		t.Logf("RCB85 EXEC %q rows=%v err=%v", q, rows, err)
	}
}
