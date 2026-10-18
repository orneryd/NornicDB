package cypher

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepassCanonicalReadersParity(t *testing.T) {
	for _, text := range []string{
		"", "MATCH (ñRETURN:İ) RETURN 'abc', \"d\", 1, 1.5, 1e-3, 0x1f, $longName",
		"RETURN 'a''b', 'a\\u0041', \"a\\\"b\", `a``1`, $(['A']), 0x, 00, 1e, 1e+",
		"RETURN 1 // comment\r\n /* a */ `name`", "'unfinished", "\"unfinished", "`unfinished",
		"RETURN 9223372036854775807, 9223372036854775808, 0x8000000000000000",
		"\u2003\t RETURN 1 \r\n\u00a0", "RETURN 0x" + strings.Repeat("0", 1000) + "f",
		"RETURN 1." + strings.Repeat("0", 2000) + "1e2",
	} {
		probe := &pipelineCancellation{ctx: context.Background()}
		require.Equal(t, upperASCII(text), upperASCIIWithCancellation(text, probe))
		want, changed := normalizeLiteralText(text)
		got, gotChanged := normalizeLiteralTextWithCancellation(text, probe)
		require.Equal(t, want, got)
		require.Equal(t, changed, gotChanged)
		require.Equal(t, strings.TrimSpace(text), trimSpaceWithCancellation(text, probe))
		require.Equal(t, strings.TrimSpace(text), trimSpaceWithCancellation(text, nil))
		require.NoError(t, probe.err)
	}
}

func TestPrepassLongNumericRangeParity(t *testing.T) {
	values := []string{"", "0x", "0x0", "0x" + strings.Repeat("0", 2000) + "7fffffffffffffff", "0x8000000000000000"}
	boundary := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 1024), new(big.Int).Lsh(big.NewInt(1), 970))
	for _, offset := range []int64{-1, 0, 1} {
		number := new(big.Int).Add(boundary, big.NewInt(offset)).String()
		for _, zeros := range []int{1000, 10000} {
			values = append(values, number+strings.Repeat("0", zeros)+"e-"+strconv.Itoa(zeros), number+"."+strings.Repeat("0", zeros)+"1")
		}
		coefficient := number + strings.Repeat("0", 10000)
		for _, point := range []int{1, 309, 799, 800, 801, 4096, 5000} {
			values = append(values, coefficient[:point]+"."+coefficient[point:]+"e"+strconv.Itoa(309-point))
		}
		values = append(values, "0."+strings.Repeat("0", 10000)+number+"e10309")
	}
	for _, prefix := range []string{"1", "17976931348623157", "17976931348623158", "2", "9"} {
		for _, exponent := range []string{"-999999999999999999999", "-1000", "-309", "0", "308", "309", "1000", "9999999999999999999"} {
			values = append(values, prefix+"."+strings.Repeat("0", 2048)+"1e"+exponent)
		}
	}
	random := rand.New(rand.NewSource(571))
	for index := 0; index < 200; index++ {
		var digits strings.Builder
		for count := 0; count < 800+random.Intn(9000); count++ {
			digits.WriteByte(byte('0' + random.Intn(10)))
		}
		values = append(values, "1."+digits.String()+"e"+strconv.Itoa(random.Intn(2200)-1100))
	}
	for _, number := range values {
		want := false
		switch {
		case strings.HasPrefix(number, "0x"):
			_, err := strconv.ParseInt(number[2:], 16, 64)
			want = err == nil
		case len(number) > 1 && number[0] == '0':
			// The existing cache contract deliberately keeps every leading-zero
			// spelling verbatim, even a valid fractional form.
			want = false
		case number != "":
			_, err := strconv.ParseFloat(number, 64)
			want = err == nil
		}
		probe := &pipelineCancellation{ctx: context.Background()}
		require.Equal(t, want, normNumberSafeWithCancellation(number, probe), "number prefix %q suffix %q len %d", number[:min(len(number), 40)], number[max(0, len(number)-45):], len(number))
		require.NoError(t, probe.err)
	}
}

func TestPrepassCanonicalReadersStopInsideWork(t *testing.T) {
	long := strings.Repeat("x", 1<<20)
	readers := map[string]func(*pipelineCancellation){
		"upper prefix": func(p *pipelineCancellation) {
			require.Empty(t, upperASCIIWithCancellation(strings.Repeat("X", 1<<20)+"a", p))
		},
		"upper conversion": func(p *pipelineCancellation) { require.Empty(t, upperASCIIWithCancellation(long, p)) },
		"trim leading": func(p *pipelineCancellation) {
			require.Empty(t, trimSpaceWithCancellation(strings.Repeat("\u2003", 1<<18)+"x", p))
		},
		"trim trailing": func(p *pipelineCancellation) {
			require.Empty(t, trimSpaceWithCancellation("x"+strings.Repeat("\u2003", 1<<18), p))
		},
		"comment":      func(p *pipelineCancellation) { queryCommentEndWithCancellation("/*"+long+"*/", 0, p) },
		"line comment": func(p *pipelineCancellation) { queryCommentEndWithCancellation("//"+long, 0, p) },
		"quoted": func(p *pipelineCancellation) {
			_, closed := scanCypherQuotedTextWithCancellation("'"+long+"'", 0, '\'', p)
			require.False(t, closed)
		},
		"delimiter": func(p *pipelineCancellation) {
			require.Equal(t, -1, findMatchingDelimiterWithCancellation("("+long+")", 0, '(', ')', p))
		},
		"float range": func(p *pipelineCancellation) {
			require.False(t, floatLiteralInRangeWithCancellation("1."+strings.Repeat("0", 1<<20)+"e1", p))
		},
		"hex range": func(p *pipelineCancellation) {
			require.False(t, normNumberSafeWithCancellation("0x"+strings.Repeat("0", 1<<20)+"f", p))
		},
	}
	for _, text := range []string{long, "'" + long + "'", "\"" + long + "\"", "`" + long + "`", "/*" + long + "*/", "//" + long, "$" + long, "$(" + long + ")", "0x" + strings.Repeat("f", 1<<20), strings.Repeat("1", 1<<20), "1." + strings.Repeat("1", 1<<20), "1e" + strings.Repeat("1", 1<<20)} {
		text := text
		readers["literal "+text[:min(len(text), 8)]] = func(p *pipelineCancellation) {
			canonical, changed := normalizeLiteralTextWithCancellation(text, p)
			require.Empty(t, canonical)
			require.False(t, changed)
		}
	}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
			probe := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
			read(probe)
			require.ErrorIs(t, probe.err, context.Canceled)
			require.ErrorIs(t, getExpressionFailure(probe.ctx), context.Canceled)
			require.Equal(t, 2, ctx.calls)
		})
	}
}

func TestPrepassLiteralFlushSharesProbe(t *testing.T) {
	// The first check in flush's byte copy must consume this same budget.
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
	probe := &pipelineCancellation{ctx: ctx, remaining: 97}
	text := strings.Repeat("x", 95) + "'a'"
	canonical, changed := normalizeLiteralTextWithCancellation(text, probe)
	require.ErrorIs(t, probe.err, context.Canceled)
	require.Empty(t, canonical)
	require.False(t, changed)
}

var prepassFixtureQueries = []string{
	"", "RETURN 1", "RETURN 1.5, .25, 1e-5", "RETURN 0x7f, 0O17, -9223372036854775808",
	"RETURN 1e999", "RETURN 1e+", "RETURN 5.", "RETURN 1bad", "RETURN 0xg", "RETURN 0x", "RETURN 12#", "RETURN 01", "RETURN 9223372036854775808",
	"RETURN 1.2bad", "RETURN 1.2#", "RETURN 0x8000000000000000", "RETURN 0o77777777777777777777777777777777777777777777",
	"RETURN 'a\\u0041', \"x\", `a``b` // -> 1bad\n/* <expr> */", "RETURN '\\uD800'", "RETURN '\\UFFFFFFFF'", "RETURN '\\uZZZZ'",
	"RETURN ñ, éa, a€, ‿x", "RETURN a—b", "RETURN €a", "RETURN a·", "RETURN \xff", "RETURN 'a—b' /* — */ // —\n",
	"MATCH (n {bad}) RETURN n", "MATCH (n {good: 1}) RETURN n", "RETURN {bad key: 1}", "RETURN {good: 1, `a``b`: 2, 'str': 3}",
	"RETURN {`bad`extra: 1}", "RETURN n {.key, .`a b`, .*}", "RETURN {a: EXISTS { MATCH (n) RETURN n }}",
	"MATCH (n:Count {bad}) RETURN n", "MATCH (count {bad}) RETURN count", "CALL (a, b) { RETURN {ok: 1} AS x } RETURN x",
	"MATCH ((a)-->(b)){1,6} RETURN a", "RETURN count { MATCH (n) RETURN n }", "RETURN {call: 1}", "RETURN {bad}",
	"CREATE INDEX x FOR (n:N) ON (n.k) OPTIONS {indexConfig: {`a b`: true}}", "CREATE CONSTRAINT x FOR (n:N) REQUIRE {n.x: 1}",
	"ALTER DATABASE x SET LIMIT 01", "RETURN a -> 1", "MATCH (a)-->(b) RETURN a", "RETURN {key: <expr>}", "RETURN x <> y, x < y",
	"RETURN 'unfinished", "RETURN /* unfinished", "RETURN // tail", "RETURN `a\\`b`",
	"RETURN CASE WHEN 1 = 1 THEN {a: 1} ELSE {a: 2} END AS x", "RETURN 'a\\n\\r\\t\\b\\f\\\\\\\"'",
}

func TestPrepassStaticValidatorParity(t *testing.T) {
	validators := []struct {
		plain   func(string) error
		checked func(string, *pipelineCancellation) error
	}{
		{validateUnicodeOperators, validateUnicodeOperatorsWithCancellation},
		{validateUnicodeStringLiterals, validateUnicodeStringLiteralsWithCancellation},
		{validateStaticMapKeys, validateStaticMapKeysWithCancellation},
		{validateNumericLiterals, validateNumericLiteralsWithCancellation},
		{validateArrowOperators, validateArrowOperatorsWithCancellation},
		{validatePlaceholderOperands, validatePlaceholderOperandsWithCancellation},
	}
	for index, validator := range validators {
		for _, query := range prepassFixtureQueries {
			want := validator.plain(query)
			probe := &pipelineCancellation{ctx: context.Background()}
			got := validator.checked(query, probe)
			require.Equal(t, fmt.Sprint(want), fmt.Sprint(got), "validator %d, query %q", index, query)
			require.NoError(t, probe.err)
		}
	}
}

func TestPrepassScannerCancellationReplay(t *testing.T) {
	for fixture, query := range prepassFixtureQueries {
		actions := []func(*pipelineCancellation){
			func(p *pipelineCancellation) { _ = validateUnicodeOperatorsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = validateUnicodeStringLiteralsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = validateStaticMapKeysWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = validateNumericLiteralsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = validateArrowOperatorsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = validatePlaceholderOperandsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _, _ = canonicalizeNumericLiteralsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _, _ = decodeCypherQuotedStringWithCancellation(query, p) },
			func(p *pipelineCancellation) { _, _ = normalizeLiteralTextWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = lowerASCIIWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = indexASCIIFoldWithCancellation(query, "return", p) },
			func(p *pipelineCancellation) { _, _, _ = scanIdentifierTokenWithCancellation(query, 0, p) },
			func(p *pipelineCancellation) {
				_ = keywordIndexFromWithCancellation(query, "RETURN", 0, defaultKeywordScanOpts(), p)
			},
			func(p *pipelineCancellation) {
				opts := defaultKeywordScanOpts()
				opts.SkipBraces = true
				_ = keywordIndexFromWithCancellation(query, "ORDER BY", 0, opts, p)
			},
			func(p *pipelineCancellation) {
				_ = findAllTopLevelPipelineKeywordPositionsWithCancellation(query, "WITH", p)
			},
		}
		for action, run := range actions {
			ctx := &pipelineCountingContext{Context: context.Background()}
			probe := &pipelineCancellation{ctx: ctx}
			run(probe)
			require.NoError(t, probe.err)
			if ctx.calls == 0 {
				continue
			}
			work := (ctx.calls-1)*pipelineCancellationInterval + pipelineCancellationInterval + 1 - probe.remaining
			for stop := 1; stop <= work; stop++ {
				ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
				probe := &pipelineCancellation{ctx: ctx, remaining: stop}
				run(probe)
				require.ErrorIs(t, probe.err, context.Canceled, "fixture=%d action=%d stop=%d/%d", fixture, action, stop, work)
				require.Equal(t, 1, ctx.calls)
			}
		}
	}
}

func TestPrepassStaticValidatorsStopInsideTokens(t *testing.T) {
	long := strings.Repeat("x", 1<<20)
	for name, validator := range map[string]func(string, *pipelineCancellation) error{
		"unicode operator": validateUnicodeOperatorsWithCancellation,
		"unicode literal":  validateUnicodeStringLiteralsWithCancellation,
		"map":              validateStaticMapKeysWithCancellation,
		"number":           validateNumericLiteralsWithCancellation,
		"arrow":            validateArrowOperatorsWithCancellation,
		"placeholder":      validatePlaceholderOperandsWithCancellation,
	} {
		for _, query := range []string{long, "'" + long + "'", "\"" + long + "\"", "`" + long + "`", "/*" + long + "*/", "//" + long, "RETURN {" + long + ": 1}", "RETURN 1." + strings.Repeat("0", 1<<20)} {
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
			probe := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
			err := validator(query, probe)
			require.ErrorIs(t, err, context.Canceled, name)
			require.ErrorIs(t, getExpressionFailure(probe.ctx), context.Canceled, name)
			require.Equal(t, 2, ctx.calls, name)
		}
	}
}

func replayPrepassWork(t *testing.T, run func(*pipelineCancellation)) {
	t.Helper()
	ctx := &pipelineCountingContext{Context: context.Background()}
	probe := &pipelineCancellation{ctx: ctx}
	run(probe)
	require.NoError(t, probe.err)
	if ctx.calls == 0 {
		return
	}
	work := (ctx.calls-1)*pipelineCancellationInterval + pipelineCancellationInterval + 1 - probe.remaining
	for stop := 1; stop <= work; stop++ {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
		probe := &pipelineCancellation{ctx: ctx, remaining: stop}
		run(probe)
		require.ErrorIs(t, probe.err, context.Canceled, "stop %d/%d", stop, work)
		require.Equal(t, 1, ctx.calls)
	}
}

func TestPrepassPrivateCaseReadersReplay(t *testing.T) {
	for _, text := range []string{
		"", " CASE CASE WHEN 1 THEN case END ", "CASE WHEN x THEN CASE WHEN y THEN 1 END ELSE 2 END",
		"RETURN x IS :: LIST<LIST<INTEGER>> END", "RETURN n:% END", "n:  % END", "WITH x STARTS WITH y END",
		"THEN case.id END", "CASE case WHEN 1 THEN case END", "CASE end WHEN 1 THEN 2 END", "CASE as WHEN 1 THEN 2 END",
		"THEN not END", "AND not THEN", "CASE WHEN 'a\\\"b' THEN x END", "CASE WHEN (x) THEN [1] ELSE {k:1} END",
		"WITH case WHERE case = 1", "when 1 then end end", "CASE and.id WHEN 1 THEN 2 END", "case + 1", "case[0]",
	} {
		for at := 0; at <= len(text); at++ {
			actions := []func(*pipelineCancellation){
				func(p *pipelineCancellation) { _ = caseKeywordAtWithCancellation(text, at, "CASE", p) },
				func(p *pipelineCancellation) { _ = caseKeywordAtWithCancellation(text, at, "END", p) },
				func(p *pipelineCancellation) { _ = caseKeywordAtWithCancellation(text, at, "WHEN", p) },
				func(p *pipelineCancellation) { _ = caseKeywordAtWithCancellation(text, at, "THEN", p) },
				func(p *pipelineCancellation) { _ = caseOperandExpectedBeforeWithCancellation(text, at, p) },
				func(p *pipelineCancellation) {
					_ = casePreviousWordInWithCancellation(text, at, p, "as", "case", "starts", "ends")
				},
				func(p *pipelineCancellation) { _ = casePrefixWordIsVariableWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseHasPreviousTokenWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseNestedLacksEndWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = casePreviousWordIsCaseWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseOperandEndsAtWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseVariableNamedCaseWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseSimpleTestContinuesAtWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = caseWordAtWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = insideCaseBlockWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = bracketDepthAtWithCancellation(text, at, p) },
				func(p *pipelineCancellation) { _ = enclosingCaseBlockEndWithCancellation(text, at, p) },
			}
			if at < len(text) {
				actions = append(actions,
					func(p *pipelineCancellation) { _ = caseTypeParameterClosesWithCancellation(text, at, p) },
					func(p *pipelineCancellation) { _ = caseBinaryOperatorAtWithCancellation(text, at, p) },
				)
			}
			for _, run := range actions {
				replayPrepassWork(t, run)
			}
		}
	}
}

func TestPrepassPrivateLexicalReadersReplay(t *testing.T) {
	for _, text := range append(prepassFixtureQueries,
		"  STARTS /* c */ WITH x", "RETURN n ENDS WITH x", "x TIME WITH TIME ZONE", "RETURN a ORDER BY b",
		"  `a``b`  ", "  'a''b'  ", "  \"a\\n\"  ", "  '\\u0041\\U0001F600' ", "  '\\uD83D\\uDE00' ",
		"0x"+strings.Repeat("0", 30)+"f", "RETURN 1_000.5_0e1_0, 0x_1_F, 0o1_7, 0_1, 1__2, 1_",
	) {
		actions := []func(*pipelineCancellation){
			func(p *pipelineCancellation) { _ = isOperatorWithWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = lastLiveByteWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = prevWordStartWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = startsWithClauseAfterDistinctWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = startsWithClauseAfterNameWithCancellation(text, p) },
			func(p *pipelineCancellation) { _, _ = trimKeywordWSBoundsWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = isNameableClauseKeywordWithCancellation(text, p) },
			func(p *pipelineCancellation) { _, _ = keywordMatchAtWithCancellation(text, 0, "ORDER BY", 0, 8, p) },
			func(p *pipelineCancellation) { _ = isValidIdentifierWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = validMapLiteralKeyWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = isWholeCypherQuotedStringWithCancellation(text, p) },
			func(p *pipelineCancellation) { _, _ = decodeCypherQuotedStringWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = isBacktickQuotedNameWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = symbolicNameValueWithCancellation(text, p) },
			func(p *pipelineCancellation) { _, _ = mapProjectionPropertySelectorWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = normalizePropertyKeyWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = splitTopLevelCommaWithCancellation(text, p) },
			func(p *pipelineCancellation) {
				_ = precedingSubqueryExpressionKeywordWithCancellation(text, len(text), p)
			},
			func(p *pipelineCancellation) { _ = subqueryBraceBodyStartsLikeClauseWithCancellation(text, -1, p) },
			func(p *pipelineCancellation) { _ = inProjectionClauseWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = insidePatternDelimiterWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = colonIsMapKeySeparatorWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _ = quantifiedGroupQuantifierAtWithCancellation(text, len(text), p) },
			func(p *pipelineCancellation) { _, _ = canonicalizeNumericLiteralsWithCancellation(text, p) },
			func(p *pipelineCancellation) { _ = numericLiteralHasUnaryMinusWithCancellation(text, len(text), p) },
		}
		for _, run := range actions {
			replayPrepassWork(t, run)
		}
		for at := 0; at <= len(text); at++ {
			replayPrepassWork(t, func(p *pipelineCancellation) { _ = wordIsNameWithCancellation(text, at, p) })
			replayPrepassWork(t, func(p *pipelineCancellation) {
				_ = clauseKeywordNamedByContextWithCancellation(text, at, min(at+4, len(text)), "WITH", p)
			})
			replayPrepassWork(t, func(p *pipelineCancellation) {
				_ = clauseKeywordUsedAsNameWithCancellation(text, at, min(at+6, len(text)), "RETURN", p)
			})
			replayPrepassWork(t, func(p *pipelineCancellation) { _ = queryGapEndWithCancellation(text, at, p) })
		}
	}
}

func TestPrepassLegacyReaderContract(t *testing.T) {
	for _, text := range append(prepassFixtureQueries, " RETURN ", "ORDER BY x", "`a``b`", "\"x\"", "'a''b'", "$(x)") {
		p := &pipelineCancellation{ctx: context.Background()}
		for _, pair := range []struct {
			plain   func(string) bool
			checked func(string, *pipelineCancellation) bool
		}{
			{startsWithClauseAfterDistinct, startsWithClauseAfterDistinctWithCancellation}, {startsWithClauseAfterName, startsWithClauseAfterNameWithCancellation},
			{wordStartsExpression, wordStartsExpressionWithCancellation}, {isNameableClauseKeyword, isNameableClauseKeywordWithCancellation},
			{isValidIdentifier, isValidIdentifierWithCancellation}, {validMapLiteralKey, validMapLiteralKeyWithCancellation},
			{isBacktickQuotedName, isBacktickQuotedNameWithCancellation}, {isWholeCypherQuotedString, isWholeCypherQuotedStringWithCancellation},
			{mayHaveGroupedNumericLiteral, mayHaveGroupedNumericLiteralWithCancellation},
		} {
			require.Equal(t, pair.plain(text), pair.checked(text, p))
		}
		for _, pair := range []struct {
			plain   func(string, int) bool
			checked func(string, int, *pipelineCancellation) bool
		}{
			{caseTypeParameterCloses, caseTypeParameterClosesWithCancellation}, {casePrefixWordIsVariable, casePrefixWordIsVariableWithCancellation},
			{caseHasPreviousToken, caseHasPreviousTokenWithCancellation}, {caseNestedLacksEnd, caseNestedLacksEndWithCancellation},
			{casePreviousWordIsCase, casePreviousWordIsCaseWithCancellation}, {caseOperandEndsAt, caseOperandEndsAtWithCancellation},
			{caseVariableNamedCase, caseVariableNamedCaseWithCancellation}, {caseSimpleTestContinuesAt, caseSimpleTestContinuesAtWithCancellation},
			{caseBinaryOperatorAt, caseBinaryOperatorAtWithCancellation}, {insideCaseBlock, insideCaseBlockWithCancellation},
			{precedingSubqueryExpressionKeyword, precedingSubqueryExpressionKeywordWithCancellation}, {subqueryBraceBodyStartsLikeClause, subqueryBraceBodyStartsLikeClauseWithCancellation},
			{inProjectionClause, inProjectionClauseWithCancellation}, {insidePatternDelimiter, insidePatternDelimiterWithCancellation},
			{colonIsMapKeySeparator, colonIsMapKeySeparatorWithCancellation}, {quantifiedGroupQuantifierAt, quantifiedGroupQuantifierAtWithCancellation},
		} {
			if text != "" {
				require.Equal(t, pair.plain(text, 0), pair.checked(text, 0, p))
			}
		}
		for _, word := range []string{"RETURN", "WITH", "CASE"} {
			require.Equal(t, casePreviousWordIn(text, len(text), word), casePreviousWordInWithCancellation(text, len(text), p, word))
		}
		for _, pair := range []struct {
			plain   func(string) string
			checked func(string, *pipelineCancellation) string
		}{
			{symbolicNameValue, symbolicNameValueWithCancellation}, {normalizePropertyKey, normalizePropertyKeyWithCancellation},
		} {
			require.Equal(t, pair.plain(text), pair.checked(text, p))
		}
		require.Equal(t, skipASCIISpaces(text, 0, len(text)), skipASCIISpacesWithCancellation(text, 0, len(text), p))
		require.Equal(t, followedByTimeZone(text, 0), followedByTimeZoneWithCancellation(text, 0, p))
		require.Equal(t, normNumberSafe(text), normNumberSafeWithCancellation(text, p))
		require.NoError(t, p.err)
	}
	for _, number := range []string{"1", "0x1", ".25", "1e3", "1e999", "-1"} {
		p := &pipelineCancellation{ctx: context.Background()}
		a, errA := validateNumericLiteralAt(number, 0)
		b, errB := validateNumericLiteralAtWithCancellation(number, 0, p)
		require.Equal(t, a, b)
		require.Equal(t, fmt.Sprint(errA), fmt.Sprint(errB))
		a, errA = validateFloatingLiteralTail(number, 0, len(number))
		b, errB = validateFloatingLiteralTailWithCancellation(number, 0, len(number), p)
		require.Equal(t, a, b)
		require.Equal(t, fmt.Sprint(errA), fmt.Sprint(errB))
		require.Equal(t, numericLiteralHasUnaryMinus(number, len(number)), numericLiteralHasUnaryMinusWithCancellation(number, len(number), p))
		x, y := groupedNumericLiteralEnd(number, 0)
		z, w := groupedNumericLiteralEndWithCancellation(number, 0, p)
		require.Equal(t, x, z)
		require.Equal(t, y, w)
		e, v, g := numericDigitRun(number, 0, isASCIIDigit, false)
		e2, v2, g2 := numericDigitRunWithCancellation(number, 0, isASCIIDigit, false, p)
		require.Equal(t, []interface{}{e, v, g}, []interface{}{e2, v2, g2})
	}
	for _, quoted := range []string{"'a''b'", `"a\n\r\t\b\f\\\""`, `'\u0041'`, `'\U0001F600'`, `'\uD83D\uDE00'`, `'\uD800'`, `'\uZZZZ'`, `'\UFFFFFFFF'`} {
		replayPrepassWork(t, func(p *pipelineCancellation) { _, _ = decodeCypherQuotedStringWithCancellation(quoted, p) })
	}
	for _, number := range []string{"1." + strings.Repeat("0", 800) + "e+1", "1." + strings.Repeat("0", 800) + "e-1", strings.Repeat("0", 800) + "e0", "0x" + strings.Repeat("0", 30) + "f"} {
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = normNumberSafeWithCancellation(number, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = floatLiteralInRangeWithCancellation(number, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) {
			_, _ = parseUintLiteralWithCancellation(strings.Repeat("0", 30)+"f", 16, p)
		})
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = validateNumericLiteralsWithCancellation("RETURN "+number, p) })
	}
	for _, needle := range []string{"", "needle", strings.Repeat("a", 80)} {
		text := strings.Repeat("x", 4100) + needle
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = indexTextWithCancellation(text, needle, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = equalFoldASCIIWithCancellation(text, text, p) })
		require.Equal(t, strings.Index(text, needle), indexTextWithCancellation(text, needle, nil))
	}
	for _, text := range []string{"abcDEF", "NOT     CASE", "CASE  RETURN", "ORDER     BY x", "`x``y`", "'x''y'"} {
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = lowerASCIIWithCancellation(text, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = wordStartsExpressionWithCancellation(text, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { _ = nameContinuesExpressionWithCancellation(text, p) })
		if text[0] == '`' || text[0] == '\'' {
			a, b := scanCypherQuotedText(text, 0, text[0])
			c, d := scanCypherQuotedTextWithCancellation(text, 0, text[0], &pipelineCancellation{ctx: context.Background()})
			require.Equal(t, a, c)
			require.Equal(t, b, d)
		}
	}
	require.Contains(t, numericLiteralError("InvalidNumberLiteral", "1oops").Error(), `"1oops"`)
}
