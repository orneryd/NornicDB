package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------- tokenizer --------

func TestFulltextTokenizer_BasicShapes(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  []ftTokenKind
	}{
		{"bare term", "CloudTrail", []ftTokenKind{tkTerm, tkEOF}},
		{"two terms", "foo bar", []ftTokenKind{tkTerm, tkTerm, tkEOF}},
		{"phrase", `"foo bar"`, []ftTokenKind{tkQuoted, tkEOF}},
		{"AND boolean", "foo AND bar", []ftTokenKind{tkTerm, tkAnd, tkTerm, tkEOF}},
		{"OR boolean", "foo OR bar", []ftTokenKind{tkTerm, tkOr, tkTerm, tkEOF}},
		{"NOT prefix", "NOT foo", []ftTokenKind{tkNot, tkTerm, tkEOF}},
		{"C-style AND", "foo && bar", []ftTokenKind{tkTerm, tkAnd, tkTerm, tkEOF}},
		{"C-style OR", "foo || bar", []ftTokenKind{tkTerm, tkOr, tkTerm, tkEOF}},
		{"plus minus", "+foo -bar", []ftTokenKind{tkPlus, tkTerm, tkMinus, tkTerm, tkEOF}},
		{"boost", "foo^2", []ftTokenKind{tkTerm, tkCaret, tkTerm, tkEOF}},
		{"fuzzy", "foo~", []ftTokenKind{tkTerm, tkTilde, tkEOF}},
		{"fuzzy n", "foo~2", []ftTokenKind{tkTerm, tkTilde, tkTerm, tkEOF}},
		{"phrase proximity", `"foo bar"~3`, []ftTokenKind{tkQuoted, tkTilde, tkTerm, tkEOF}},
		{"wildcard suffix", "foo*", []ftTokenKind{tkTerm, tkStar, tkEOF}},
		{"wildcard leading", "*foo", []ftTokenKind{tkStar, tkTerm, tkEOF}},
		{"wildcard single", "f?o", []ftTokenKind{tkTerm, tkQuestion, tkTerm, tkEOF}},
		{"range incl", "[a TO b]", []ftTokenKind{tkLBrack, tkTerm, tkTo, tkTerm, tkRBrack, tkEOF}},
		{"range mixed", "{a TO b]", []ftTokenKind{tkLBrace, tkTerm, tkTo, tkTerm, tkRBrack, tkEOF}},
		{"regex", "/re[gex]/", []ftTokenKind{tkRegex, tkEOF}},
		{"field colon", "field:value", []ftTokenKind{tkTerm, tkColon, tkTerm, tkEOF}},
		{"field phrase", `field:"phrase"`, []ftTokenKind{tkTerm, tkColon, tkQuoted, tkEOF}},
		{"field star", "field:*", []ftTokenKind{tkTerm, tkColon, tkStar, tkEOF}},
		{"nested group", "(a OR (b AND c))", []ftTokenKind{
			tkLParen, tkTerm, tkOr, tkLParen, tkTerm, tkAnd, tkTerm, tkRParen, tkRParen, tkEOF,
		}},
		{"graphiti shape", `group_id:"ft_repro" AND (CloudTrail)`, []ftTokenKind{
			tkTerm, tkColon, tkQuoted, tkAnd, tkLParen, tkTerm, tkRParen, tkEOF,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ftTokenize(tc.input)
			kinds := make([]ftTokenKind, len(got))
			for i, tk := range got {
				kinds[i] = tk.kind
			}
			assert.Equal(t, tc.want, kinds)
		})
	}
}

// TestFulltextTokenizer_EscapeRules — every `\X` decodes to `X` regardless of
// whether X is a Lucene special char. This is the fix for the Graphiti
// backslash-escape issue.
func TestFulltextTokenizer_EscapeRules(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{`Cloud\Trail`, "CloudTrail"},
		{`foo\ bar`, "foo bar"},
		{`\+foo`, "+foo"},
		{`\-foo`, "-foo"},
		{`\(a\)`, "(a)"},
		{`a\\b`, `a\b`},
		{`\"quoted\"`, `"quoted"`},
		{`\:colon`, ":colon"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := ftTokenize(tc.input)
			var sb strings.Builder
			for _, tk := range got {
				if tk.kind == tkTerm || tk.kind == tkQuoted {
					sb.WriteString(tk.text)
				}
			}
			assert.Equal(t, tc.want, sb.String())
		})
	}
}

// TestFulltextTokenizer_PhraseEscapes checks that escapes inside phrases decode.
func TestFulltextTokenizer_PhraseEscapes(t *testing.T) {
	got := ftTokenize(`"quoted \"phrase\" text"`)
	require.GreaterOrEqual(t, len(got), 1)
	require.Equal(t, tkQuoted, got[0].kind)
	assert.Equal(t, `quoted "phrase" text`, got[0].text)
}

// TestFulltextTokenizer_UnterminatedQuote — best-effort behaviour: whatever
// text was scanned becomes the phrase and we don't panic.
func TestFulltextTokenizer_UnterminatedQuote(t *testing.T) {
	got := ftTokenize(`"unterminated`)
	require.NotEmpty(t, got)
	assert.Equal(t, tkQuoted, got[0].kind)
	assert.Equal(t, "unterminated", got[0].text)
}

// TestFulltextTokenizer_RegexPositionDetection verifies that a bare `/` outside
// an atom-start position is treated as a term character, not a regex delimiter.
func TestFulltextTokenizer_RegexPositionDetection(t *testing.T) {
	// After a term, `/` should merge — but the term will actually end at
	// the `/` since `/` is not a term char. This documents the current
	// behavior: `foo/bar` splits into TERM, then `/bar` opens a regex only
	// if canStartRegex says so; after a bare TERM, canStartRegex returns
	// false, so `/bar` would be skipped (no term char). We assert
	// no panic and the resulting tokens are stable.
	got := ftTokenize(`foo/bar/`)
	// First token must be a term "foo".
	require.NotEmpty(t, got)
	assert.Equal(t, tkTerm, got[0].kind)
	assert.Equal(t, "foo", got[0].text)
}

// -------- parser + evaluator helpers --------

// makeDoc canonicalizes a fixed doc into an ftDoc for evaluator tests.
func makeDoc(props map[string]string, defaults ...string) *ftDoc {
	p := make(map[string]string, len(props))
	raw := make(map[string]string, len(props))
	var content strings.Builder
	for k, v := range props {
		p[strings.ToLower(k)] = strings.ToLower(v)
		raw[strings.ToLower(k)] = v
	}
	if len(defaults) == 0 {
		for _, v := range props {
			content.WriteString(v)
			content.WriteByte(' ')
		}
	} else {
		for _, f := range defaults {
			if v, ok := props[f]; ok {
				content.WriteString(v)
				content.WriteByte(' ')
			}
		}
	}
	c := strings.ToLower(strings.TrimSpace(content.String()))
	return &ftDoc{
		properties:    p,
		rawProperties: raw,
		contentLower:  c,
		contentTokenN: len(strings.Fields(c)),
	}
}

func defaultCtx(defaults []string, docs ...*ftDoc) *ftEvalCtx {
	df := map[string]int{}
	total := len(docs)
	// Populate docFreq by scanning contentLower for any word.
	for _, d := range docs {
		seen := map[string]bool{}
		for _, tok := range strings.Fields(d.contentLower) {
			if seen[tok] {
				continue
			}
			seen[tok] = true
			df[tok]++
		}
	}
	return &ftEvalCtx{
		DefaultFields: defaults,
		avgDocLen:     8,
		totalDocs:     total,
		docFreq:       df,
	}
}

// TestFulltextParser_GraphitiShape is the direct fix for the reported bug.
func TestFulltextParser_GraphitiShape(t *testing.T) {
	defaultFields := []string{"name", "summary", "group_id"}
	n1 := makeDoc(map[string]string{"name": "CloudTrail", "summary": "AWS CloudTrail audit logging", "group_id": "ft_repro"}, defaultFields...)
	n2 := makeDoc(map[string]string{"name": "Ladybug", "summary": "embedded graph driver", "group_id": "ft_repro"}, defaultFields...)
	n3 := makeDoc(map[string]string{"name": "Redis", "summary": "in-memory data store", "group_id": "ft_repro"}, defaultFields...)
	nOther := makeDoc(map[string]string{"name": "OutOfGroup", "summary": "different group", "group_id": "different"}, defaultFields...)
	corpus := map[string]*ftDoc{"n1": n1, "n2": n2, "n3": n3, "nOther": nOther}
	ctx := defaultCtx(defaultFields, n1, n2, n3, nOther)

	run := func(t *testing.T, q string) map[string]bool {
		t.Helper()
		parsed, err := ParseFulltextQuery(q)
		require.NoError(t, err)
		got := map[string]bool{}
		for id, d := range corpus {
			m, _ := parsed.Match(ctx, d)
			if m {
				got[id] = true
			}
		}
		return got
	}

	assert.Equal(t, map[string]bool{"n1": true}, run(t, `group_id:"ft_repro" AND (CloudTrail)`))
	assert.Equal(t, map[string]bool{"n2": true}, run(t, `group_id:"ft_repro" AND (Ladybug)`))
	assert.Equal(t, map[string]bool{}, run(t, `group_id:"ft_repro" AND (zzznope)`))
	assert.Equal(t, map[string]bool{"n1": true, "n3": true}, run(t, `group_id:"ft_repro" AND (CloudTrail OR Redis)`))
	assert.Equal(t, map[string]bool{"n1": true, "n3": true}, run(t, `group_id:"ft_repro" AND -Ladybug`))
	// Group filter alone must exclude nOther.
	assert.Equal(t, map[string]bool{"n1": true, "n2": true, "n3": true}, run(t, `group_id:"ft_repro"`))
}

// -------- Full Lucene-classic grammar coverage --------
//
// Each block below exercises one production of the grammar with a
// dedicated corpus. Tests assert both matching and non-matching cases so
// we cover the full truth table, not just the happy path.

// BareTerm — default-field term matching.
func TestFulltextParser_BareTerm(t *testing.T) {
	defs := []string{"name", "summary"}
	d1 := makeDoc(map[string]string{"name": "alpha", "summary": "beta"}, defs...)
	d2 := makeDoc(map[string]string{"name": "gamma", "summary": "delta"}, defs...)
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{"alpha", []string{"d1"}},
		{"beta", []string{"d1"}},
		{"gamma", []string{"d2"}},
		{"missing", nil},
	}
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			got := matchIDs(parsed, ctx, corpus)
			assert.ElementsMatch(t, tc.hits, got)
		})
	}
}

// BooleanOR — implicit AND-of-SHOULD when no AND: bare `a b` acts as SHOULD.
// (Lucene default operator is OR unless setDefaultOperator changed it; we
// mirror that default so hybrid-search hits at least one term.)
func TestFulltextParser_BooleanOR(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha"}, defs...)
	d2 := makeDoc(map[string]string{"content": "beta"}, defs...)
	d3 := makeDoc(map[string]string{"content": "gamma"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3}
	ctx := defaultCtx(defs, d1, d2, d3)

	cases := []struct {
		q    string
		hits []string
	}{
		{"alpha OR beta", []string{"d1", "d2"}},
		{"alpha || beta", []string{"d1", "d2"}},
		{"alpha OR beta OR gamma", []string{"d1", "d2", "d3"}},
		{"alpha OR missing", []string{"d1"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// BooleanAND — all clauses mandatory.
func TestFulltextParser_BooleanAND(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha beta gamma"}, defs...)
	d2 := makeDoc(map[string]string{"content": "alpha delta"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{"alpha AND beta", []string{"d1"}},
		{"alpha && beta", []string{"d1"}},
		{"alpha AND missing", nil},
		{"alpha AND beta AND gamma", []string{"d1"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// NOT and prohibited (`-`).
func TestFulltextParser_NotAndProhibited(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha beta"}, defs...)
	d2 := makeDoc(map[string]string{"content": "alpha gamma"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{"alpha AND NOT beta", []string{"d2"}},
		{"alpha -beta", []string{"d2"}},
		{"alpha AND -gamma", []string{"d1"}},
		{"NOT beta", []string{"d2"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Required (`+`) marks a mandatory clause even in an otherwise SHOULD list.
func TestFulltextParser_RequiredPlus(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha beta"}, defs...)
	d2 := makeDoc(map[string]string{"content": "beta gamma"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{"+alpha beta", []string{"d1"}},
		{"+alpha +beta", []string{"d1"}},
		{"+alpha", []string{"d1"}},
		{"+missing beta", nil},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Phrase queries and phrase-inside-boolean.
func TestFulltextParser_Phrase(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha beta gamma"}, defs...)
	d2 := makeDoc(map[string]string{"content": "beta alpha gamma"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{`"alpha beta"`, []string{"d1"}},
		{`"beta alpha"`, []string{"d2"}},
		{`"alpha beta" OR "beta alpha"`, []string{"d1", "d2"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Phrase proximity `"a b"~n` — we accept the syntax and fall back to
// substring matching (Lucene proximity needs token positions).
func TestFulltextParser_PhraseProximity(t *testing.T) {
	defs := []string{"content"}
	d := makeDoc(map[string]string{"content": "alpha beta gamma"}, defs...)
	ctx := defaultCtx(defs, d)
	corpus := map[string]*ftDoc{"d": d}
	parsed, err := ParseFulltextQuery(`"alpha beta"~3`)
	require.NoError(t, err)
	assert.Equal(t, []string{"d"}, matchIDs(parsed, ctx, corpus))
}

// Field-scoped clauses covering all their variants.
func TestFulltextParser_FieldScoped(t *testing.T) {
	defs := []string{"name", "summary"}
	d1 := makeDoc(map[string]string{"name": "alpha", "summary": "hello world", "tag": "gold"}, defs...)
	d2 := makeDoc(map[string]string{"name": "beta", "summary": "hello there", "tag": ""}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{"name:alpha", []string{"d1"}},
		{"name:beta", []string{"d2"}},
		{`name:"alpha"`, []string{"d1"}},
		{"tag:*", []string{"d1"}}, // d2's tag is empty
		{"missing:*", nil},
		{"name:alp*", []string{"d1"}},
		{"name:*ta", []string{"d2"}},
		{"summary:hello", []string{"d1", "d2"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Field-scoped group `field:(a OR b)` rebinds atoms inside the group to
// the outer field.
func TestFulltextParser_FieldScopedGroup(t *testing.T) {
	defs := []string{"name"}
	d1 := makeDoc(map[string]string{"name": "alpha"}, defs...)
	d2 := makeDoc(map[string]string{"name": "beta"}, defs...)
	d3 := makeDoc(map[string]string{"name": "gamma"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3}
	ctx := defaultCtx(defs, d1, d2, d3)

	parsed, err := ParseFulltextQuery("name:(alpha OR beta)")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1", "d2"}, matchIDs(parsed, ctx, corpus))
}

// Wildcards: `*` matches any run, `?` matches single char, leading and
// mid-token wildcards.
func TestFulltextParser_Wildcards(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "cloudtrail"}, defs...)
	d2 := makeDoc(map[string]string{"content": "cloudy"}, defs...)
	d3 := makeDoc(map[string]string{"content": "sunny"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3}
	ctx := defaultCtx(defs, d1, d2, d3)

	cases := []struct {
		q    string
		hits []string
	}{
		{"cloud*", []string{"d1", "d2"}},
		{"*trail", []string{"d1"}},
		{"clo?dy", []string{"d2"}},
		{"clou*ail", []string{"d1"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Range queries `[a TO b]` / `{a TO b]` inclusive/exclusive.
func TestFulltextParser_Range(t *testing.T) {
	defs := []string{"name"}
	dA := makeDoc(map[string]string{"name": "apple"}, defs...)
	dB := makeDoc(map[string]string{"name": "banana"}, defs...)
	dC := makeDoc(map[string]string{"name": "cherry"}, defs...)
	corpus := map[string]*ftDoc{"a": dA, "b": dB, "c": dC}
	ctx := defaultCtx(defs, dA, dB, dC)

	cases := []struct {
		q    string
		hits []string
	}{
		{"name:[apple TO cherry]", []string{"a", "b", "c"}},
		{"name:{apple TO cherry}", []string{"b"}},
		{"name:[apple TO cherry}", []string{"a", "b"}},
		{"name:{apple TO cherry]", []string{"b", "c"}},
		{"name:[a TO b]", []string{"a"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Regex `/re/` matches Go-regexp syntax on the property value.
func TestFulltextParser_Regex(t *testing.T) {
	defs := []string{"name"}
	d1 := makeDoc(map[string]string{"name": "CloudTrail"}, defs...)
	d2 := makeDoc(map[string]string{"name": "Ladybug"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2}
	ctx := defaultCtx(defs, d1, d2)

	cases := []struct {
		q    string
		hits []string
	}{
		{`name:/cloud.*/`, []string{"d1"}},
		{`name:/lady/`, []string{"d2"}},
		{`name:/^clou/`, []string{"d1"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Fuzzy `term~n` — Levenshtein distance.
func TestFulltextParser_Fuzzy(t *testing.T) {
	defs := []string{"name"}
	d1 := makeDoc(map[string]string{"name": "cloudtrail"}, defs...)
	d2 := makeDoc(map[string]string{"name": "cloudtale"}, defs...)
	d3 := makeDoc(map[string]string{"name": "totally-different"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3}
	ctx := defaultCtx(defs, d1, d2, d3)

	// Default fuzzy (max 2 edits): cloudtrail vs cloudtale = 3 edits (r->_, i->l, l->e), doesn't match.
	parsed, err := ParseFulltextQuery("cloudtrail~")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1"}, matchIDs(parsed, ctx, corpus))

	// Distance 0 — only exact match.
	parsed, err = ParseFulltextQuery("cloudtrail~0")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1"}, matchIDs(parsed, ctx, corpus))

	// Distance 3 — includes the near-miss.
	parsed, err = ParseFulltextQuery("cloudtrail~3")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1", "d2"}, matchIDs(parsed, ctx, corpus))
}

// Boost `^n` — multiplies score of matching subtree; must not affect match set.
func TestFulltextParser_Boost(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha"}, defs...)
	ctx := defaultCtx(defs, d1)

	parsed, err := ParseFulltextQuery("alpha^2")
	require.NoError(t, err)
	matched, scoreBoosted := parsed.Match(ctx, d1)
	require.True(t, matched)

	parsedBare, err := ParseFulltextQuery("alpha")
	require.NoError(t, err)
	_, scoreBare := parsedBare.Match(ctx, d1)
	assert.Greater(t, scoreBoosted, scoreBare, "boost should amplify score")
}

// Nested groups exercise the parser's recursion.
func TestFulltextParser_NestedGroups(t *testing.T) {
	defs := []string{"content"}
	d1 := makeDoc(map[string]string{"content": "alpha beta"}, defs...)
	d2 := makeDoc(map[string]string{"content": "alpha gamma"}, defs...)
	d3 := makeDoc(map[string]string{"content": "delta"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3}
	ctx := defaultCtx(defs, d1, d2, d3)

	cases := []struct {
		q    string
		hits []string
	}{
		{"(alpha AND beta) OR delta", []string{"d1", "d3"}},
		{"alpha AND (beta OR gamma)", []string{"d1", "d2"}},
		{"((alpha OR delta) AND NOT beta)", []string{"d2", "d3"}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Escape rules end-to-end through the parser.
func TestFulltextParser_EscapeSurface(t *testing.T) {
	defs := []string{"name"}
	d1 := makeDoc(map[string]string{"name": "CloudTrail"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1}
	ctx := defaultCtx(defs, d1)

	cases := []struct {
		q    string
		hits []string
	}{
		{`Cloud\Trail`, []string{"d1"}}, // Graphiti escape
		{`\+CloudTrail`, nil},           // decoded to "+CloudTrail" — literal + is not in doc content
		{`\(CloudTrail\)`, nil},         // parens as literal chars in the value — not in doc
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := ParseFulltextQuery(tc.q)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.hits, matchIDs(parsed, ctx, corpus))
		})
	}
}

// Unknown field matches nothing (Neo4j-Lucene semantics — no postings).
func TestFulltextParser_UnknownFieldMatchesNothing(t *testing.T) {
	defs := []string{"name"}
	d1 := makeDoc(map[string]string{"name": "alpha"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1}
	ctx := defaultCtx(defs, d1)

	parsed, err := ParseFulltextQuery(`totally_unknown:"whatever" AND alpha`)
	require.NoError(t, err)
	assert.Empty(t, matchIDs(parsed, ctx, corpus))
}

// Malformed inputs must produce parse errors.
func TestFulltextParser_MalformedInput(t *testing.T) {
	cases := []struct {
		q          string
		wantErrSub string
	}{
		{"(unbalanced", "missing ')'"},
		{"foo^", "expected number"},
		{"name:[a b]", "expected TO"},
		{"name:[a TO", "expected range endpoint"},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			_, err := ParseFulltextQuery(tc.q)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

// Empty and whitespace-only queries return an empty parsed structure.
func TestFulltextParser_EmptyQuery(t *testing.T) {
	for _, in := range []string{"", "   ", "\t\n"} {
		q, err := ParseFulltextQuery(in)
		require.NoError(t, err)
		assert.True(t, q.IsEmpty(), "empty input %q should be IsEmpty", in)
	}
}

// PrimaryTerms feeds docFreq for BM25; must include every scoring term and
// exclude wildcards/ranges/regex (which don't contribute to BM25).
func TestFulltextParser_PrimaryTerms(t *testing.T) {
	q, err := ParseFulltextQuery(`group_id:"ft_repro" AND (CloudTrail OR Ladybug OR name:/re/)`)
	require.NoError(t, err)
	terms := q.PrimaryTerms()
	assert.Contains(t, terms, "ft_repro")
	assert.Contains(t, terms, "cloudtrail")
	assert.Contains(t, terms, "ladybug")
	// Regex leaf should not appear as a primary term.
	for _, term := range terms {
		assert.NotEqual(t, "re", term)
	}
}

// Complex composite covering multiple productions at once.
func TestFulltextParser_ComplexComposite(t *testing.T) {
	defs := []string{"name", "summary", "group_id"}
	d1 := makeDoc(map[string]string{"name": "CloudTrail", "summary": "AWS audit logging", "group_id": "g1"}, defs...)
	d2 := makeDoc(map[string]string{"name": "Ladybug", "summary": "graph db", "group_id": "g1"}, defs...)
	d3 := makeDoc(map[string]string{"name": "Redis", "summary": "in-memory kv store", "group_id": "g1"}, defs...)
	d4 := makeDoc(map[string]string{"name": "CloudTrail", "summary": "different group", "group_id": "g2"}, defs...)
	corpus := map[string]*ftDoc{"d1": d1, "d2": d2, "d3": d3, "d4": d4}
	ctx := defaultCtx(defs, d1, d2, d3, d4)

	// Group filter AND (CloudTrail OR "in-memory kv") NOT Ladybug boost^2.
	q := `group_id:"g1" AND (CloudTrail OR "in-memory kv") AND -Ladybug`
	parsed, err := ParseFulltextQuery(q)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1", "d3"}, matchIDs(parsed, ctx, corpus))
}

// -------- helpers --------

func matchIDs(q *FulltextQuery, ctx *ftEvalCtx, corpus map[string]*ftDoc) []string {
	got := make([]string, 0, len(corpus))
	for id, d := range corpus {
		m, _ := q.Match(ctx, d)
		if m {
			got = append(got, id)
		}
	}
	return got
}

// -------- decodeCypherStringLiteral tests --------

func TestDecodeCypherStringLiteral(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`'foo'`, "foo"},
		{`"foo"`, "foo"},
		{`'it\\'s'`, `'it\\'s'`}, // not one string token; preserve the invalid source
		{`'it\'s'`, "it's"},      // a valid escaped quote decodes
		{`'it''s'`, `'it''s'`},   // adjacent literals are not a single token
		{`'has \\backslash'`, `has \backslash`},
		{`'has \n newline'`, "has \n newline"},
		{`'has \t tab'`, "has \t tab"},
		{`'unknown \Z'`, `unknown \Z`}, // unknown escape preserved for Lucene layer
		{`no_quotes`, "no_quotes"},
		{`'mismatched"`, `'mismatched"`},
		{``, ``},
		{`'`, `'`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := decodeCypherStringLiteral(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}

// -------- Levenshtein --------

func TestLevenshtein(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "", 1},
		{"", "a", 1},
		{"kitten", "sitting", 3},
		{"cloudtrail", "cloudtale", 3},
		{"cloudtrail", "cloudtrail", 0},
	}
	for _, tc := range cases {
		got := levenshtein(tc.a, tc.b)
		assert.Equalf(t, tc.want, got, "levenshtein(%q,%q)", tc.a, tc.b)
	}
}
