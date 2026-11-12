package cypher

import (
	"context"
	"fmt"
	"slices"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// A statementExecutionContract retains the grammar-owned
// antlr.StatementEffects together with the parse it was read from. That product
// is unique per successful parse, so it carries its own SyntaxIdentity and its
// own source and offsets: a body-only spelling and the same statement behind a
// graph reference never share it, even when every effect they decide agrees.
//
// These facts are the source-independent projection of one contract. Every
// retained site keeps the canonical grammar window it was read from, so the
// contracts compare by the flags and callable products they expose instead of
// by pointer or source identity.
type statementContractFacts struct {
	source                                                string
	writes, transaction, calls, dynamic, unknownProcedure bool
	nodeImages                                            ProcedureNodeImages
	selectsDatabase, stateChange                          bool
	effect                                                statementEffect
	effects                                               statementEffectFacts
}

// statementEffectFacts is the comparable half of antlr.StatementEffects: its
// structural flags, its executable query scopes and the procedure and scalar
// invocations it retains. Identity and source text stay with the parse that
// produced them; every retained scope and invocation keeps its own grammar
// window, so a graph reference can be re-anchored by removing exactly the bytes
// its spelling inserted.
type statementEffectFacts struct {
	writes, readOnly, remoteReadOnly, allowsExecutableQueries bool
	executableScopes                                          []statementScopeFact
	executableCalls                                           []statementCallableFact
	calls                                                     []statementCallableFact
	functions                                                 []statementFunctionFact
}

// statementScopeFact is one retained executable scope: the exact source bytes
// the grammar owns and the byte window they were read from in the statement
// this contract was read from.
type statementScopeFact struct {
	text       string
	start, end int
}

// statementCallableFact is one retained invocation as the contract exposes it:
// the decoded qualified name and the exact invocation text, which carries the
// argument expression.
type statementCallableFact struct {
	name, text string
}

// statementFunctionFact is one retained scalar invocation: its decoded name,
// the exact invocation text that carries its argument expression, the grammar
// window that owns it, and whether the grammar places it inside a USE clause.
type statementFunctionFact struct {
	statementCallableFact
	start, end     int
	graphReference bool
}

// statementGraphReference is exactly what a USE spelling inserted ahead of the
// statement body: the inserted bytes and the statement offsets that own them.
// An insertion offset names the byte where the spelling spliced its reference
// in, so re-anchoring removes those bytes and nothing else. An empty text means
// the spelling inserted no reference.
type statementGraphReference struct {
	text       string
	insertions []int
}

// statementContractFactsOf projects one contract onto comparable facts. Every
// retained site is proven to belong to source, the statement this contract was
// read from: a scalar invocation must carry that statement's own source and
// byte window, and an executable scope must be the exact text of a query node
// of it. Two contracts then compare by those facts alone.
func statementContractFactsOf(ctx context.Context, source string, contract statementExecutionContract) (statementContractFacts, error) {
	// readStatementExecutionContract reads the statement after the execution
	// mode owner trims it, so that is the source its retained sites carry.
	statement := strings.TrimSpace(source)
	effects := contract.functions
	facts := statementContractFacts{
		source: statement,
		writes: contract.writes, transaction: contract.transaction, calls: contract.calls,
		dynamic: contract.dynamic, unknownProcedure: contract.unknownProcedure,
		nodeImages: contract.nodeImages, selectsDatabase: contract.selectsDatabase,
		stateChange: contract.stateChange, effect: contract.effect(),
		effects: statementEffectFacts{writes: effects.Writes(), remoteReadOnly: effects.RemoteReadOnly()},
	}
	own := func(owner, text string) (string, error) {
		if text == "" || !strings.Contains(statement, text) {
			return "", fmt.Errorf("%s text %q is not in its own statement %q", owner, text, statement)
		}
		return text, nil
	}
	// ReadOnly reports the structural verdict and, when the statement is
	// read-only, hands over exactly the invocations live policy would gate.
	var executable []cypherantlr.StatementCall
	facts.effects.readOnly = effects.ReadOnly(func(call cypherantlr.StatementCall) bool {
		executable = append(executable, call)
		return true
	})
	for _, call := range executable {
		text, err := own(fmt.Sprintf("executable procedure %q", call.Name()), call.Text())
		if err != nil {
			return statementContractFacts{}, err
		}
		facts.effects.executableCalls = append(facts.effects.executableCalls, statementCallableFact{name: call.Name(), text: text})
	}
	// AllowsExecutableQueries exposes the retained executable scopes: EXPLAIN
	// suppresses its own scope and every later scope keeps its own source window.
	var scopes []string
	facts.effects.allowsExecutableQueries = effects.AllowsExecutableQueries(func(scope string) bool {
		scopes = append(scopes, scope)
		return true
	})
	scopeFacts, err := statementScopeFacts(ctx, statement, scopes)
	if err != nil {
		return statementContractFacts{}, err
	}
	facts.effects.executableScopes = scopeFacts
	for index := 0; index < effects.CallCount(); index++ {
		call := effects.Call(index)
		text, err := own(fmt.Sprintf("procedure invocation %q", call.Name()), call.Text())
		if err != nil {
			return statementContractFacts{}, err
		}
		facts.effects.calls = append(facts.effects.calls, statementCallableFact{name: call.Name(), text: text})
	}
	functionFacts, err := statementFunctionFacts(statement, effects)
	if err != nil {
		return statementContractFacts{}, err
	}
	facts.effects.functions = functionFacts
	return facts, nil
}

// statementScopeFacts anchors the executable scopes the effects owner retained
// onto the frozen source it read them from. The retained scope text stays the
// authority; the canonical grammar product for the same source supplies each
// scope's own byte window, and every retained scope must be the exact text of a
// grammar query node in the owner's own order. A window is therefore never
// guessed from the text it happens to spell.
func statementScopeFacts(ctx context.Context, statement string, scopes []string) ([]statementScopeFact, error) {
	if len(scopes) == 0 {
		return nil, nil
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, statement, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return nil, fmt.Errorf("read the grammar windows of %q: %w", statement, err)
	}
	facts := make([]statementScopeFact, 0, len(scopes))
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if len(facts) == len(scopes) || node.Rule() != cypherantlr.CypherParserRULE_query {
			return nil
		}
		text := scopes[len(facts)]
		if node.Text() != text {
			return nil
		}
		start, end := node.Span()
		facts = append(facts, statementScopeFact{text: text, start: start, end: end})
		return nil
	})
	if len(facts) != len(scopes) {
		return nil, fmt.Errorf("retained executable scope %q has no grammar window in %q", scopes[len(facts)], statement)
	}
	return facts, nil
}

// statementFunctionFacts reads one canonical grammar product's retained scalar
// invocations in grammar order: the decoded qualified name, the exact
// invocation text that carries the argument expression, the site's own byte
// window in the source that produced it, and the USE clause that owns it.
func statementFunctionFacts(source string, effects cypherantlr.StatementEffects) ([]statementFunctionFact, error) {
	facts := make([]statementFunctionFact, 0, effects.FunctionCount())
	for index := 0; index < effects.FunctionCount(); index++ {
		site := effects.Function(index)
		if !site.Valid() || site.Rule() != cypherantlr.CypherParserRULE_functionInvocation {
			return nil, fmt.Errorf("retained function site %d is not a scalar invocation", index)
		}
		if site.Source() != source {
			return nil, fmt.Errorf("retained function site %d belongs to another statement source", index)
		}
		start, end := site.Span()
		if start < 0 || start >= end || end > len(source) {
			return nil, fmt.Errorf("retained function site %d span %d:%d is outside its own statement", index, start, end)
		}
		facts = append(facts, statementFunctionFact{
			statementCallableFact: statementCallableFact{
				name: site.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName(),
				text: source[start:end],
			},
			start: start, end: end,
			graphReference: statementInsideGraphReference(site),
		})
	}
	return facts, nil
}

// statementReferenceFunctions reads the scalar invocations a supplied graph
// reference owns through the canonical USE grammar product. A USE reference's
// arguments are expressions and its own name is a catalog target, not a call,
// so only the argument expressions contribute; each retained invocation keeps
// its decoded name and the exact text that carries its arguments.
func statementReferenceFunctions(ctx context.Context, reference string) ([]statementCallableFact, error) {
	if reference == "" {
		return nil, nil
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, reference, cypherantlr.UseEntry, false, cypherantlr.AcceptedSource)
	if err != nil {
		return nil, fmt.Errorf("read the graph reference %q: %w", reference, err)
	}
	facts, err := statementFunctionFacts(syntax.Source(), syntax.Effects())
	if err != nil {
		return nil, err
	}
	return statementCallableFacts(facts), nil
}

// windows resolves the byte windows a USE spelling inserted. Every declared
// offset must own exactly the inserted reference bytes, in source order, so a
// spelling can never claim a window it did not insert.
func (reference statementGraphReference) windows(statement string) ([][2]int, error) {
	if reference.text == "" {
		if len(reference.insertions) != 0 {
			return nil, fmt.Errorf("graph reference has no text but %d insertions", len(reference.insertions))
		}
		return nil, nil
	}
	if len(reference.insertions) == 0 {
		return nil, fmt.Errorf("graph reference %q declares no insertion", reference.text)
	}
	windows := make([][2]int, 0, len(reference.insertions))
	previous := 0
	for _, start := range reference.insertions {
		end := start + len(reference.text)
		if start < previous || end > len(statement) || statement[start:end] != reference.text {
			return nil, fmt.Errorf("inserted reference %q is not at offset %d of %q", reference.text, start, statement)
		}
		windows = append(windows, [2]int{start, end})
		previous = end
	}
	return windows, nil
}

// statementScopesWithoutReference re-anchors the executable scopes of a USE
// spelling onto the statement body. A window is removed only from a scope whose
// canonical grammar window contains it, and only those bytes are removed: an
// occurrence of the same text inside a string literal, a comment or a
// pre-existing nested USE clause belongs to the body, so it is kept and every
// non-inserted byte of the scope is compared verbatim. A window no executable
// scope owns is an error, because that insertion would otherwise disappear from
// the comparison.
func statementScopesWithoutReference(scopes []statementScopeFact, windows [][2]int, inserted string) ([]string, error) {
	anchored := make([]string, len(scopes))
	owned := make([]bool, len(windows))
	for index, scope := range scopes {
		if scope.start < 0 || scope.end < scope.start || scope.end-scope.start != len(scope.text) {
			return nil, fmt.Errorf("executable scope %d window %d:%d does not own its %d bytes", index, scope.start, scope.end, len(scope.text))
		}
		text := scope.text
		removed := 0
		for window, span := range windows {
			if span[0] < scope.start || span[1] > scope.end {
				continue
			}
			start, end := span[0]-scope.start-removed, span[1]-scope.start-removed
			if end > len(text) || text[start:end] != inserted {
				return nil, fmt.Errorf("executable scope %d does not carry the inserted reference at offset %d", index, span[0])
			}
			text = text[:start] + text[end:]
			removed += span[1] - span[0]
			owned[window] = true
		}
		anchored[index] = text
	}
	for window, found := range owned {
		if !found {
			return nil, fmt.Errorf("inserted reference at offset %d is not owned by an executable scope", windows[window][0])
		}
	}
	return anchored, nil
}

// statementFunctionsByInsertion separates the scalar invocations a graph
// reference's own arguments retain from the invocations the statement body
// already owns. A site belongs to the inserted reference only when its canonical
// grammar window lies inside a window the spelling inserted, so body text that
// spells the same invocation in a nested USE clause stays body-owned.
func statementFunctionsByInsertion(facts []statementFunctionFact, windows [][2]int) (body, inserted []statementFunctionFact, err error) {
	for _, fact := range facts {
		owner := false
		for _, span := range windows {
			if fact.start >= span[0] && fact.end <= span[1] {
				owner = true
				continue
			}
			if fact.start < span[1] && fact.end > span[0] {
				return nil, nil, fmt.Errorf("scalar invocation %q crosses an inserted reference", fact.name)
			}
		}
		if owner {
			inserted = append(inserted, fact)
			continue
		}
		body = append(body, fact)
	}
	return body, inserted, nil
}

func statementScopeTexts(facts []statementScopeFact) []string {
	texts := make([]string, len(facts))
	for index, fact := range facts {
		texts[index] = fact.text
	}
	return texts
}

func statementCallableFacts(facts []statementFunctionFact) []statementCallableFact {
	callables := make([]statementCallableFact, len(facts))
	for index, fact := range facts {
		callables[index] = fact.statementCallableFact
	}
	return callables
}

// compareStatementContractFacts reports every semantic difference between a body
// contract and a spelling that adds a graph reference. reference is exactly what
// that spelling inserted and where: those byte windows are removed from the
// executable scopes they belong to and from the scalar invocations the reference
// owns, and the reference's own callables are read through the canonical USE
// grammar product. Every effect, routing, permission, image, unknown-procedure
// and state flag, and every retained callable, scope and argument fact, is
// compared.
func compareStatementContractFacts(ctx context.Context, want, got statementContractFacts, reference statementGraphReference) error {
	windows, windowErr := reference.windows(got.source)
	var differences []string
	report := func(name string, want, got any) {
		differences = append(differences, fmt.Sprintf("%s: want %#v, got %#v", name, want, got))
	}
	compareFlag := func(name string, want, got bool) {
		if want != got {
			report(name, want, got)
		}
	}
	compareCallables := func(name string, want, got []statementCallableFact) {
		if !slices.Equal(want, got) {
			report(name, want, got)
		}
	}
	compareFlag("writes", want.writes, got.writes)
	compareFlag("transaction", want.transaction, got.transaction)
	compareFlag("calls", want.calls, got.calls)
	compareFlag("dynamic", want.dynamic, got.dynamic)
	compareFlag("unknownProcedure", want.unknownProcedure, got.unknownProcedure)
	compareFlag("selectsDatabase", want.selectsDatabase, got.selectsDatabase)
	compareFlag("stateChange", want.stateChange, got.stateChange)
	if want.nodeImages != got.nodeImages {
		report("nodeImages", want.nodeImages, got.nodeImages)
	}
	if want.effect != got.effect {
		report("effect", want.effect, got.effect)
	}
	compareFlag("effects.writes", want.effects.writes, got.effects.writes)
	compareFlag("effects.readOnly", want.effects.readOnly, got.effects.readOnly)
	compareFlag("effects.remoteReadOnly", want.effects.remoteReadOnly, got.effects.remoteReadOnly)
	compareFlag("effects.allowsExecutableQueries", want.effects.allowsExecutableQueries, got.effects.allowsExecutableQueries)
	wantScopes := statementScopeTexts(want.effects.executableScopes)
	if windowErr != nil {
		report("effects.executableScopes", wantScopes, windowErr)
	} else if gotScopes, err := statementScopesWithoutReference(got.effects.executableScopes, windows, reference.text); err != nil {
		report("effects.executableScopes", wantScopes, err)
	} else if !slices.Equal(wantScopes, gotScopes) {
		report("effects.executableScopes", wantScopes, gotScopes)
	}
	compareCallables("effects.executableCalls", want.effects.executableCalls, got.effects.executableCalls)
	compareCallables("effects.calls", want.effects.calls, got.effects.calls)
	// The body's scalar invocations must be unchanged, in order. The graph
	// reference's own argument invocations are a routing product of this
	// spelling: their grammar windows are re-anchored above, and here their
	// decoded name and exact argument text are compared against the canonical
	// USE grammar product that supplied the reference.
	wantBody, wantReference := splitFunctionFacts(want.effects.functions)
	gotBody, gotReference := splitFunctionFacts(got.effects.functions)
	compareCallables("effects.functions", statementCallableFacts(wantBody), statementCallableFacts(gotBody))
	if windowErr != nil {
		report("effects.referenceFunctions", "insertion windows", windowErr)
	} else if bodyOwned, inserted, err := statementFunctionsByInsertion(gotReference, windows); err != nil {
		report("effects.referenceFunctions", "insertion-owned scalar invocations", err)
	} else {
		compareCallables("effects.referenceFunctions", statementCallableFacts(wantReference), statementCallableFacts(bodyOwned))
		canonical, err := statementReferenceFunctions(ctx, reference.text)
		if err != nil {
			report("effects.insertedFunctions", "canonical graph reference", err)
		} else {
			expected := make([]statementCallableFact, 0, len(canonical)*len(windows))
			for range windows {
				expected = append(expected, canonical...)
			}
			compareCallables("effects.insertedFunctions", expected, statementCallableFacts(inserted))
		}
	}
	if len(differences) == 0 {
		return nil
	}
	return fmt.Errorf("statement contract facts differ: %s", strings.Join(differences, "; "))
}

// statementInsideGraphReference reports whether the grammar places this site
// inside a USE clause, i.e. in the graph reference a routing consumer owns.
func statementInsideGraphReference(site cypherantlr.SyntaxNode) bool {
	for parent, ok := site.Parent(); ok; parent, ok = parent.Parent() {
		if parent.Rule() == cypherantlr.CypherParserRULE_useClause {
			return true
		}
	}
	return false
}

// splitFunctionFacts separates the statement body's scalar invocations from the
// graph reference's argument invocations.
func splitFunctionFacts(facts []statementFunctionFact) (body, reference []statementFunctionFact) {
	for _, fact := range facts {
		if fact.graphReference {
			reference = append(reference, fact)
			continue
		}
		body = append(body, fact)
	}
	return body, reference
}
