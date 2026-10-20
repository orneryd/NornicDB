package cypher

import (
	"strings"
	"sync"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// semanticNativeInput is an admission-lifetime view. It is never published in
// a scope recording or retained in an executor's semantic verdict cache.
type semanticNativeInput struct {
	input  fabric.ExpressionInput
	names  *quotedVariableNames
	text   string
	scopes []fabric.ExpressionScope
	once   sync.Once
}

func (input *semanticNativeInput) localScopes() []fabric.ExpressionScope {
	input.once.Do(func() { input.scopes = input.input.Scopes() })
	return input.scopes
}

func newSemanticNativeInput(input fabric.ExpressionInput) *semanticNativeInput {
	if !input.Valid() {
		return nil
	}
	return &semanticNativeInput{input: input, text: input.Text()}
}

func (scope staticTypeScope) withNativeInput(input fabric.ExpressionInput) staticTypeScope {
	names := scope.nativeNames()
	scope.noLocalScopes = false
	scope.native = newSemanticNativeInput(input)
	if scope.native != nil {
		scope.native.names = names
	}
	return scope
}

func (checker staticOperatorChecker) rootBindingScope(expression string) (fabric.ExpressionScope, bool) {
	if !fabric.QuerySyntaxDemand(expression).Has(fabric.NativeLocalScopes) {
		var absent fabric.ExpressionScope
		return absent, false
	}
	if checker.scope.native != nil {
		if len(checker.scope.native.localScopes()) == 0 {
			var absent fabric.ExpressionScope
			return absent, false
		}
		if checker.scope.native.text == expression {
			return checker.scope.native.input.RootScope()
		}
	}
	return fabric.RootExpressionScope(expression)
}

func (checker staticOperatorChecker) checkRange(text string, start, end int) (staticOperand, error) {
	if checker.scope.native != nil && checker.scope.native.text == text {
		checker.scope = checker.scope.nativeRange(text, start, end)
	} else {
		checker.scope.native = nil
	}
	return checker.check(text[start:end])
}

func (checker staticOperatorChecker) trimNativeInput(text string) staticOperatorChecker {
	if checker.scope.native == nil || checker.scope.native.text != text {
		return checker
	}
	start := len(text) - len(strings.TrimLeftFunc(text, unicode.IsSpace))
	end := len(strings.TrimRightFunc(text, unicode.IsSpace))
	if start != 0 || end != len(text) {
		checker.scope = checker.scope.withNativeInput(checker.scope.native.input.Slice(start, end))
		checker.scope.native.text = text[start:end]
	}
	return checker
}

func (scope staticTypeScope) nativeScopes(text string) []fabric.ExpressionScope {
	if scope.noLocalScopes {
		return nil
	}
	if scope.native != nil && (len(scope.native.localScopes()) == 0 || scope.native.text == text) {
		return scope.native.localScopes()
	}
	return fabric.ExpressionScopes(text)
}

func (scope staticTypeScope) nativeScopesAt(text string, at int) []fabric.ExpressionScope {
	if scope.noLocalScopes {
		return nil
	}
	if scope.native != nil {
		if len(scope.native.localScopes()) == 0 {
			return nil
		}
		if scope.native.text == text {
			return scope.native.input.ScopesAt(at)
		}
	}
	return fabric.ExpressionScopes(text)
}

func nativeLocalNames(scopes []fabric.ExpressionScope, queryLevel bool) expressionLocalNameSet {
	locals := make(expressionLocalNameSet)
	for _, scope := range scopes {
		if queryLevel && scope.QueryDepth != 0 {
			continue
		}
		for _, declaration := range scope.Declarations {
			if !declaration.Pattern && !declaration.Query {
				locals[declaration.Name] = struct{}{}
			}
		}
	}
	return locals
}

func localBindingIn(scopes []fabric.ExpressionScope, operand int, name string) (localBinding, int, bool) {
	var found localBinding
	foundIndex, foundOpen, present := 0, -1, false
	for _, scope := range scopes {
		if scope.Open >= operand || scope.Open < foundOpen {
			continue
		}
		binding := localBindingFromNative(scope)
		for index := 0; index < binding.count; index++ {
			if binding.names[index] == name && (binding.declared[index] == operand || binding.scope <= operand && operand < binding.close) {
				found, foundIndex, foundOpen, present = binding, index, scope.Open, true
			}
		}
	}
	return found, foundIndex, present
}

// nativeRange retains the exact parser-derived parent interval. It does not
// search for text, so identical operands in different scopes stay distinct.
func (scope staticTypeScope) nativeRange(text string, start, end int) staticTypeScope {
	if scope.native != nil && scope.native.text == text {
		scope = scope.withNativeInput(scope.native.input.Slice(start, end))
		scope.native.text = text[start:end]
		return (scope.operatorChecker()).trimNativeInput(text[start:end]).scope
	}
	scope.native = nil
	return scope
}

func (checker staticOperatorChecker) childOperand(text string, index int) staticOperatorChecker {
	if checker.scope.native == nil {
		return checker
	}
	children := checker.scope.native.input.ChildExpressions()
	if index < len(children) && children[index].Text() == strings.TrimSpace(text) {
		checker.scope = checker.scope.withNativeInput(children[index])
	} else {
		checker.scope.native = nil
	}
	return checker
}

// withNativeMask preserves the source owner when an existing semantic phase
// blanks already-admitted operands. Only that phase supplies the mask; matching
// text, cache keys or coincidentally equal lengths never discover provenance.
func (scope staticTypeScope) withNativeMask(text string) staticTypeScope {
	if scope.native != nil {
		scope = scope.withNativeInput(scope.native.input)
		scope.native.text = text
	}
	return scope
}

// withClauseInput consumes the statement admission's parser decision. An
// ordinary clause whose complete operand population has no local syntax
// carries no native source handle that a later helper could resolve eagerly.
func (scope staticTypeScope) withClauseInput(clause pipelineClause) staticTypeScope {
	if clause.noLocalScopes {
		scope.native, scope.noLocalScopes = nil, true
		return scope
	}
	return scope.withNativeInput(clause.input)
}

func (scope staticTypeScope) nativeNames() *quotedVariableNames {
	if scope.native != nil {
		return scope.native.names
	}
	return nil
}
