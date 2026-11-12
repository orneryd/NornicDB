package cypher

import (
	"context"
	"sort"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// bundledAPOCSymbolPrefix is the Go symbol prefix of the bundled APOC
// library's function handlers.
const bundledAPOCSymbolPrefix = "github.com/orneryd/nornicdb/apoc/"

// nativePluginFunction reports a plugin function outside the bundled APOC
// library: native code that may read the stored vectors of a node it is given.
func nativePluginFunction(function *PluginFunctionHandler) bool {
	return function != nil && !strings.HasPrefix(function.Symbol(), bundledAPOCSymbolPrefix)
}

// nativeFunctionInputs is the scoped type walk's immutable proof for retained
// invocation sites. A missing site requires full images, never an inferred
// scalar result. Source and original spans distinguish equal text in different
// bindings and different constituent fragments.
type nativeFunctionInputs struct {
	generation uint64
	uncertain  bool
	sites      map[nativeInputSite]bool // true when an argument may contain an entity
}

type nativeInputSite struct {
	source     string
	start, end int
}
type nativeFunctionInputsKey struct{}

func nativeInputSiteFor(node cypherantlr.SyntaxNode) nativeInputSite {
	start, end := node.Span()
	return nativeInputSite{node.Source(), start, end}
}

func (inputs *nativeFunctionInputs) merge(other nativeFunctionInputs) {
	inputs.uncertain = inputs.uncertain || other.uncertain || inputs.generation != other.generation
	if len(other.sites) == 0 {
		return
	}
	if inputs.sites == nil {
		inputs.sites = make(map[nativeInputSite]bool, len(other.sites))
	}
	for site, reaches := range other.sites {
		inputs.sites[site] = inputs.sites[site] || reaches
	}
}

// recordNativeFunctionInputs reads the existing grammar's sorted invocation
// sites and the semantic walk's actual clause scope. Nested clauses belong to
// their own scope; local expression binders conservatively keep full images.
func recordNativeFunctionInputs(ctx context.Context, clause pipelineClause, scope staticTypeScope, projected func() staticTypeScope) error {
	inputs, _ := ctx.Value(nativeFunctionInputsKey{}).(*nativeFunctionInputs)
	if inputs == nil || !clause.syntax.Valid() {
		return nil
	}
	effects := clause.syntax.Statement().Effects()
	start, end := clause.syntax.Span()
	first := sort.Search(effects.FunctionCount(), func(index int) bool {
		at, _ := effects.Function(index).Span()
		return at >= start
	})
	var ordinary, afterProjection *staticOperatorChecker
	for index := first; index < effects.FunctionCount(); index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		call := effects.Function(index)
		at, _ := call.Span()
		if at >= end {
			break
		}
		owned, local, after := false, false, false
		for parent, ok := call.Parent(); ok; parent, ok = parent.Parent() {
			if _, isClause := querySyntaxClauseKind(parent); isClause {
				owned = parent.ID() == clause.syntax.ID()
				break
			}
			switch parent.Rule() {
			case cypherantlr.CypherParserRULE_listComprehension, cypherantlr.CypherParserRULE_patternComprehension,
				cypherantlr.CypherParserRULE_filterWith, cypherantlr.CypherParserRULE_reduceExpression:
				local = true
			case cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt, cypherantlr.CypherParserRULE_where:
				after = projected != nil
			case cypherantlr.CypherParserRULE_functionInvocation:
				if name := parent.ChildRule(cypherantlr.CypherParserRULE_invocationName); name.Valid() {
					local = local || isQuantifierOrReduceFunction(lowerASCII(name.QualifiedName()))
				}
			}
		}
		if !owned {
			continue
		}
		reaches := local
		arguments := call.ChildRule(cypherantlr.CypherParserRULE_expressionChain)
		if !local && arguments.Valid() {
			checker := &ordinary
			if after {
				checker = &afterProjection
			}
			if *checker == nil {
				selected := scope
				if after {
					selected = projected()
				}
				*checker = &staticOperatorChecker{scope: selected, checked: make(map[string]staticCheckResult)}
			}
			for arg := 0; arg < arguments.ChildCount(); arg++ {
				expression := arguments.Child(arg)
				if expression.Rule() != cypherantlr.CypherParserRULE_expression {
					continue
				}
				operand, err := (*checker).check(ctx, expression.Code())
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil || operand.mayReachGraphEntity() {
					reaches = true
					break
				}
			}
		}
		if inputs.sites == nil {
			inputs.sites = make(map[nativeInputSite]bool)
		}
		site := nativeInputSiteFor(call)
		inputs.sites[site] = inputs.sites[site] || reaches
	}
	return nil
}
