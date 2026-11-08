package cypher

import (
	"context"
	"errors"
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

var errNativeFunctionFound = errors.New("native function found")

// callsNativeFunction reports whether text calls a plugin function outside the
// bundled APOC library (nativePluginFunction), with the shared function-call
// scanner. A scan the caller's context stops counts as a call, so the
// statement keeps full node reads.
func callsNativeFunction(ctx context.Context, text string) bool {
	if PluginFunctionLookup == nil {
		return false
	}
	err := forEachFunctionInvocation(ctx, text, func(name, _ string, _ int) error {
		if handler, found := pluginFunctionHandler(name); found && nativePluginFunction(handler) {
			return errNativeFunctionFound
		}
		return nil
	})
	return err != nil
}

// callSubqueryRunsSeparately reports whether text has a CALL subquery that
// runs IN TRANSACTIONS or selects a graph with USE in its body: that part of
// the statement runs on an executor of its own (transaction_batch.go,
// pipeline_call_operator.go). A USE that selects the graph of the whole
// statement doesn't count. It reads the statement with the effect owner's
// grammar and arguments (readStatementExecutionContract), so the parse is the
// cached one; text it cannot read counts as such a subquery. The keyword
// tokens are only a cheap hint (a property or label may be named after them);
// the grammar-owned clauses decide.
func callSubqueryRunsSeparately(ctx context.Context, text string) bool {
	_, body := parseExecutionMode(text)
	structure, err := cypherantlr.ParseSyntax(ctx, body, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return true
	}
	for i := 0; i < structure.TokenCount(); i++ {
		if kind := structure.Token(i).Kind(); kind != cypherantlr.CypherParserUSE && kind != cypherantlr.CypherParserTRANSACTIONS {
			continue
		}
		found, depth := false, 0
		structure.Walk(func(node cypherantlr.SyntaxNode) func() {
			switch node.Rule() {
			case cypherantlr.CypherParserRULE_transactionBatch:
				found = true
			case cypherantlr.CypherParserRULE_useClause:
				found = found || depth > 0
			case cypherantlr.CypherParserRULE_callSubquery:
				depth++
				return func() { depth-- }
			}
			return nil
		})
		return found
	}
	return false
}
