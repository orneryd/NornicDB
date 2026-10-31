package antlr

// StatementEffects is the grammar-owned immutable effect view retained with a
// StatementSyntax. It caches structural facts only: procedure effects belong to
// the executing registry, which may differ between local and remote databases.
type StatementEffects struct{ data *statementSyntaxData }

type statementEffectsData struct {
	writes            bool
	merges            bool
	calls             []statementCallData
	remoteReadOnly    bool
	readOnly          bool
	executableQueries []int
}
type statementCallData struct {
	node     int
	name     string
	executes bool
}

// StatementCall identifies one grammar-owned procedure invocation.
type StatementCall struct {
	data *statementSyntaxData
	call statementCallData
}

// Name is the decoded qualified procedure name, without resolving a registry.
func (call StatementCall) Name() string { return call.call.name }

// Text is the original invocation source used by procedure policy consumers.
func (call StatementCall) Text() string {
	return (SyntaxNode{syntaxView[syntaxNode]{call.data, call.call.node}}).Text()
}

// Effects returns the retained structural effects, without another syntax walk.
func (syntax StatementSyntax) Effects() StatementEffects { return StatementEffects{syntax.data} }

// Writes reports an updating clause or command, independent of procedure modes.
func (effects StatementEffects) Writes() bool {
	return effects.data != nil && effects.data.effects.writes
}

// Merges reports a MERGE clause anywhere in the statement.
func (effects StatementEffects) Merges() bool {
	return effects.data != nil && effects.data.effects.merges
}

// CallCount returns the number of procedure invocations, including nested calls.
func (effects StatementEffects) CallCount() int {
	if effects.data == nil {
		return 0
	}
	return len(effects.data.effects.calls)
}

// Call returns an immutable invocation at index, in grammar traversal order.
func (effects StatementEffects) Call(index int) StatementCall {
	return StatementCall{effects.data, effects.data.effects.calls[index]}
}

// ReadOnly reports whether every executable scope is structurally read-only
// and every executable procedure call is accepted by the caller's live policy.
// EXPLAIN suppresses only its own scope; later statements are still checked.
func (effects StatementEffects) ReadOnly(allowCall func(StatementCall) bool) bool {
	if effects.data == nil || !effects.data.effects.readOnly {
		return false
	}
	for _, call := range effects.data.effects.calls {
		if call.executes && (allowCall == nil || !allowCall(StatementCall{effects.data, call})) {
			return false
		}
	}
	return true
}

// AllowsExecutableQueries applies live policy to every retained executable
// query scope. Explained scopes are omitted; scripts and nested queries retain
// their own original source windows. Policy results are never cached.
func (effects StatementEffects) AllowsExecutableQueries(allow func(string) bool) bool {
	if effects.data == nil || allow == nil {
		return false
	}
	for _, id := range effects.data.effects.executableQueries {
		if !allow((SyntaxNode{syntaxView[syntaxNode]{effects.data, id}}).Text()) {
			return false
		}
	}
	return true
}

// RemoteReadOnly proves a parsed query does not execute an update or opaque
// procedure call. EXPLAIN suppresses execution in its own query scope. Unknown
// syntax and non-query command roots never acquire this proof.
func (effects StatementEffects) RemoteReadOnly() bool {
	return effects.data != nil && effects.data.effects.remoteReadOnly
}

func (syntax StatementSyntax) collectEffects() statementEffectsData {
	result := statementEffectsData{}
	query, remoteEffect, nonReadEffect, explained := false, false, false, 0
	syntax.Walk(func(node SyntaxNode) func() {
		if node.Rule() == CypherParserRULE_query {
			hasBody, explains := false, false
			for i := 0; i < node.ChildCount(); i++ {
				child := node.Child(i)
				switch child.Rule() {
				case CypherParserRULE_queryPrefix:
					if _, ok := child.DirectToken(CypherParserEXPLAIN); ok {
						explains = true
					}
				case CypherParserRULE_regularQuery, CypherParserRULE_standaloneCall, CypherParserRULE_privilegeCommand, CypherParserRULE_schemaCommand, CypherParserRULE_administrationCommand, CypherParserRULE_showCommand, CypherParserRULE_terminateCommand:
					start, end := child.Span()
					hasBody = hasBody || end > start
				}
			}
			query = query || hasBody
			if !hasBody {
				nonReadEffect = true
				remoteEffect = true
			}
			if explains {
				explained++
				return func() { explained-- }
			}
			if explained == 0 && hasBody {
				result.executableQueries = append(result.executableQueries, node.id)
			}
		}
		if node.Rule() == CypherParserRULE_mergeSt {
			result.merges = true
		}
		switch node.Rule() {
		case CypherParserRULE_updatingStatement, CypherParserRULE_schemaCommand, CypherParserRULE_administrationCommand, CypherParserRULE_privilegeCommand, CypherParserRULE_terminateCommand:
			result.writes = true
			if explained == 0 {
				remoteEffect = true
				nonReadEffect = true
			}
		case CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall:
			result.calls = append(result.calls, statementCallData{node: node.id, name: node.ChildRule(CypherParserRULE_invocationName).QualifiedName(), executes: explained == 0})
			if explained == 0 {
				remoteEffect = true
			}
		case CypherParserRULE_shellCommand, CypherParserRULE_transactionStatement:
			remoteEffect = true
			nonReadEffect = true
		}
		return nil
	})
	result.readOnly = query && !nonReadEffect
	result.remoteReadOnly = query && !remoteEffect
	return result
}
