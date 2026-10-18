package fabric

// ImportReader is how Fabric reads what a CALL body imports from the outer
// row. Its one implementation is the Cypher executor's body-scope owner
// (bodyScopeOf), which this package can't import; the executor sets it on
// the planner and the executor it builds.
type ImportReader interface {
	// CallBodyImports reads an unscoped CALL body: reads, the outer names
	// it reads, and importing, its importing WITH list (nil when it has
	// none; then the body imports what it reads). err is an importing WITH
	// Neo4j rejects (WITH k AS kk, WITH k WHERE …) or a body the static
	// checks reject.
	CallBodyImports(body string) (reads, importing []string, err error)
	// ExpressionReads reports whether expression reads variable.
	ExpressionReads(expression, variable string) bool
}

// WithImportReader sets the reader the planner reads CALL body imports with.
func (p *FabricPlanner) WithImportReader(reader ImportReader) *FabricPlanner {
	p.imports = reader
	return p
}

// SetImportReader sets the reader the executor reads correlated imports
// with.
func (e *FabricExecutor) SetImportReader(reader ImportReader) {
	e.imports = reader
}

// callImports reads a CALL block's body (bodyScopeOf through the reader):
// its importing WITH columns, and the outer names the inner fragment is
// given (selects: only those; false: every outer column, for a body with an
// importing WITH, which selects from them itself).
func (p *FabricPlanner) callImports(body string) (importing, imports []string, selects bool, err error) {
	if p.imports == nil {
		return nil, nil, false, nil
	}
	reads, importing, err := p.imports.CallBodyImports(body)
	if err != nil {
		return nil, nil, false, err
	}
	if importing != nil {
		return importing, nil, false, nil
	}
	return nil, reads, true, nil
}

// selectBindings is the bindings named in names.
func selectBindings(bindings map[string]interface{}, names []string) map[string]interface{} {
	selected := make(map[string]interface{}, len(names))
	for _, name := range names {
		if value, ok := bindings[name]; ok {
			selected[name] = value
		}
	}
	return selected
}

// expressionReads reports whether expression reads variable (ImportReader).
// Without a reader nothing is known, and it answers true: a batched lookup
// that needs a term not to read the import is declined for the per-row
// apply.
func (e *FabricExecutor) expressionReads(expression, variable string) bool {
	if e.imports == nil {
		return true
	}
	return e.imports.ExpressionReads(expression, variable)
}
