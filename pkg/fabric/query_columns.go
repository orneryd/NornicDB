package fabric

// ProjectionColumns is the schema declared by projection syntax; a star waits
// for runtime scope. Alias values, variable names and expression text retain
// their distinct Cypher naming rules.
func ProjectionColumns(p *QueryProjection) []string {
	if p == nil || p.Star {
		return nil
	}
	columns := make([]string, 0, len(p.Items))
	for _, item := range p.Items {
		columns = append(columns, item.Column)
	}
	return columns
}

// ResultClause owns the public projection and its source span. UNION uses its
// first branch's schema, including when names are restored after rewriting.
func (q *QueryStructure) ResultClause() *QueryClause {
	var last *QueryClause
	for i := range q.Clauses {
		clause := &q.Clauses[i]
		if clause.Kind == "UNION" {
			break
		}
		if clause.Kind == "RETURN" {
			last = clause
		}
	}
	return last
}

// ResultColumns selects the declared public projection. A star remains owned
// by runtime scope.
func (q *QueryStructure) ResultColumns() []string {
	clause := q.ResultClause()
	if clause == nil {
		return nil
	}
	return ProjectionColumns(clause.Projection)
}

// YieldClause selects an explicit declaration in the first UNION branch.
// Nested CALLs retain their own structure and cannot export declarations here.
func (q *QueryStructure) YieldClause() *QueryYield {
	for i := range q.Clauses {
		clause := &q.Clauses[i]
		if clause.Kind == "UNION" {
			break
		}
		if clause.Yield != nil {
			return clause.Yield
		}
	}
	return nil
}

// DeclaredColumns reports only native syntax declarations. Relative bodies may
// lack their caller's imports; a star never initiates semantic admission here.
func (q *QueryStructure) DeclaredColumns() []string {
	if columns := q.ResultColumns(); columns != nil {
		return columns
	}
	if y := q.YieldClause(); y != nil && !y.Star {
		columns := make([]string, 0, len(y.Items))
		for _, item := range y.Items {
			name := item.Name
			if item.Alias != "" {
				name = item.Alias
			}
			columns = append(columns, name)
		}
		return columns
	}
	return nil
}

// ValidateUnions checks source schemas before any rendering/normalization can
// change expression column names. Runtime metadata supplies unknown star and
// procedure schemas through the same UnionColumnOrder contract.
func (q *QueryStructure) ValidateUnions() error {
	if q.Err != nil {
		return q.Err
	}
	return q.unionErr
}

// Static schemas belong to the immutable syntax result, not each execution.
func (q *QueryStructure) validateUnionSchemas() error {
	if q.Err != nil {
		return q.Err
	}
	var schemas [][]string
	var projection *QueryProjection
	union := false
	for _, clause := range q.Clauses {
		if clause.Nested != nil {
			if err := clause.Nested.ValidateUnions(); err != nil {
				return err
			}
		}
		if clause.Kind == "RETURN" {
			projection = clause.Projection
		}
		if clause.Kind == "UNION" {
			union = true
			schemas = append(schemas, ProjectionColumns(projection))
			projection = nil
		}
	}
	if !union {
		return nil
	}
	schemas = append(schemas, ProjectionColumns(projection))
	var expected []string
	for _, columns := range schemas {
		if columns == nil {
			continue
		}
		if expected == nil {
			expected = columns
			continue
		}
		if _, same := UnionColumnOrder(expected, columns); !same {
			return &UnionColumnsError{Message: UnionColumnMismatchMessage(expected, columns)}
		}
	}
	return nil
}
