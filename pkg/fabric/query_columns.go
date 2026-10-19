package fabric

import "fmt"

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

// ResultColumns selects the declared public projection. UNION uses its first
// branch's schema; a star remains owned by runtime scope.
func (q *QueryStructure) ResultColumns() []string {
	var first, last *QueryProjection
	union := false
	for _, clause := range q.Clauses {
		if clause.Kind == "RETURN" {
			if first == nil {
				first = clause.Projection
			}
			last = clause.Projection
		}
		if clause.Kind == "UNION" {
			union = true
		}
	}
	if union {
		return ProjectionColumns(first)
	}
	return ProjectionColumns(last)
}

// ValidateUnions checks source schemas before any rendering/normalization can
// change expression column names. Runtime metadata supplies unknown star and
// procedure schemas through the same UnionColumnOrder contract.
func (q *QueryStructure) ValidateUnions() error {
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
			return fmt.Errorf("UNION queries must return the same columns (got %v and %v)", expected, columns)
		}
	}
	return nil
}
