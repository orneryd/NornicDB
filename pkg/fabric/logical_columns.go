package fabric

import "github.com/orneryd/nornicdb/pkg/localization"

// ColumnResolver is the native compiler's schema projection. Fabric does not
// reconstruct variable scope from MATCH, CALL, WITH or procedure syntax.
type ColumnResolver func(query string, inputs []string, owner string) (*LogicalFrame, error)

// LogicalFrame is the compiler's immutable scope/type descriptor. Types remain
// opaque here; only the native type owner interprets them.
type LogicalFrame struct {
	Inputs, Globals, Columns []string
	InputTypes, OutputTypes  interface{}
}

func (p *FabricPlanner) SetColumnResolver(resolve ColumnResolver) { p.resolveColumns = resolve }

// resolveLogicalColumns runs once while constructing the immutable plan. Empty
// row streams retain the same schema as executed streams, including RETURN *.
func (p *FabricPlanner) resolveLogicalColumns(fragment Fragment, inputs []string, owner string) ([]string, error) {
	switch f := fragment.(type) {
	case *FragmentInit:
		if f.logicalQuery != "" {
			frame, err := p.resolveColumns(f.logicalQuery, inputs, owner)
			if err != nil {
				return nil, err
			}
			f.Columns = frame.Inputs
		} else if f.ImportColumns != nil {
			f.Columns = f.ImportColumns
		} else {
			f.Columns = inputs
		}
		return f.Columns, nil
	case *FragmentExec:
		frame, err := p.resolveColumns(f.Query, inputs, owner)
		if err != nil {
			return nil, err
		}
		if frame == nil || frame.InputTypes == nil || frame.OutputTypes == nil {
			return nil, &QuerySyntaxError{Message: localization.CypherCommandRoutingFabricPreparedContextInvalid()}
		}
		f.Frame = frame
		f.Columns = frame.Columns
		return f.Columns, nil
	case *FragmentApply:
		if !f.Projection && (f.Call == nil || f.Call.Nested == nil) {
			return nil, &QuerySyntaxError{Message: localization.CypherCommandRoutingFabricPreparedContextInvalid()}
		}
		outer, err := p.resolveLogicalColumns(f.Input, inputs, owner)
		if err != nil {
			return nil, err
		}
		innerOwner := owner
		if f.Call != nil {
			innerOwner = f.Call.ExecutionText
		}
		// The native compilation supplies each branch's inputs and retained
		// imports. Fabric passes available columns only to identify that frame.
		inner, err := p.resolveLogicalColumns(f.Inner, outer, innerOwner)
		if err != nil {
			return nil, err
		}
		if f.Projection {
			f.Columns = inner
		} else if f.unit() {
			f.Columns = outer
		} else {
			f.Columns = combineColumns(outer, inner)
		}
		return f.Columns, nil
	case *FragmentUnion:
		left, err := p.resolveLogicalColumns(f.LHS, inputs, owner)
		if err != nil {
			return nil, err
		}
		if _, err = p.resolveLogicalColumns(f.RHS, inputs, owner); err != nil {
			return nil, err
		}
		f.Columns = left
		return left, nil
	case *FragmentLeaf:
		return f.Columns, nil
	}
	return nil, nil
}
