package tck

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

var procedureSignaturePattern = regexp.MustCompile(`^\s*([^\s(]+)\s*\((.*)\)\s*::\s*\((.*)\)\s*$`)

// RegisterProcedure installs a scenario-local TCK procedure in the production
// Cypher registry. Reset clears it before the next scenario.
func (b *BoltBackend) RegisterProcedure(_ context.Context, signature string, rows [][]string) error {
	spec, fixture, err := parseProcedureFixture(signature, rows)
	if err != nil {
		return err
	}
	return cypher.RegisterUserProcedure(spec, func(_ context.Context, _ *cypher.StorageExecutor, _ string, args []interface{}) (*cypher.ExecuteResult, error) {
		result := &cypher.ExecuteResult{Columns: make([]string, len(spec.Returns)), Rows: [][]interface{}{}}
		for i, column := range spec.Returns {
			result.Columns[i] = column.Name
		}
		for _, row := range fixture {
			matched := true
			for i := range spec.Params {
				if i >= len(args) || !reflect.DeepEqual(row[i], args[i]) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			output := make([]interface{}, len(spec.Returns))
			copy(output, row[len(spec.Params):])
			result.Rows = append(result.Rows, output)
		}
		return result, nil
	})
}

func parseProcedureFixture(signature string, rows [][]string) (cypher.ProcedureSpec, [][]interface{}, error) {
	match := procedureSignaturePattern.FindStringSubmatch(signature)
	if match == nil {
		return cypher.ProcedureSpec{}, nil, fmt.Errorf("invalid procedure signature %q", signature)
	}
	params, err := parseProcedureParams(match[2])
	if err != nil {
		return cypher.ProcedureSpec{}, nil, err
	}
	returns, err := parseProcedureColumns(match[3])
	if err != nil {
		return cypher.ProcedureSpec{}, nil, err
	}
	spec := cypher.ProcedureSpec{
		Name: match[1], Signature: strings.TrimSpace(signature), Mode: cypher.ProcedureModeRead,
		Params: params, Returns: returns, MinArgs: len(params), MaxArgs: len(params),
	}
	width := len(params) + len(returns)
	if len(rows) == 0 {
		return spec, nil, fmt.Errorf("procedure fixture %q has no header", signature)
	}
	if len(rows[0]) != width {
		return spec, nil, fmt.Errorf("procedure fixture header has width %d, want %d", len(rows[0]), width)
	}
	fixture := make([][]interface{}, 0, len(rows)-1)
	for rowIndex, row := range rows[1:] {
		if len(row) != width {
			return spec, nil, fmt.Errorf("procedure fixture row %d has width %d, want %d", rowIndex+1, len(row), width)
		}
		values := make([]interface{}, width)
		for columnIndex, raw := range row {
			value, parseErr := ParseValue(raw)
			if parseErr != nil {
				return spec, nil, fmt.Errorf("parse procedure fixture row %d column %d: %w", rowIndex+1, columnIndex, parseErr)
			}
			values[columnIndex] = value
		}
		fixture = append(fixture, values)
	}
	return spec, fixture, nil
}

func parseProcedureParams(input string) ([]cypher.ProcedureParam, error) {
	parts := splitProcedureFields(input)
	params := make([]cypher.ProcedureParam, 0, len(parts))
	for _, part := range parts {
		name, typeName, optional, err := parseProcedureField(part)
		if err != nil {
			return nil, err
		}
		params = append(params, cypher.ProcedureParam{Name: name, Type: typeName, Optional: optional})
	}
	return params, nil
}

func parseProcedureColumns(input string) ([]cypher.ProcedureColumn, error) {
	parts := splitProcedureFields(input)
	columns := make([]cypher.ProcedureColumn, 0, len(parts))
	for _, part := range parts {
		name, typeName, _, err := parseProcedureField(part)
		if err != nil {
			return nil, err
		}
		columns = append(columns, cypher.ProcedureColumn{Name: name, Type: typeName})
	}
	return columns, nil
}

func splitProcedureFields(input string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func parseProcedureField(input string) (name, typeName string, optional bool, err error) {
	parts := strings.SplitN(input, "::", 2)
	if len(parts) != 2 {
		return "", "", false, fmt.Errorf("invalid procedure field %q", input)
	}
	name = strings.TrimSpace(parts[0])
	typeName = strings.TrimSpace(parts[1])
	optional = strings.HasSuffix(typeName, "?")
	typeName = strings.TrimSuffix(typeName, "?")
	if name == "" || typeName == "" {
		return "", "", false, fmt.Errorf("invalid procedure field %q", input)
	}
	return name, typeName, optional, nil
}
