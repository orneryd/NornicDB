package fgac

import (
	"fmt"
	"reflect"
	"strings"
)

// Outcome is one normalized Bolt result or Neo4j error.
type Outcome struct {
	Columns []string
	Rows    [][]any
	Code    string
	Message string
}

// Denied reports whether the outcome is a Neo4j client error.
func (o Outcome) Denied() bool {
	return o.Code != ""
}

// CodeMatches compares an actual Neo4j error code with an expectation. A
// trailing dot in the expected code matches a documented error family.
func CodeMatches(actual, expected string) bool {
	if strings.HasSuffix(expected, ".") {
		return strings.HasPrefix(actual, expected)
	}
	return actual == expected
}

// RowsEqual compares normalized result rows.
func RowsEqual(left, right [][]any) bool {
	return reflect.DeepEqual(left, right)
}

// ColumnsEqual compares result column names.
func ColumnsEqual(left, right []string) bool {
	return reflect.DeepEqual(left, right)
}

// CheckExpectation applies one documented expectation to an outcome.
func CheckExpectation(expectation Expectation, outcome Outcome) error {
	switch expectation.Kind {
	case "allow":
		if outcome.Denied() {
			return fmt.Errorf("expected rows but got %s: %s", outcome.Code, outcome.Message)
		}
		if !RowsEqual(expectation.Rows, outcome.Rows) {
			return fmt.Errorf("rows differ: expected %v, got %v", expectation.Rows, outcome.Rows)
		}
	case "deny":
		if !outcome.Denied() {
			return fmt.Errorf("expected %s but the statement succeeded with rows %v", expectation.Code, outcome.Rows)
		}
		if !CodeMatches(outcome.Code, expectation.Code) {
			return fmt.Errorf("expected %s, got %s: %s", expectation.Code, outcome.Code, outcome.Message)
		}
	default:
		return fmt.Errorf("unknown expectation kind %q", expectation.Kind)
	}
	return nil
}

// CompareOutcomes checks that a candidate outcome matches the reference
// outcome for the same documented case.
func CompareOutcomes(reference, candidate Outcome) error {
	if reference.Denied() || candidate.Denied() {
		if reference.Code != candidate.Code {
			return fmt.Errorf("error code differs: reference %s, candidate %s", reference.Code, candidate.Code)
		}
		return nil
	}
	if !ColumnsEqual(reference.Columns, candidate.Columns) {
		return fmt.Errorf("columns differ: reference %v, candidate %v", reference.Columns, candidate.Columns)
	}
	if !RowsEqual(reference.Rows, candidate.Rows) {
		return fmt.Errorf("rows differ: reference %v, candidate %v", reference.Rows, candidate.Rows)
	}
	return nil
}
