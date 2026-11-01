// Package statementdiagtest supplies bounded statement/diagnostic contracts to
// engine and transport tests. It never selects cases with a parser under test.
package statementdiagtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

const (
	SyntaxError   = "Neo.ClientError.Statement.SyntaxError"
	ArgumentError = "Neo.ClientError.Statement.ArgumentError"
)

type Case struct {
	Name, Query string
	// Empty denotes success. Rejection classes belong to each contract,
	// including argument admission before grammar, rather than one blanket
	// syntax class imposed by the transport observer.
	ExpectedStatus string
}

// Cases cover the separate producer contracts found in the typed inventory:
// source admission, grammar, early shared checks, binding, extensions and
// prepared language. Every input is bounded and safe on a fresh empty store.
func Cases() []Case {
	return []Case{
		{"grammar-token", "RETURN 1 @", SyntaxError},
		{"grammar-eof", "MATCH (n) RETURN n,", SyntaxError},
		{"grammar-delimiter", "MATCH (n RETURN n", SyntaxError},
		{"alias", "UNWIND [1, 2] AS x y RETURN x", SyntaxError},
		{"clause-order", "UNWIND [1, 2] AS x WHERE x > 1 RETURN x", SyntaxError},
		{"call-import", "CALL (1) { RETURN 1 AS value } RETURN value", SyntaxError},
		{"call-nested-import", "CALL () { CALL (1) { RETURN 1 AS value } RETURN value } RETURN value", SyntaxError},
		{"call-expression", "RETURN EXISTS { RETURN 1 + } AS value", SyntaxError},
		{"binding", "RETURN undefinedDiagnosticVariable", SyntaxError},
		{"function", "RETURN undefinedDiagnosticFunction(1)", SyntaxError},
		{"static-map", "RETURN {1: 'value'}", SyntaxError},
		{"list-comma", "RETURN [1,]", SyntaxError},
		{"adjacent-operands", "RETURN 1 'value'", SyntaxError},
		{"not-in", "RETURN 1 NOT IN [1]", SyntaxError},
		{"source-escape", `RETURN '\u00ZZ'`, SyntaxError},
		{"label-policy", "MATCH (n) WHERE n IS NOT Code RETURN count(n)", SyntaxError},
		{"extension", "SHOW LIMITS YIELD name WHERE name IN ['a',]", SyntaxError},
		{"catalog", "CREATE CONSTRAINT", SyntaxError},
		{"header-conflict", "CYPHER 5 CYPHER 25 RETURN 1", ArgumentError},
		{"constant", "RETURN 1 AS value", ""},
		{"unwind", "UNWIND [1, 2] AS value RETURN value", ""},
		{"shared-reading-clauses", "LET value = 2 FILTER value > 1 RETURN value", ""},
		{"shared-iteration", "FOR value IN [1, 2] RETURN value", ""},
		{"prepared-label-expression", "CYPHER 25 MATCH (n) WHERE n IS LABELED Known RETURN n", ""},
		{"prepared-composition", "CALL () { WHEN true THEN RETURN 1 AS value } RETURN value", ""},
		{"prepared-projection", "CYPHER 25 LET value = 1 CALL db.labels() YIELD label RETURN *", ""},
	}
}

func Prefixes() []string {
	return []string{"", "EXPLAIN ", "PROFILE ", "CYPHER 5 ", "CYPHER 5 EXPLAIN ", "CYPHER 5 PROFILE ", "CYPHER 25 ", "CYPHER 25 EXPLAIN ", "CYPHER 25 PROFILE "}
}

// TransportCases exercise public encoders at the distinct admission boundaries,
// plus accepted statements to detect over-rejection.
func TransportCases() []Case {
	var cases []Case
	for _, item := range Cases() {
		switch item.Name {
		case "grammar-token", "alias", "call-import", "binding", "static-map", "source-escape", "label-policy", "header-conflict", "constant", "shared-reading-clauses", "prepared-label-expression":
			cases = append(cases, item)
		}
	}
	return cases
}

func Forms(item Case) []string {
	if strings.HasPrefix(item.Query, "CYPHER 25 ") {
		body := strings.TrimPrefix(item.Query, "CYPHER 25 ")
		return []string{item.Query, "CYPHER 25 EXPLAIN " + body, "CYPHER 25 PROFILE " + body, "CYPHER 25 PROFILE " + item.Query}
	}
	forms := make([]string, 0, len(Prefixes()))
	for _, prefix := range Prefixes() {
		forms = append(forms, prefix+item.Query)
	}
	return forms
}

// RoutedForms add the readers which continue a statement in a source window.
// A dialect header belongs to the outer statement, never an inline CALL body.
func RoutedForms(item Case, database string) []string {
	forms := Forms(item)
	if !strings.HasPrefix(item.Query, "CYPHER ") && item.Name != "catalog" && item.Name != "extension" {
		for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
			forms = append(forms, prefix+"USE "+database+" "+item.Query)
			forms = append(forms, prefix+"CALL () { "+item.Query+" } RETURN *")
		}
	}
	return forms
}

type Position struct{ Line, Column int }

type Diagnostic struct {
	Status, Detail, Text, Class, LocalizedCode string
	Message                                    *localization.Message
	Positions                                  []Position
	Rendered                                   map[string]string
}

// Observe retains the full message/data contract, display text, conformance
// detail and translations. JSON encoding below compares nested template data
// independently of Go map iteration order.
func Observe(err error, manager *localization.Manager) (Diagnostic, error) {
	if err == nil {
		return Diagnostic{}, nil
	}
	code, _ := nornicerrors.Neo4jStatus(err)
	got := Diagnostic{Status: code, Text: err.Error(), Class: fmt.Sprintf("%T", err)}
	var detail interface{ BoltErrorDetail() string }
	if errors.As(err, &detail) {
		got.Detail = detail.BoltErrorDetail()
	}
	var localized *localization.LocalizedError
	if !errors.As(err, &localized) {
		return got, nil
	}
	message := localized.Message
	got.Message, got.LocalizedCode, got.Class = &message, localized.Code, string(message.ID)
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case localization.Message:
			if value.ID == localization.MessageCypherANTLRUnexpectedInput {
				line, lineOK := value.Data["Line"].(int)
				column, columnOK := value.Data["Column"].(int)
				if lineOK && columnOK {
					got.Positions = append(got.Positions, Position{line, column})
				}
			}
			visit(value.Data)
		case localization.MessageSequence:
			for _, item := range value.Messages {
				visit(item)
			}
		case map[string]any:
			// The existing diagnostic sequence is ordered by Detail, not map keys.
			visit(value["Detail"])
		}
	}
	visit(message)
	got.Rendered = map[string]string{}
	for _, name := range []string{"en-US", "es-ES", "en-XA"} {
		ctx := localization.WithPreferences(context.Background(), language.MustParse(name))
		text, _, renderErr := manager.Render(ctx, localization.ErrorRequest(localized))
		if renderErr != nil {
			return got, renderErr
		}
		got.Rendered[name] = text
	}
	return got, nil
}

func Encode(value any) ([]byte, error) { return json.Marshal(value) }
