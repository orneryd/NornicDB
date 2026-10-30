package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"errors"
	"context"
	"fmt"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// SemanticError describes a Cypher validation failure that must retain its
// Neo4j-compatible Bolt classification and conformance detail.
type SemanticError struct {
	Code    string
	Detail  string
	Message string
	// Source coordinates are recorded by their diagnostic producer, separately
	// from prose about offsets in data/protocol errors.
	sourceDiagnostic *sourceByteDiagnostic
}

// classifiedCypherError adds Neo4j/TCK classification to an existing typed
// error without changing its localized message or unwrap identity.
type classifiedCypherError struct {
	cause  error
	code   string
	detail string
}

func (e *classifiedCypherError) Error() string           { return e.cause.Error() }
func (e *classifiedCypherError) Unwrap() error           { return e.cause }
func (e *classifiedCypherError) BoltErrorCode() string   { return e.code }
func (e *classifiedCypherError) BoltErrorDetail() string { return e.detail }

// Error formats the error so non-Bolt callers retain the Neo4j status code.
func (e *SemanticError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// BoltErrorCode returns the Neo4j-compatible Bolt error code.
func (e *SemanticError) BoltErrorCode() string { return e.Code }

// BoltErrorDetail returns the openCypher conformance error detail.
func (e *SemanticError) BoltErrorDetail() string { return e.Detail }

func invalidBooleanOperandError(value interface{}) error {
	return &SemanticError{
		Code:    "Neo.ClientError.Statement.SyntaxError",
		Detail:  "InvalidArgumentType",
		Message: fmt.Sprintf("boolean operator requires BOOLEAN or NULL operands, got %T", value),
	}
}

// procedureCallFailedError is Neo4j's Procedure.ProcedureCallFailed: a
// procedure failed while running, with a cause that carries no status of its
// own (#657).
type procedureCallFailedError struct {
	procedure string
	cause     error
}

func (e *procedureCallFailedError) Error() string {
	return fmt.Sprintf("Failed to invoke procedure `%s`: Caused by: %s", e.procedure, e.cause.Error())
}
func (e *procedureCallFailedError) Unwrap() error { return e.cause }
func (e *procedureCallFailedError) BoltErrorCode() string {
	return "Neo.ClientError.Procedure.ProcedureCallFailed"
}

// procedureRuntimeError is the error of procedure failing while running with
// err: ProcedureCallFailed, as in Neo4j, unless err carries its own status
// (a Cypher error of a statement the procedure ran, a transient failure, ...).
// Every CALL dispatch reports procedure failures through it.
func procedureRuntimeError(procedure string, err error) error {
	if nornicerrors.HasNeo4jStatus(err) {
		return err
	}
	return &procedureCallFailedError{procedure: procedure, cause: err}
}

func newSemanticError(code, detail, message string) error {
	return &SemanticError{Code: code, Detail: detail, Message: message}
}

// syntaxAdmissionError retains specific pattern diagnostics at grammar admission.
func syntaxAdmissionError(ctx context.Context, source string, stage SourceStage, cause error, partial ...cypherantlr.StatementSyntax) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	view := cyphertext.NewSource(ctx, source, stage)
	if err := ctx.Err(); err != nil {
		return err
	}
	if view.Err == nil {
		source = view.Text
		if err := malformedCallScopeError(ctx, source); err != nil {
			return err
		}
	}
	if activeSyntaxValidator() != syntaxValidANTLR {
		// This native declaration-spelling diagnostic is only framing for a
		// failed grammar product. Accepted lexical bindings come from the CST.
		if err := validateListComprehensionVariables(source); err != nil {
			return err
		}
		if len(partial) > 0 {
			if err := syntaxProjectionDeclarationError(ctx, partial[0]); err != nil {
				return err
			}
		}
		// Retain the existing reduce/allReduce signature diagnostic when grammar
		// admission fails: the text has no grammar calls, so the REDUCE and
		// allReduce keywords are located directly and their form is checked.
		if err := reduceFormSyntaxError(ctx, source); err != nil {
			return err
		}

	}
	if invalidRelationshipPattern(source) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidRelationshipPattern", "invalid variable-length relationship pattern")
	}
	var localized *localization.LocalizedError
	if errors.As(cause, &localized) {
		const code = "Neo.ClientError.Statement.SyntaxError"
		// The grammar owns diagnostic identity and structured coordinates. The
		// admission adapter adds transport classification without flattening it.
		classified := localization.NewLocalizedError(code, localized.Message, cause)
		return &classifiedCypherError{cause: fmt.Errorf("%s: %w", code, classified), code: code, detail: "UnexpectedSyntax"}
	}
	message := strings.ReplaceAll(cause.Error(), "mismatched input ", "Invalid input ")
	message = strings.ReplaceAll(message, "extraneous input ", "Invalid input ")
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", message)
}

// invalidForeachError classifies malformed updates and invalid runtime list
// operands, returning a fresh mutable SemanticError for each rejected statement.
func invalidForeachError() error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidForeach", "invalid or unsupported FOREACH update")
}

// invalidCallImportError preserves the release status-bearing public diagnostic.
// Header grammar admission decides validity; this is only its message factory.
func invalidCallImportError() error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", "CALL imports must contain only variable names")
}

// Failed native framing can retain a complete RETURN after an invalid command
// prefix. Preserve the release declaration diagnostic by asking the same
// ordered declaration constructor; this diagnostic-only grammar window never
// publishes a scope, binding identity, accepted statement or result header.
func syntaxProjectionDeclarationError(ctx context.Context, syntax cypherantlr.StatementSyntax) error {
	if !syntax.Valid() {
		return nil
	}
	if err := admitCommandProjectionItems(ctx, syntax); err != nil {
		return err
	}
	for i := 0; i < syntax.TokenCount(); i++ {
		token := syntax.Token(i)
		if token.Channel() != 0 || token.Kind() != cypherantlr.CypherLexerRETURN {
			continue
		}
		start, _ := token.Span()
		source := syntax.Source()[start:]
		if names := quotedVariableNamesFor(ctx, syntax.Source()); names != nil {
			source, _ = names.OriginalText(cyphertext.Span{Start: start, End: len(syntax.Source())}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
		}
		projection, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.ClauseEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			if err := admitCommandProjectionItems(ctx, projection); err != nil {
				return err
			}
		}
	}
	return nil
}

// reduceFormSyntaxError checks the form of each REDUCE / allReduce keyword
// followed by "(" in text the grammar rejected, outside strings and comments.
func reduceFormSyntaxError(ctx context.Context, source string) error {
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	for _, keyword := range []string{"REDUCE", "allReduce"} {
		for from := 0; from < len(source); {
			at := keywordIndexFrom(source, keyword, from, opts)
			if at < 0 {
				break
			}
			from = at + len(keyword)
			open := queryGapEnd(source, from)
			if open >= len(source) || source[open] != '(' {
				continue
			}
			if err := checkReduceForm(ctx, source[at:from], source, open, func(staticArgumentType, string, int) error { return nil }); err != nil {
				return err
			}
		}
	}
	return nil
}
