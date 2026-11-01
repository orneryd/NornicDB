package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// executingStatementContextKey marks a context in which a statement is
// executing, so a statement supplied by a procedure owns a new raw source.
type executingStatementContextKey struct{}

// statementDiagnosticError seals a diagnostic in the submitted source's
// coordinate system. Its immutable message is projected at the grammar owner,
// not repeatedly translated by the lowering and result-restoration chain.
type statementDiagnosticError struct{ cause error }

func (e *statementDiagnosticError) Error() string { return e.cause.Error() }
func (e *statementDiagnosticError) Unwrap() error { return e.cause }

func executingStatementContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, executingStatementContextKey{}, true)
}

func ownedStatementDiagnostic(ctx context.Context, text string, stage SourceStage, message localization.Message, cause error, refinement error) error {
	const status = "Neo.ClientError.Statement.SyntaxError"
	detail := "UnexpectedSyntax"
	if refinement != nil {
		// A specific semantic explanation refines the canonical grammar
		// product; it cannot replace that product's identity or coordinates.
		var specific *localization.LocalizedError
		var explanation localization.Message
		if errors.As(refinement, &specific) {
			explanation = specific.Message
		} else {
			explanation = localization.ExternalDiagnostic(refinement)
		}
		parts := []localization.Message{explanation}
		if positions, ok := message.Data["Detail"].(localization.MessageSequence); ok {
			parts = append(parts, positions.Messages...)
		}
		sequence := localization.MessageSequence{Messages: parts, Separator: "; "}
		message = localization.CypherANTLRParseSyntaxError(sequence.String(), parts...)
		var classified interface{ BoltErrorDetail() string }
		if errors.As(refinement, &classified) {
			detail = classified.BoltErrorDetail()
		}
	}
	view, found := cyphertext.RetainedSource(ctx, text, stage)
	if !found {
		view = cyphertext.NewSource(ctx, text, stage)
	}
	// Even an edit-free source window owns a base offset. Do not discard it
	// using the ordinary result-rewrite adapter's no-edits fast path.
	projection := &queryRewrite{Source: *view}
	if projected, changed := (resultRestoration{source: projection}).value(message, sourceValueText); changed {
		message = projected.(localization.Message)
	}
	return &statementDiagnosticError{cause: &classifiedCypherError{
		cause: fmt.Errorf("%s: %w", status, localization.NewLocalizedError(status, message, cause)),
		code:  status, detail: detail,
	}}
}

func isOwnedStatementDiagnostic(err error) bool {
	var diagnostic *statementDiagnosticError
	return errors.As(err, &diagnostic)
}
