package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strconv"
	"strings"
)

// sourceQueryRewrite adapts the common source map to the existing presentation
// restoration chain. Later canonical/numeric rewrites restore into this view,
// then this final link restores original columns, plans and diagnostics.
func sourceQueryRewrite(source *cyphertext.Source) *queryRewrite {
	if len(source.Edits) == 0 {
		return nil
	}
	rewrite := &queryRewrite{Source: *source}
	return rewrite
}

type sourceByteDiagnostic struct {
	offset int
	prefix string
}

func newSourceOffsetError(code, detail, prefix string, offset int) error {
	return &SemanticError{Code: code, Detail: detail, Message: prefix + strconv.Itoa(offset), sourceDiagnostic: &sourceByteDiagnostic{offset: offset, prefix: prefix}}
}

// restoreSourceError preserves typed diagnostic products and cause identity.
// Offset facts come from producers, never from guessing at arbitrary prose.
func (r *queryRewrite) restoreSourceError(err error) (error, bool) {
	if err == nil {
		return nil, false
	}
	restoredText, changed := (resultRestoration{source: r}).value(err.Error(), sourceDiagnosticText)
	message := restoredText.(string)
	var semanticCopy *SemanticError
	var localizedCopy *localization.LocalizedError
	var semantic *SemanticError
	if errors.As(err, &semantic) && semantic.sourceDiagnostic != nil {
		diagnostic := *semantic.sourceDiagnostic
		if offset, _ := r.OriginalOffset(diagnostic.offset, true, cyphertext.SourceExtent); offset != diagnostic.offset {
			diagnostic.offset = offset
			copy := *semantic
			copy.sourceDiagnostic = &diagnostic
			copy.Message = diagnostic.prefix + strconv.Itoa(offset)
			semanticCopy = &copy
			message = strings.ReplaceAll(message, semantic.Error(), copy.Error())
			changed = true
		}
	}
	var localized *localization.LocalizedError
	if errors.As(err, &localized) {
		if restored, mapped := (resultRestoration{source: r}).value(localized.Message, sourceValueText); mapped {
			copy := *localized
			copy.Message = restored.(localization.Message)
			localizedCopy = &copy
			changed = true
		}
	}
	if !changed {
		return err, false
	}
	return localization.ProjectError(err, message, localizedCopy, semanticCopy), true
}

func withSourceRewrite(ctx context.Context, rewrite *queryRewrite) context.Context {
	if rewrite == nil {
		return ctx
	}
	source, known := cypherantlr.RetainedSyntaxSource(ctx, rewrite.Original)
	if !known {
		source = cyphertext.AcceptedSourceFor(ctx, rewrite.Original)
	}
	view := source.Rewrite(rewrite.Text, rewrite.Edits)
	ctx = cypherantlr.WithSyntaxProgram(ctx, view)
	return cyphertext.WithSourceView(ctx, view)
}

type sourceTrimPolicy uint8

const (
	trimSourceWhitespace sourceTrimPolicy = iota
	trimSourceTerminator
)

// sourceWhitespaceExtent returns the half-open byte extent of text with
// Unicode White_Space trimmed from both ends. It uses the shared lexer's
// IsWhitespace classification (unicode.IsSpace) so callers and the
// normalization tables agree on one whitespace population. The end is
// clamped so it is never smaller than start.
func sourceWhitespaceExtent(text string) (start, end int) {
	start = len(text) - len(strings.TrimLeftFunc(text, cyphertext.IsWhitespace))
	end = len(strings.TrimRightFunc(text, cyphertext.IsWhitespace))
	if end < start {
		end = start
	}
	return start, end
}

func trimSourceText(ctx context.Context, text string, policy sourceTrimPolicy) (context.Context, string) {
	source := cyphertext.SourceFor(ctx, text)
	start, end := sourceWhitespaceExtent(text)
	trimmed := text[start:end]
	if policy == trimSourceTerminator {
		trimmed = trimTrailingStatementDelimiters(trimmed)
	}
	// An unchanged extent already has the exact retained source mapping. Do
	// not add another window and context layer for the same accepted text.
	if start == 0 && len(trimmed) == len(text) {
		if _, retained := cyphertext.RetainedSource(ctx, text, cyphertext.AcceptedSource); retained {
			return ctx, trimmed
		}
	}
	view := source.Window(start, start+len(trimmed))
	return cyphertext.WithSourceView(ctx, view), trimmed
}

// generatedStatementContext is the explicit ingress of a new execution program
// constructed by an existing planner (new predicates/projections/bindings).
// A mere source window or an adapter miss must never call it. Each producer is
// enumerated in the R17 source ledger; execution decisions remain with de70.
func generatedStatementContext(ctx context.Context, text string) context.Context {
	return statementProgramContext(ctx, text, cyphertext.RawSource)
}

// compilerStatementContext is an independently declared, already-accepted
// schema/validation program. It shares the constructor, but never normalizes
// compiler text as raw transport input.
func compilerStatementContext(ctx context.Context, text string) context.Context {
	return statementProgramContext(ctx, text, cyphertext.AcceptedSource)
}

func statementProgramContext(ctx context.Context, text string, stage cyphertext.SourceStage) context.Context {
	var source *cyphertext.Source
	if stage == cyphertext.RawSource {
		ctx, source = cyphertext.WithSource(ctx, text)
	} else {
		source = cyphertext.NewSource(ctx, text, stage)
		ctx = cyphertext.WithSourceView(ctx, source)
	}
	return cypherantlr.WithSyntaxOwner(ctx, source.Text)
}
