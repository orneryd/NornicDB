package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	if isOwnedStatementDiagnostic(err) {
		return err, false
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
	source := cyphertext.SourceFor(ctx, rewrite.Original)
	return cyphertext.WithSourceView(ctx, source.Rewrite(rewrite.Text, rewrite.Edits))
}

// statementSource distinguishes a new client statement from a transparent
// continuation. Source windows are supplied by their readers, never recovered
// by searching for matching query text.
func statementSource(ctx context.Context, text string, continuation bool) (context.Context, *cyphertext.Source) {
	if continuation {
		if source, ok := cyphertext.RetainedSource(ctx, text, cyphertext.AcceptedSource); ok {
			return cyphertext.WithSourceView(ctx, source), source
		}
	} else if nested, _ := ctx.Value(executingStatementContextKey{}).(bool); nested {
		source := cyphertext.NewSource(ctx, text, cyphertext.RawSource)
		return cyphertext.WithSourceView(ctx, source), source
	}
	return cyphertext.WithSource(ctx, text)
}

func statementSourceWindow(ctx context.Context, text string, span cyphertext.Span) context.Context {
	source, ok := cyphertext.RetainedSource(ctx, text, cyphertext.AcceptedSource)
	if !ok {
		source = cyphertext.NewSource(ctx, text, cyphertext.AcceptedSource)
	}
	return cyphertext.WithSourceView(ctx, source.Window(span.Start, span.End))
}

// withSourceAffixes keeps generated framing outside the user's coordinate
// system. The original text remains a known source window after prefix/suffix
// insertion, including a re-applied mode or FINISH on a continued statement.
func withSourceAffixes(ctx context.Context, text, prefix, suffix string) (context.Context, string) {
	source, ok := cyphertext.RetainedSource(ctx, text, cyphertext.AcceptedSource)
	if !ok {
		source = cyphertext.NewSource(ctx, text, cyphertext.AcceptedSource)
	}
	edits := make([]cyphertext.SourceEdit, 0, 2)
	if prefix != "" {
		edits = append(edits, cyphertext.SourceEdit{Original: cyphertext.Span{}, Accepted: cyphertext.Span{End: len(prefix)}})
	}
	if suffix != "" {
		edits = append(edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: len(text), End: len(text)}, Accepted: cyphertext.Span{Start: len(prefix) + len(text), End: len(prefix) + len(text) + len(suffix)}})
	}
	text = prefix + text + suffix
	return cyphertext.WithSourceView(ctx, source.Rewrite(text, edits)), text
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
	return cyphertext.WithSourceView(ctx, source.Window(start, start+len(trimmed))), trimmed
}
