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
	return cyphertext.WithSourceView(ctx, source.Window(start, start+len(trimmed))), trimmed
}
