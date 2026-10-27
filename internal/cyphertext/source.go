package cyphertext

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"
)

// Span is a half-open byte interval in a source or its accepted lexical view.
type Span struct{ Start, End int }

// SourceEdit relates a replacement or deletion to the original source. A
// deletion has an empty Accepted span; both intervals remain in source order.
type SourceEdit struct{ Original, Accepted Span }

// Source retains the submitted text, the text accepted by execution, and the
// edits connecting them. Opaque quoted tokens and comments retain their bytes.
type Source struct {
	Original, Text string
	Err            error
	Edits          []SourceEdit
	parent         *Source
	base           int
}

// NormalizeSource applies the executor's accepted character transformations.
// For example, PASS\u200bWORD becomes PASSWORD while retaining the deleted span.
// NewSource constructs an explicitly raw or already accepted source view.
// Accepted input is never Unicode-expanded again.
func NewSource(ctx context.Context, text string, stage SourceStage) *Source {
	switch stage {
	case AcceptedSource:
		return &Source{Original: text, Text: text}
	case RawSource:
		return normalizeSource(ctx, text)
	default:
		return &Source{Original: text, Text: text, Err: errors.New("invalid source stage")}
	}
}

func NormalizeSource(original string) *Source {
	return NewSource(context.Background(), original, RawSource)
}

func normalizeSource(ctx context.Context, original string) *Source {
	projected := ProjectUnicode(ctx, original)
	if projected.Err != nil {
		return projected
	}
	if PlainASCII(projected.Text) && !strings.ContainsAny(projected.Text, "\v\f") {
		return projected
	}
	normalized := normalizeAccepted(ctx, projected.Text)
	if normalized.Err != nil {
		return normalized
	}
	if len(normalized.Edits) == 0 {
		return projected
	}
	if len(projected.Edits) == 0 {
		return normalized
	}
	return projected.Rewrite(normalized.Text, normalized.Edits)
}

func normalizeAccepted(ctx context.Context, original string) *Source {
	source := &Source{Original: original, Text: original}
	var out strings.Builder
	last := 0
	nextPoll := 0
	for index := 0; index < len(original); {
		if index >= nextPoll {
			nextPoll = index + 1024
			if err := ctx.Err(); err != nil {
				source.Err = err
				return source
			}
		}
		if end, opaque := SkipToken(original, index); opaque {
			index = end
			continue
		}
		character, width := utf8.DecodeRuneInString(original[index:])
		replacement, changed := SyntaxConfusableReplacement(character)
		if arrow, ok := arrowReplacement(original, index, width); ok {
			replacement, changed = string(arrow), true
		} else if !changed {
			if space, ok := WhitespaceReplacement(character); ok {
				replacement, changed = string(space), true
			} else if ignorableFormat(character) {
				replacement, changed = "", true
			}
		}
		if changed {
			out.WriteString(original[last:index])
			start := out.Len()
			out.WriteString(replacement)
			source.Edits = append(source.Edits, SourceEdit{Span{index, index + width}, Span{start, out.Len()}})
			last = index + width
		}
		index += width
	}
	if len(source.Edits) != 0 {
		out.WriteString(original[last:])
		source.Text = out.String()
	}
	return source
}

// SourceMappingPolicy selects how a boundary inside a replacement is mapped.
type SourceMappingPolicy uint8

const (
	// SourceExtent maps replacement interiors to their whole original extent.
	// At deletions span starts are right-biased and ends are left-biased.
	SourceExtent SourceMappingPolicy = iota
	// SourceExactBoundaries rejects interiors of the current view's replacements.
	// Exact replacement edges retain quoted-name restoration's boundary rules.
	SourceExactBoundaries
)

// OriginalOffset maps a byte boundary in Text to Original. Exact-boundary
// policy applies to this view only: its parent/window association retains the
// established extent policy for earlier normalization stages.
func (source *Source) OriginalOffset(offset int, start bool, policy SourceMappingPolicy) (int, bool) {
	shift, mapped := 0, offset
	resolved := false
	for _, edit := range source.Edits {
		before := offset < edit.Accepted.Start || !start && offset == edit.Accepted.Start
		if policy == SourceExactBoundaries {
			before = offset < edit.Accepted.Start || start && offset == edit.Accepted.Start
		}
		if before {
			break
		}
		if policy == SourceExactBoundaries {
			if offset == edit.Accepted.End {
				mapped, resolved = edit.Original.End, true
				break
			}
			if offset < edit.Accepted.End {
				if offset != edit.Accepted.Start {
					return 0, false
				}
				mapped, resolved = edit.Original.Start, true
				break
			}
		} else if start && offset < edit.Accepted.End {
			mapped, resolved = edit.Original.Start, true
			break
		} else if !start && offset <= edit.Accepted.End {
			mapped, resolved = edit.Original.End, true
			break
		}
		shift = edit.Original.End - edit.Accepted.End
	}
	if !resolved {
		mapped = offset + shift
	}
	mapped += source.base
	if source.parent != nil {
		return source.parent.OriginalOffset(mapped, start, SourceExtent)
	}
	return mapped, true
}

// Rewrite composes a canonical rewrite with this source association. Its edit
// Original spans refer to this view's Text, and mapping continues to the root.
func (source *Source) Rewrite(text string, edits []SourceEdit) *Source {
	return &Source{Original: source.Original, Text: text, Edits: edits, parent: source}
}

// Window retains an exact child source window without searching by its value.
func (source *Source) Window(start, end int) *Source {
	return &Source{Original: source.Original, Text: source.Text[start:end], parent: source, base: start}
}

// OriginalText maps one accepted byte extent to its original spelling. A strict
// boundary consumer may supply its established recovery policy when a span
// intersects a rewrite; extent consumers pass nil and never invent source text.
// The validity result distinguishes a mapped empty extent from an unmapped span.
func (source *Source) OriginalText(span Span, policy SourceMappingPolicy, recoverText func(string) string) (string, bool) {
	if span.Start < 0 || span.End < span.Start || span.End > len(source.Text) {
		return "", false
	}
	if original, ok := source.OriginalSpan(span, policy); ok && original.Start >= 0 && original.End >= original.Start && original.End <= len(source.Original) {
		return source.Original[original.Start:original.End], true
	}
	if recoverText != nil {
		return recoverText(source.Text[span.Start:span.End]), true
	}
	return "", false
}

// WithSourceView retains a known rewrite/window product on its execution path.
func WithSourceView(ctx context.Context, source *Source) context.Context {
	return context.WithValue(ctx, sourceKey{}, source)
}

// OriginalSpan maps an accepted span to its original half-open extent. Exact
// policy rejects replacement interiors and invalid original extents; extent
// policy preserves empty spans and the normalization boundary bias.
func (source *Source) OriginalSpan(span Span, policy SourceMappingPolicy) (Span, bool) {
	start, ok := source.OriginalOffset(span.Start, true, policy)
	if !ok {
		return Span{}, false
	}
	if span.End == span.Start && policy == SourceExtent {
		return Span{start, start}, true
	}
	end, ok := source.OriginalOffset(span.End, false, policy)
	if !ok || policy == SourceExactBoundaries && (start < 0 || end < start || end > len(source.Original)) {
		return Span{}, false
	}
	return Span{start, end}, true
}

type sourceKey struct{}

// RetainedSource returns only an existing source association. Accepted callers
// never reinterpret their text as raw input when no association exists.
func RetainedSource(ctx context.Context, text string, stage SourceStage) (*Source, bool) {
	if source, ok := ctx.Value(sourceKey{}).(*Source); ok {
		for view := source; view != nil; view = view.parent {
			if view.Text == text {
				return view, true
			}
			if stage == RawSource && view.parent == nil && view.Original == text {
				return view, true
			}
		}
	}
	return nil, false
}

// WithSource retains one normalization result across execution and its output
// consumers. Nested statements obtain their own source association.
func WithSource(ctx context.Context, text string) (context.Context, *Source) {
	if source, ok := RetainedSource(ctx, text, RawSource); ok {
		return ctx, source
	}
	source := normalizeSource(ctx, text)
	return context.WithValue(ctx, sourceKey{}, source), source
}

// SourceFor reuses the statement's source product, or normalizes standalone text.
func SourceFor(ctx context.Context, text string) *Source {
	_, source := WithSource(ctx, text)
	return source
}

// PlainASCII is the cheap no-normalization screen shared by admission.
func PlainASCII(text string) bool {
	for index := range text {
		if text[index] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// These private tables are the normalization population. Execution and its
// caller/sink properties use them; tests compare the independent fixture set
// to their complete key union, including the whitespace owner's Unicode table.
var syntaxConfusableReplacements = map[rune]string{
	'→': "->", '←': "<-", '（': "(", '）': ")", '［': "[", '］': "]", '｛': "{", '｝': "}",
	'，': ",", '：': ":", '；': ";", '．': ".", '＝': "=", '＜': "<", '＞': ">", '＄': "$",
}
var ignorableFormatRunes = map[rune]bool{'\u200b': true, '\u200c': true, '\u200d': true, '\u2060': true, '\ufeff': true}
var arrowDashRunes = map[rune]bool{'\u00ad': true, '‐': true, '‑': true, '‒': true, '–': true, '—': true, '―': true, '﹘': true, '﹣': true, '－': true}
var arrowLeftRunes = map[rune]bool{'⟨': true, '〈': true, '﹤': true, '＜': true}
var arrowRightRunes = map[rune]bool{'⟩': true, '〉': true, '﹥': true, '＞': true}

// SyntaxConfusableReplacement is the accepted punctuation normalization table.
func SyntaxConfusableReplacement(character rune) (string, bool) {
	replacement, ok := syntaxConfusableReplacements[character]
	return replacement, ok
}

func ignorableFormat(character rune) bool { return ignorableFormatRunes[character] }

// Arrow characters normalize only in the existing relationship-arrow context.
func arrowReplacement(text string, index, width int) (byte, bool) {
	neighbor := func(step int) rune {
		for cursor := index; ; {
			var character rune
			position := cursor
			if step < 0 {
				if cursor == 0 {
					return 0
				}
				var size int
				character, size = utf8.DecodeLastRuneInString(text[:cursor])
				cursor -= size
				position = cursor
			} else {
				if cursor == index {
					cursor += width
				}
				if cursor >= len(text) {
					return 0
				}
				position = cursor
				var size int
				character, size = utf8.DecodeRuneInString(text[cursor:])
				cursor += size
			}
			if WhitespaceAt(text, position) == 0 {
				return character
			}
		}
	}
	character, _ := utf8.DecodeRuneInString(text[index:])
	switch {
	case arrowDash(character):
		previous, next := neighbor(-1), neighbor(1)
		before := previous == ')' || previous == ']' || previous == '<' || previous == '-' || arrowDash(previous) || arrowLeft(previous)
		after := next == '(' || next == '[' || next == '>' || next == '-' || arrowDash(next) || arrowRight(next)
		return '-', before && after
	case arrowLeft(character):
		next := neighbor(1)
		return '<', next == '-' || arrowDash(next)
	case arrowRight(character):
		previous := neighbor(-1)
		return '>', previous == '-' || arrowDash(previous)
	}
	return 0, false
}

func arrowDash(character rune) bool  { return arrowDashRunes[character] }
func arrowLeft(character rune) bool  { return arrowLeftRunes[character] }
func arrowRight(character rune) bool { return arrowRightRunes[character] }

// RewriteMatchPolicy selects the observation a presentation consumer needs.
type RewriteMatchPolicy uint8

const (
	// RewriteSpanOverlap observes a known accepted source interval.
	RewriteSpanOverlap RewriteMatchPolicy = iota
	// RewriteFragmentMention observes replacement spelling in a text fragment.
	RewriteFragmentMention
)

// RewriteMatch supplies either a known accepted Span or a Fragment. Markers
// retain the quoted-name owner's generated identifiers within expanded edits;
// they are used only with RewriteFragmentMention and are never inferred here.
type RewriteMatch struct {
	Policy   RewriteMatchPolicy
	Span     Span
	Fragment string
	Markers  []string
}

// HasRewrite observes canonical edits through one ordered traversal. For
// example, span overlap controls exact source restoration, while a quoted-name
// column checks fragment mentions and its existing generated-name markers.
func (source *Source) HasRewrite(match RewriteMatch) bool {
	if match.Policy == RewriteFragmentMention {
		for _, marker := range match.Markers {
			if strings.Contains(match.Fragment, marker) {
				return true
			}
		}
	}
	for _, edit := range source.Edits {
		if match.Policy == RewriteSpanOverlap {
			if edit.Accepted.Start >= match.Span.End {
				return false
			}
			if edit.Accepted.End > match.Span.Start {
				return true
			}
		} else if strings.Contains(match.Fragment, source.Text[edit.Accepted.Start:edit.Accepted.End]) {
			return true
		}
	}
	return false
}
