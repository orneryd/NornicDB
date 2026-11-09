package cyphertext

import (
	"strings"
)

// CredentialSpan is recorded by the value reader, before the value is handed
// to a manager. Offsets refer to that reader's original source window.
type CredentialSpan struct {
	Start, End int
	Raw, Value string
	Password   bool
}

// CredentialSet belongs to one statement. Grammar and native DDL readers append
// the values they consume; display consumers never infer a value's spelling.
type CredentialSet struct {
	Spans  []CredentialSpan
	Source *Source
}

func (set *CredentialSet) Record(source string, start, end int, value string, password bool) {
	if set == nil || start < 0 || end < start || end > len(source) {
		return
	}
	raw := source[start:end]
	if set.Source != nil && source == set.Source.Text {
		set.RecordSource(set.Source, start, end, value, password)
		return
	}
	set.Spans = append(set.Spans, CredentialSpan{Start: start, End: end, Raw: raw, Value: value, Password: password})
}

// RecordSource records a value in an explicitly retained source view/window.
// Grammar and native adapters supply their consumed extent, never a text search.
func (set *CredentialSet) RecordSource(source *Source, start, end int, value string, password bool) {
	if set == nil || start < 0 || end < start || end > len(source.Text) {
		return
	}
	span, _ := source.OriginalSpan(Span{start, end}, SourceExtent)
	set.Spans = append(set.Spans, CredentialSpan{Start: span.Start, End: span.End, Raw: source.Original[span.Start:span.End], Value: value, Password: password})
}

// PasswordBoundary is a positional fail-closed boundary, even when malformed
// text before the keyword prevents tokenization. It does not read its value.
func PasswordBoundary(source string) int {
	return NormalizeSource(source).PasswordBoundary()
}

// PasswordBoundary classifies the accepted view and returns an original byte
// offset, including when normalization joins the keyword across deleted runes.
func (source *Source) PasswordBoundary() int {
	const keyword = "PASSWORD"
	for index := 0; index+len(keyword) <= len(source.Text); index++ {
		end := index + len(keyword)
		if strings.EqualFold(source.Text[index:end], keyword) && KeywordBoundary(source.Text, index, end) {
			offset, _ := source.OriginalOffset(index, true, SourceExtent)
			return offset
		}
	}
	return -1
}
