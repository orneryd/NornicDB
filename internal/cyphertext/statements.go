package cyphertext

// StatementSpans splits source only at live semicolons. Each half-open span
// retains its original bytes; a trailing delimiter yields an empty final span.
// Import and browser adapters decide their own empty-segment presentation.
func StatementSpans(text string) []Span {
	return NormalizeSource(text).StatementSpans()
}

// StatementSpans classifies the accepted source and returns original spans.
func (source *Source) StatementSpans() []Span {
	text := source.Text
	var spans []Span
	start := 0
	for index := 0; index < len(text); index++ {
		if end, opaque := SkipToken(text, index); opaque {
			index = end - 1
			continue
		}
		if text[index] == ';' {
			span, _ := source.OriginalSpan(Span{start, index}, SourceExtent)
			spans = append(spans, span)
			start = index + 1
		}
	}
	span, _ := source.OriginalSpan(Span{start, len(text)}, SourceExtent)
	return append(spans, span)
}
