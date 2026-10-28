package antlr

import (
	"context"
	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode/utf8"
)

// sourceCharStream is the one accepted-text representation. It owns the
// accepted text, the cursor, the coordinate product and every non-lookahead
// ANTLR operation. A nil boundary table is the ordinary ASCII representation;
// Unicode and unpaired UTF16 units share the canonical source codepoint
// representation through the boundary table. Only encoding lookup and
// cancellation instrumentation are delegated to the four concrete views.
type sourceCharStream struct {
	// text is the accepted-text frozen value, borrowed from the source header
	// and never copied byte-for-byte.
	text     string
	position int
	// length caches the virtual position count once per stream: the accepted
	// byte count for ASCII, or the codepoint count for a boundary table.
	length int
	// source is cold origin metadata only: OriginalText maps through it while
	// the accepted text above stays frozen for the stream's lifetime.
	source *cyphertext.Source
	// boundaries is nil for ASCII, otherwise one byte offset per codepoint
	// plus the terminating accepted length.
	boundaries []int
}

// lookaheadIndex resolves an ANTLR LA offset to the virtual position it names.
// It owns every offset rule: zero lookahead, the backstep for negative
// offsets, and the EOF bound. index is -1 when the offset names no position;
// terminal is the value such a lookup reports, which is TokenEOF for every
// real offset and the literal zero for LA(0).
func (s *sourceCharStream) lookaheadIndex(offset, length int) (index, terminal int) {
	if offset == 0 {
		return -1, 0
	}
	if offset < 0 {
		offset++
	}
	at := s.position + offset - 1
	if at < 0 || at >= length {
		return -1, antlr.TokenEOF
	}
	return at, 0
}

// byteLookahead returns the single accepted byte at an ANTLR LA offset.
func (s *sourceCharStream) byteLookahead(offset int) int {
	index, terminal := s.lookaheadIndex(offset, len(s.text))
	if index < 0 {
		return terminal
	}
	return int(s.text[index])
}

// indexedLookahead returns the accepted codepoint at an ANTLR LA offset.
func (s *sourceCharStream) indexedLookahead(offset int) int {
	index, terminal := s.lookaheadIndex(offset, len(s.boundaries)-1)
	if index < 0 {
		return terminal
	}
	return s.codepointLookahead(index)
}

func (s *sourceCharStream) codepointLookahead(at int) int {
	value, _ := cyphertext.ReadCodepoint(s.text, s.boundaries[at])
	return int(value)
}

func (s *sourceCharStream) Mark() int   { return -1 }
func (s *sourceCharStream) Release(int) {}
func (s *sourceCharStream) Index() int  { return s.position }
func (s *sourceCharStream) Seek(at int) { s.position = at }
func (s *sourceCharStream) Size() int   { return s.length }
func (s *sourceCharStream) Consume() {
	if s.position >= s.length {
		panic("cannot consume EOF")
	}
	s.position++
}
func (s *sourceCharStream) GetSourceName() string {
	if s.text == s.source.Original && utf8.ValidString(s.text) {
		return "<empty>"
	}
	return "<source>"
}
func (s *sourceCharStream) byteOffset(at int) int {
	if s.boundaries == nil {
		return at
	}
	return s.boundaries[at]
}
func (s *sourceCharStream) GetText(start, stop int) string {
	start = max(0, start)
	stop = min(stop, s.length-1)
	if start > stop {
		return ""
	}
	return s.text[s.byteOffset(start):s.byteOffset(stop+1)]
}
func (s *sourceCharStream) GetTextFromTokens(start, end antlr.Token) string {
	if start == nil || end == nil {
		return ""
	}
	return s.GetText(start.GetStart(), end.GetStop())
}
func (s *sourceCharStream) GetTextFromInterval(interval antlr.Interval) string {
	return s.GetText(interval.Start, interval.Stop)
}
func (s *sourceCharStream) OriginalText(start, stop int) string {
	start = max(0, start)
	stop = min(stop, s.length-1)
	if start > stop {
		return ""
	}
	original, _ := s.source.OriginalText(cyphertext.Span{Start: s.byteOffset(start), End: s.byteOffset(stop + 1)}, cyphertext.SourceExtent, nil)
	return original
}

// byteSourceCharStream is the ASCII view: its only own member is the encoding
// lookup delegated by the ANTLR CharStream interface.
type byteSourceCharStream struct {
	sourceCharStream
}

func (s *byteSourceCharStream) LA(offset int) int { return s.byteLookahead(offset) }

// indexedSourceCharStream is the codepoint view, selected by a boundary table.
type indexedSourceCharStream struct {
	sourceCharStream
}

func (s *indexedSourceCharStream) LA(offset int) int { return s.indexedLookahead(offset) }

// contextSourceCharStream adds cancellation instrumentation to the same core
// representation. It owns only Consume polling; its lookahead is a concrete
// encoding view below, so cancellation is not a per-character branch.
type contextSourceCharStream struct {
	sourceCharStream
	contextCharCancellation
}

func (s *contextSourceCharStream) Consume() {
	if s.position&1023 == 0 {
		s.poll()
	}
	s.sourceCharStream.Consume()
}

// contextByteSourceCharStream rethrows a swallowed unwind, then reads bytes.
type contextByteSourceCharStream struct {
	contextSourceCharStream
}

func (s *contextByteSourceCharStream) LA(offset int) int {
	s.rethrow()
	return s.byteLookahead(offset)
}

// contextIndexedSourceCharStream rethrows a swallowed unwind, then reads
// codepoints. Sampling its cancellation check is unchanged.
type contextIndexedSourceCharStream struct {
	contextSourceCharStream
}

func (s *contextIndexedSourceCharStream) LA(offset int) int {
	s.check()
	return s.indexedLookahead(offset)
}

// sourceStreamView builds the single concrete view a parse needs and returns
// the coordinate product embedded in it together with the ANTLR stream. Only
// that view is allocated; the core value is copied into it in place, so no
// separate core allocation exists and no local escapes.
func sourceStreamView(core sourceCharStream, ctx context.Context) (*sourceCharStream, antlr.CharStream) {
	if ctx.Done() != nil {
		contextView := contextSourceCharStream{sourceCharStream: core, contextCharCancellation: contextCharCancellation{ctx: ctx}}
		if core.boundaries == nil {
			view := &contextByteSourceCharStream{contextSourceCharStream: contextView}
			return &view.sourceCharStream, view
		}
		view := &contextIndexedSourceCharStream{contextSourceCharStream: contextView}
		return &view.sourceCharStream, view
	}
	if core.boundaries == nil {
		view := &byteSourceCharStream{sourceCharStream: core}
		return &view.sourceCharStream, view
	}
	view := &indexedSourceCharStream{sourceCharStream: core}
	return &view.sourceCharStream, view
}

// sourceInputStream retains the checkpointed boundary build, then hands the
// core value to the view factory that selects one concrete view.
func sourceInputStream(ctx context.Context, source *cyphertext.Source) (*sourceCharStream, antlr.CharStream) {
	var boundaries []int
	for index := 0; index < len(source.Text); index++ {
		if index&1023 == 0 {
			if err := ctx.Err(); err != nil {
				panic(predictionCanceled{err})
			}
		}
		if source.Text[index] >= 0x80 {
			var err error
			boundaries, err = source.CodepointBoundaries(ctx)
			if err != nil {
				panic(predictionCanceled{err})
			}
			break
		}
	}
	length := len(source.Text)
	if boundaries != nil {
		length = len(boundaries) - 1
	}
	return sourceStreamView(sourceCharStream{source: source, text: source.Text, length: length, boundaries: boundaries}, ctx)
}
