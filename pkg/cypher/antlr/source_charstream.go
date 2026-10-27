package antlr

import (
	"context"
	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode/utf8"
)

// sourceCharStream owns accepted virtual codepoints and their byte offsets.
// A nil boundary table is the ordinary ASCII representation; Unicode and
// unpaired UTF16 units share the canonical source codepoint representation.
type sourceCharStream struct {
	source     *cyphertext.Source
	boundaries []int
	position   int
}

// sourceInputStream returns both the coordinate product and the ANTLR stream.
// Statement materialization reuses the same boundary product rather than
// building a second full offset table after parsing.
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
	if ctx.Done() != nil {
		stream := &contextSourceCharStream{sourceCharStream: sourceCharStream{source: source, boundaries: boundaries}, contextCharCancellation: contextCharCancellation{ctx: ctx}}
		return &stream.sourceCharStream, stream
	}
	stream := &sourceCharStream{source: source, boundaries: boundaries}
	return stream, stream
}

func (s *sourceCharStream) Consume() {
	if s.position >= s.Size() {
		panic("cannot consume EOF")
	}
	s.position++
}
func (s *sourceCharStream) LA(offset int) int {
	if offset == 0 {
		return 0
	}
	if offset < 0 {
		offset++
	}
	at := s.position + offset - 1
	if s.boundaries != nil {
		return s.codepointLookahead(at)
	}
	if at < 0 || at >= len(s.source.Text) {
		return antlr.TokenEOF
	}
	return int(s.source.Text[at])
}

func (s *sourceCharStream) codepointLookahead(at int) int {
	if at < 0 || at >= len(s.boundaries)-1 {
		return antlr.TokenEOF
	}
	value, _ := cyphertext.ReadCodepoint(s.source.Text, s.boundaries[at])
	return int(value)
}

func (s *sourceCharStream) Mark() int   { return -1 }
func (s *sourceCharStream) Release(int) {}
func (s *sourceCharStream) Index() int  { return s.position }
func (s *sourceCharStream) Seek(at int) { s.position = at }
func (s *sourceCharStream) Size() int {
	if s.boundaries == nil {
		return len(s.source.Text)
	}
	return len(s.boundaries) - 1
}
func (s *sourceCharStream) GetSourceName() string {
	if s.source.Text == s.source.Original && utf8.ValidString(s.source.Text) {
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
	stop = min(stop, s.Size()-1)
	if start > stop {
		return ""
	}
	return s.source.Text[s.byteOffset(start):s.byteOffset(stop+1)]
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
	stop = min(stop, s.Size()-1)
	if start > stop {
		return ""
	}
	original, _ := s.source.OriginalText(cyphertext.Span{Start: s.byteOffset(start), End: s.byteOffset(stop + 1)}, cyphertext.SourceExtent, nil)
	return original
}

// contextSourceCharStream adds cancellation instrumentation to the same
// concrete representation. It adds no separate per-stream cancellation object.
type contextSourceCharStream struct {
	sourceCharStream
	contextCharCancellation
}

func (s *contextSourceCharStream) LA(offset int) int {
	if s.boundaries == nil {
		s.rethrow()
	} else {
		s.check()
	}
	return s.sourceCharStream.LA(offset)
}
func (s *contextSourceCharStream) Consume() {
	if s.position&1023 == 0 {
		s.poll()
	}
	s.sourceCharStream.Consume()
}
