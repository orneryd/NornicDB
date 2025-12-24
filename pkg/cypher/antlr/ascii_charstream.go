package antlr

import antlr4 "github.com/antlr4-go/antlr/v4"

// asciiCharStream is a fast-path CharStream for ASCII-only inputs.
//
// It avoids the `[]rune(data)` allocation performed by antlr4.NewInputStream(data)
// while remaining correct for ASCII Cypher queries (the common case).
//
// For non-ASCII input, callers should fall back to antlr4.NewInputStream.
type asciiCharStream struct {
	data string
	i    int
	name string
}

func newASCIICharStream(s string) *asciiCharStream {
	return &asciiCharStream{
		data: s,
		i:    0,
		name: "<empty>",
	}
}

func (s *asciiCharStream) Consume() {
	if s.i >= len(s.data) {
		panic("cannot consume EOF")
	}
	s.i++
}

func (s *asciiCharStream) LA(offset int) int {
	if offset == 0 {
		return 0
	}
	if offset < 0 {
		offset++
	}
	pos := s.i + offset - 1
	if pos < 0 || pos >= len(s.data) {
		return antlr4.TokenEOF
	}
	return int(s.data[pos])
}

func (s *asciiCharStream) Mark() int { return -1 }

func (s *asciiCharStream) Release(_ int) {}

func (s *asciiCharStream) Index() int { return s.i }

func (s *asciiCharStream) Seek(index int) { s.i = index }

func (s *asciiCharStream) Size() int { return len(s.data) }

func (s *asciiCharStream) GetSourceName() string { return s.name }

func (s *asciiCharStream) GetText(start, stop int) string {
	if start < 0 {
		start = 0
	}
	if stop < start {
		return ""
	}
	if start >= len(s.data) {
		return ""
	}
	if stop >= len(s.data) {
		stop = len(s.data) - 1
	}
	return s.data[start : stop+1]
}

func (s *asciiCharStream) GetTextFromTokens(start, end antlr4.Token) string {
	if start == nil || end == nil {
		return ""
	}
	return s.GetText(start.GetStart(), end.GetStop())
}

func (s *asciiCharStream) GetTextFromInterval(iv antlr4.Interval) string {
	return s.GetText(iv.Start, iv.Stop)
}
