package bulk

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The text functions below give the standard library's results for a data
// string, and observe the statement once per CopyStride bytes they scan,
// compare or write. Like Check they stop a canceled statement's work by
// unwinding with Stopped to its boundary, so they fit where the library
// call stood, inside an expression. A string of at most CopyStride bytes is
// the library call itself.

// byteStep is the bytes of text work that count as one step of a check:
// CopyStride bytes are one stride of steps, one observation.
const byteStep = CopyStride / Stride

// CheckBytes is Check for n bytes of text work: the checkpoint observes the
// statement once per CopyStride bytes.
func (c *Checkpoint) CheckBytes(n int) {
	if c == nil {
		return
	}
	probeCheck(c)
	if c.advance(n / byteStep) {
		c.stop()
	}
}

// textCheckpoint is the checkpoint of one text call made with a context.
func textCheckpoint(ctx context.Context) Checkpoint { return Checkpoint{ctx: ctx} }

// runeChunkEnd is the end of the chunk of s that starts at start: at most
// CopyStride bytes, ending on a rune boundary.
func runeChunkEnd(s string, start int) int {
	end := min(start+CopyStride, len(s))
	for end < len(s) && end > start && !utf8.RuneStart(s[end]) {
		end--
	}
	if end == start {
		end = min(start+CopyStride, len(s))
	}
	return end
}

// TrimSpace is strings.TrimSpace.
func TrimSpace(ctx context.Context, s string) string {
	if len(s) <= CopyStride {
		return strings.TrimSpace(s)
	}
	return TrimFunc(ctx, s, unicode.IsSpace)
}

// Trim is strings.Trim.
func Trim(ctx context.Context, s, cutset string) string {
	if len(s) <= CopyStride {
		return strings.Trim(s, cutset)
	}
	return TrimFunc(ctx, s, func(r rune) bool { return strings.ContainsRune(cutset, r) })
}

// TrimFunc is strings.TrimFunc.
func TrimFunc(ctx context.Context, s string, cut func(rune) bool) string {
	if len(s) <= CopyStride {
		return strings.TrimFunc(s, cut)
	}
	c := textCheckpoint(ctx)
	start := 0
	for start < len(s) {
		end := runeChunkEnd(s, start)
		c.CheckBytes(end - start)
		kept := strings.TrimLeftFunc(s[start:end], cut)
		if len(kept) > 0 {
			start = end - len(kept)
			break
		}
		start = end
	}
	stop := len(s)
	for stop > start {
		from := max(stop-CopyStride, start)
		for from > start && !utf8.RuneStart(s[from]) {
			from--
		}
		c.CheckBytes(stop - from)
		kept := strings.TrimRightFunc(s[from:stop], cut)
		if len(kept) > 0 {
			stop = from + len(kept)
			break
		}
		stop = from
	}
	return s[start:stop]
}

// EqualFold is strings.EqualFold.
func EqualFold(ctx context.Context, s, t string) bool {
	if len(s) <= CopyStride && len(t) <= CopyStride {
		return strings.EqualFold(s, t)
	}
	c := textCheckpoint(ctx)
	scanned := 0
	for s != "" && t != "" {
		if scanned >= CopyStride {
			c.CheckBytes(scanned)
			scanned = 0
		}
		r, size := utf8.DecodeRuneInString(s)
		u, usize := utf8.DecodeRuneInString(t)
		s, t = s[size:], t[usize:]
		scanned += size + usize
		if r == u {
			continue
		}
		if !foldEqual(r, u) {
			return false
		}
	}
	return s == t
}

// foldEqual reports two runes equal under Unicode simple case folding.
func foldEqual(r, u rune) bool {
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if f == u {
			return true
		}
	}
	return false
}

// Index is strings.Index.
func Index(ctx context.Context, s, substr string) int {
	if len(s) <= CopyStride || len(substr) > CopyStride {
		return strings.Index(s, substr)
	}
	c := textCheckpoint(ctx)
	overlap := len(substr) - 1
	for start := 0; start < len(s); start += CopyStride {
		end := min(start+CopyStride+overlap, len(s))
		c.CheckBytes(end - start)
		if at := strings.Index(s[start:end], substr); at >= 0 {
			return start + at
		}
		if end == len(s) {
			break
		}
	}
	return -1
}

// Contains is strings.Contains.
func Contains(ctx context.Context, s, substr string) bool {
	return Index(ctx, s, substr) >= 0
}

// ContainsAny is strings.ContainsAny.
func ContainsAny(ctx context.Context, s, chars string) bool {
	if len(s) <= CopyStride {
		return strings.ContainsAny(s, chars)
	}
	c := textCheckpoint(ctx)
	for start := 0; start < len(s); {
		end := runeChunkEnd(s, start)
		c.CheckBytes(end - start)
		if strings.ContainsAny(s[start:end], chars) {
			return true
		}
		start = end
	}
	return false
}

// ToUpper is strings.ToUpper.
func ToUpper(ctx context.Context, s string) string {
	return mapText(ctx, s, strings.ToUpper)
}

// ToLower is strings.ToLower.
func ToLower(ctx context.Context, s string) string {
	return mapText(ctx, s, strings.ToLower)
}

// mapText applies a rune-by-rune mapping (strings.ToUpper, strings.ToLower)
// chunk by chunk: each rune maps on its own, so the chunks' results joined
// are the whole string's.
func mapText(ctx context.Context, s string, mapping func(string) string) string {
	if len(s) <= CopyStride {
		return mapping(s)
	}
	c := textCheckpoint(ctx)
	var out strings.Builder
	out.Grow(len(s))
	for start := 0; start < len(s); {
		end := runeChunkEnd(s, start)
		c.CheckBytes(end - start)
		out.WriteString(mapping(s[start:end]))
		start = end
	}
	return out.String()
}

// String is string(data).
func String(ctx context.Context, data []byte) string {
	if len(data) <= CopyStride {
		return string(data)
	}
	c := textCheckpoint(ctx)
	var out strings.Builder
	out.Grow(len(data))
	for start := 0; start < len(data); start += CopyStride {
		end := min(start+CopyStride, len(data))
		c.CheckBytes(end - start)
		out.Write(data[start:end])
	}
	return out.String()
}

// Bytes is []byte(s).
func Bytes(ctx context.Context, s string) []byte {
	if len(s) <= CopyStride {
		return []byte(s)
	}
	c := textCheckpoint(ctx)
	out := make([]byte, 0, len(s))
	for start := 0; start < len(s); start += CopyStride {
		end := min(start+CopyStride, len(s))
		c.CheckBytes(end - start)
		out = append(out, s[start:end]...)
	}
	return out
}

// UpperASCII upper-cases ASCII letters only, keeping every other byte (the
// statement scanner's upperASCII), for a data string.
func UpperASCII(ctx context.Context, s string) string {
	return mapBytes(ctx, s, 'a', 'z', 'A'-'a')
}

// LowerASCII lower-cases ASCII letters only, keeping every other byte (the
// statement scanner's lowerASCII), for a data string.
func LowerASCII(ctx context.Context, s string) string {
	return mapBytes(ctx, s, 'A', 'Z', 'a'-'A')
}

// mapBytes shifts the bytes in [low, high] by shift, chunk by chunk; a
// string with none of them is returned as it is.
func mapBytes(ctx context.Context, s string, low, high byte, shift int) string {
	c := textCheckpoint(ctx)
	first := -1
	for start := 0; start < len(s) && first < 0; start += CopyStride {
		end := min(start+CopyStride, len(s))
		if len(s) > CopyStride {
			c.CheckBytes(end - start)
		}
		for i := start; i < end; i++ {
			if s[i] >= low && s[i] <= high {
				first = i
				break
			}
		}
	}
	if first < 0 {
		return s
	}
	out := make([]byte, len(s))
	copy(out[:first], s[:first])
	for start := first; start < len(s); start += CopyStride {
		end := min(start+CopyStride, len(s))
		if len(s) > CopyStride {
			c.CheckBytes(end - start)
		}
		for i := start; i < end; i++ {
			b := s[i]
			if b >= low && b <= high {
				b = byte(int(b) + shift)
			}
			out[i] = b
		}
	}
	return string(out)
}
