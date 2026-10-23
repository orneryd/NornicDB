package bulk

import (
	"context"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The string passes below give the standard library's results, and observe
// the statement once per Stride pieces and once per CopyStride bytes they
// scan or write: a large data string's split, replace, join, repeat or field
// scan stops within one stride of a cancellation and returns the context's
// error (with no result).

// scan counts the pieces and bytes a string pass has made since its last
// observation.
type scan struct {
	ctx           context.Context
	pieces, bytes int
}

func (s *scan) observe(pieces, bytes int) error {
	s.pieces += pieces
	s.bytes += bytes
	if s.pieces >= Stride || s.bytes >= CopyStride {
		s.pieces, s.bytes = 0, 0
		return s.ctx.Err()
	}
	return nil
}

// Split is strings.Split.
func Split(ctx context.Context, s, sep string) ([]string, error) {
	return SplitN(ctx, s, sep, -1)
}

// SplitN is strings.SplitN.
func SplitN(ctx context.Context, s, sep string, n int) ([]string, error) {
	if len(s) <= CopyStride && (len(sep) > 0 || len(s) < Stride) {
		return strings.SplitN(s, sep, n), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	work := scan{ctx: ctx}
	if sep == "" {
		// One piece per UTF-8 sequence, as strings.SplitN explodes it.
		count := utf8.RuneCountInString(s)
		if n < 0 || n > count {
			n = count
		}
		parts := make([]string, 0, n)
		for i := 0; i < n-1 && len(s) > 0; i++ {
			_, size := utf8.DecodeRuneInString(s)
			parts = append(parts, s[:size])
			s = s[size:]
			if err := work.observe(1, size); err != nil {
				return nil, err
			}
		}
		if n > 0 && len(s) > 0 {
			parts = append(parts, s)
		}
		return parts, nil
	}
	var parts []string
	if n > 0 {
		parts = make([]string, 0, min(n, strings.Count(s, sep)+1))
	}
	for n < 0 || len(parts) < n-1 {
		at := strings.Index(s, sep)
		if at < 0 {
			break
		}
		parts = append(parts, s[:at])
		s = s[at+len(sep):]
		if err := work.observe(1, at+len(sep)); err != nil {
			return nil, err
		}
	}
	return append(parts, s), nil
}

// Replace is strings.Replace.
func Replace(ctx context.Context, s, old, replacement string, n int) (string, error) {
	if len(s) <= CopyStride && len(replacement) <= Stride && (len(old) > 0 || len(s) < Stride) {
		return strings.Replace(s, old, replacement, n), nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if old == replacement || n == 0 {
		return s, nil
	}
	work := scan{ctx: ctx}
	var out strings.Builder
	done := 0
	if old == "" {
		// The replacement goes before every UTF-8 sequence and at the end,
		// as strings.Replace inserts it.
		for len(s) > 0 && (n < 0 || done < n) {
			out.WriteString(replacement)
			_, size := utf8.DecodeRuneInString(s)
			out.WriteString(s[:size])
			s = s[size:]
			done++
			if err := work.observe(1, size+len(replacement)); err != nil {
				return "", err
			}
		}
		if len(s) == 0 && (n < 0 || done < n) {
			out.WriteString(replacement)
		}
		out.WriteString(s)
		return out.String(), nil
	}
	for n < 0 || done < n {
		at := strings.Index(s, old)
		if at < 0 {
			break
		}
		out.WriteString(s[:at])
		out.WriteString(replacement)
		s = s[at+len(old):]
		done++
		if err := work.observe(1, at+len(replacement)); err != nil {
			return "", err
		}
	}
	out.WriteString(s)
	return out.String(), nil
}

// ReplaceAll is strings.ReplaceAll.
func ReplaceAll(ctx context.Context, s, old, replacement string) (string, error) {
	return Replace(ctx, s, old, replacement, -1)
}

// Join is strings.Join.
func Join(ctx context.Context, elems []string, sep string) (string, error) {
	if len(elems) <= Stride {
		total := 0
		for _, elem := range elems {
			total += len(elem) + len(sep)
		}
		if total <= CopyStride {
			return strings.Join(elems, sep), nil
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	work := scan{ctx: ctx}
	var out strings.Builder
	for i, elem := range elems {
		if i > 0 {
			out.WriteString(sep)
		}
		out.WriteString(elem)
		if err := work.observe(1, len(elem)+len(sep)); err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

// Repeat is strings.Repeat.
func Repeat(ctx context.Context, s string, count int) (string, error) {
	if count < 0 {
		panic("strings: negative Repeat count")
	}
	if len(s) == 0 || count == 0 {
		return "", nil
	}
	if len(s) > math.MaxInt/count {
		panic("strings: Repeat output length overflow")
	}
	total := len(s) * count
	if total <= CopyStride {
		return strings.Repeat(s, count), nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var out strings.Builder
	out.Grow(total)
	// The output doubles, a copy of at most CopyStride bytes (whole copies of
	// s, so the prefix copied is still s repeated) at a time.
	stride := max(len(s), CopyStride/len(s)*len(s))
	out.WriteString(s)
	for out.Len() < total {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		written := out.String()
		chunk := min(len(written), total-out.Len(), stride)
		out.WriteString(written[:chunk])
	}
	return out.String(), nil
}

// Fields is strings.Fields.
func Fields(ctx context.Context, s string) ([]string, error) {
	return FieldsFunc(ctx, s, unicode.IsSpace)
}

// FieldsFunc is strings.FieldsFunc.
func FieldsFunc(ctx context.Context, s string, separator func(rune) bool) ([]string, error) {
	if len(s) <= CopyStride {
		return strings.FieldsFunc(s, separator), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	work := scan{ctx: ctx}
	fields := []string{}
	start := -1
	for at, r := range s {
		if separator(r) {
			if start >= 0 {
				fields = append(fields, s[start:at])
				start = -1
			}
		} else if start < 0 {
			start = at
		}
		if err := work.observe(0, utf8.RuneLen(r)); err != nil {
			return nil, err
		}
	}
	if start >= 0 {
		fields = append(fields, s[start:])
	}
	return fields, nil
}
