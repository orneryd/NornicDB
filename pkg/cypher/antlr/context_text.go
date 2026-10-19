package antlr

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TrimSpaceContext bounds input preparation as well as lexer work. The common
// small statement keeps the standard-library fast path.
func TrimSpaceContext(ctx context.Context, query string) (string, error) {
	if len(query) < 1024 {
		return strings.TrimSpace(query), ctx.Err()
	}
	start, end, checks := 0, len(query), 0
	for start < end {
		if checks&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		r, size := utf8.DecodeRuneInString(query[start:end])
		if !unicode.IsSpace(r) {
			break
		}
		start += size
		checks++
	}
	for start < end {
		if checks&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		r, size := utf8.DecodeLastRuneInString(query[start:end])
		if !unicode.IsSpace(r) {
			break
		}
		end -= size
		checks++
	}
	return query[start:end], ctx.Err()
}
