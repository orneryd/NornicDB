package observability

import "strings"

// SafeRedactionStreamBoundary adjusts a lexical boundary so it cannot split an
// owned credential, a partial credential prefix, or a URL-shaped span. It uses
// the same credential representations and URL reader as whole-value redaction.
func SafeRedactionStreamBoundary(text string, boundary int, credentials []string) int {
	if boundary < 0 || boundary > len(text) {
		return 0
	}
	forms := ownedCredentialForms(credentials)
	visit := func(view string, starts, ends []int) {
		sourceStart := func(index int) int {
			if starts != nil {
				return starts[index]
			}
			return index
		}
		sourceEnd := func(index int) int {
			if ends != nil {
				return ends[index-1]
			}
			return index
		}
		for _, form := range forms {
			for offset := 0; offset < len(view); {
				at := strings.Index(view[offset:], form)
				if at < 0 {
					break
				}
				at += offset
				start, end := sourceStart(at), sourceEnd(at+len(form))
				if start < boundary && end > boundary {
					boundary = start
				}
				offset = at + 1
			}
			for length := 1; length < len(form) && length <= len(view); length++ {
				if strings.HasSuffix(view, form[:length]) {
					start := sourceStart(len(view) - length)
					if start < boundary {
						boundary = start
					}
				}
			}
		}
	}
	for {
		prior := boundary
		visit(text, nil, nil)
		if strings.ContainsAny(text, "%+") {
			for _, plusSpace := range []bool{false, true} {
				view, starts, ends := credentialPercentProjection(text, plusSpace)
				visit(view, starts, ends)
			}
		}
		forEachCredentialURL(text, func(start, end int) {
			if start < boundary && end > boundary {
				boundary = start
			}
		})
		if prior == boundary {
			return boundary
		}
	}
}
