package observability

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Collect ALL representations before matching any of them. Matches refer to
// the original input; replacing a shorter prefix or an offset overlap must not
// destroy the evidence needed to cover the rest of another owned credential.
func redactOwnedCredentials(text string, credentials []string) string {
	forms := make(map[string]struct{})
	add := func(secret string) {
		if secret == "" {
			return
		}
		quoted, _ := json.Marshal(secret)
		goQuoted := strconv.Quote(secret)
		for _, form := range []string{secret, string(quoted[1 : len(quoted)-1]), goQuoted[1 : len(goQuoted)-1],
			url.QueryEscape(secret), url.PathEscape(secret), base64.StdEncoding.EncodeToString([]byte(secret))} {
			forms[form] = struct{}{}
		}
	}
	for _, credential := range credentials {
		add(credential)
		if scheme, value, ok := strings.Cut(credential, " "); ok {
			if strings.EqualFold(scheme, "Bearer") {
				add(value)
			}
			if strings.EqualFold(scheme, "Basic") {
				add(value)
				if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
					add(string(decoded))
					if username, password, ok := strings.Cut(string(decoded), ":"); ok {
						add(username)
						add(password)
					}
				}
			}
		}
	}
	if len(forms) == 0 {
		return text
	}
	ordered := make([]string, 0, len(forms))
	for form := range forms {
		ordered = append(ordered, form)
	}
	slices.SortFunc(ordered, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	var covered []bool
	mark := func(view string, starts, ends []int) {
		for _, form := range ordered {
			for offset := 0; offset < len(view); {
				i := strings.Index(view[offset:], form)
				if i < 0 {
					break
				}
				i += offset
				start, end := i, i+len(form)
				if starts != nil {
					start, end = starts[start], ends[end-1]
				}
				if covered == nil {
					covered = make([]bool, len(text))
				}
				for j := start; j < end; j++ {
					covered[j] = true
				}
				offset = i + 1
			}
		}
	}
	mark(text, nil, nil)
	// URL escaping permits any byte to be percent encoded, with either hex
	// case and any mixture of encoded and literal bytes. Decode a projection
	// with source offsets rather than enumerating exponentially many spellings.
	// This is representation matching, not a second URL/credential classifier.
	if strings.Contains(text, "%") || strings.Contains(text, "+") {
		for _, plusSpace := range []bool{false, true} {
			view, starts, ends := credentialPercentProjection(text, plusSpace)
			mark(view, starts, ends)
		}
	}
	if covered == nil {
		return text
	}
	var out strings.Builder
	out.Grow(len(text))
	for i := 0; i < len(text); i++ {
		if !covered[i] {
			out.WriteByte(text[i])
			continue
		}
		out.WriteString(redactedPlaceholder)
		for i+1 < len(text) && covered[i+1] {
			i++
		}
	}
	return out.String()
}

func credentialPercentProjection(text string, plusSpace bool) (string, []int, []int) {
	view := make([]byte, 0, len(text))
	starts, ends := make([]int, 0, len(text)), make([]int, 0, len(text))
	for i := 0; i < len(text); {
		start, c := i, text[i]
		i++
		if c == '%' && i+1 < len(text) {
			if value, err := strconv.ParseUint(text[i:i+2], 16, 8); err == nil {
				c = byte(value)
				i += 2
			}
		} else if c == '+' && plusSpace {
			c = ' '
		}
		view = append(view, c)
		starts = append(starts, start)
		ends = append(ends, i)
	}
	return string(view), starts, ends
}
