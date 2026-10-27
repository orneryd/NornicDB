package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type sensitiveNameNode struct {
	children map[byte]*sensitiveNameNode
	terminal bool
}

// Compile the canonical names once. A normal log key usually has no matching
// first byte, so the hot path avoids word allocations and scanning every alias.
type sensitiveNames struct {
	roots [256]*sensitiveNameNode
}

func newSensitiveNames(keys map[string]struct{}) *sensitiveNames {
	names := &sensitiveNames{}
	for raw := range keys {
		key := normalizeFieldName(raw)
		if key == "" {
			continue
		}
		names.findNode(key, true).terminal = true
	}
	return names
}

// findNode is the single exact-name traversal for compilation and lookup;
// the compiled trie also serves the allocation-free suffix matcher.
func (names *sensitiveNames) findNode(key string, create bool) *sensitiveNameNode {
	if key == "" {
		return nil
	}
	node := names.roots[key[0]]
	if node == nil && create {
		node = &sensitiveNameNode{}
		names.roots[key[0]] = node
	}
	for i := 1; node != nil && i < len(key); i++ {
		child := node.children[key[i]]
		if child == nil && create {
			if node.children == nil {
				node.children = make(map[byte]*sensitiveNameNode)
			}
			child = &sensitiveNameNode{}
			node.children[key[i]] = child
		}
		node = child
	}
	return node
}

func (names *sensitiveNames) matchNormalized(name string) (suffix, candidate bool) {
	for i := 0; i < len(name); i++ {
		node := names.roots[name[i]]
		for j := i; node != nil; j++ {
			if node.terminal {
				if j == len(name)-1 {
					return true, true
				}
				candidate = true
			}
			if j+1 == len(name) {
				break
			}
			node = node.children[name[j+1]]
		}
	}
	return false, candidate
}

func normalizedFieldByte(c byte) (byte, bool) {
	if c >= 'A' && c <= 'Z' {
		c += 'a' - 'A'
	}
	return c, (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// Match the normalized stream without materializing it. Safe names containing
// separators (for example safe.key) must keep the span clean path allocation-free.
func (names *sensitiveNames) match(name string) (suffix, candidate bool) {
	for i := 0; i < len(name); i++ {
		if name[i] >= 128 {
			return names.matchNormalized(normalizeFieldName(name))
		}
	}
	for i := 0; i < len(name); i++ {
		c, letter := normalizedFieldByte(name[i])
		if !letter {
			continue
		}
		node := names.roots[c]
		for j := i; node != nil; {
			next := j + 1
			for next < len(name) {
				if _, letter := normalizedFieldByte(name[next]); letter {
					break
				}
				next++
			}
			if node.terminal {
				if next == len(name) {
					return true, true
				}
				candidate = true
			}
			if next == len(name) {
				break
			}
			c, _ = normalizedFieldByte(name[next])
			node = node.children[c]
			j = next
		}
	}
	return false, candidate
}

// isSensitive classifies a field using this compiled name set and its boundary
// policy. Displays, slog handlers and span projections share this owner.
func (names *sensitiveNames) isSensitive(name string) bool {
	suffix, candidate := names.match(name)
	if suffix {
		return true
	}
	if !candidate {
		return false
	}
	parts := fieldWords(name)
	if len(parts) == 0 {
		return false
	}
	// These suffixes describe credentials rather than containing their material.
	// Keep normal config paths, key IDs and token budgets useful in output.
	switch parts[len(parts)-1] {
	case "id", "ref", "path", "file", "name", "uri", "url", "type", "version", "enabled", "length", "count", "limit", "budget", "tokens", "expiry", "expires", "ttl":
		return false
	}
	for i := range parts {
		for j := i + 1; j <= len(parts); j++ {
			key := strings.Join(parts[i:j], "")
			// URL-only aliases name a query value, not metadata describing it
			// (auth_provider, for example). Generic credential names still nest.
			if node := names.findNode(key, false); node != nil && node.terminal && (!strings.HasPrefix(key, "urlquery") || j == len(parts)) {
				return true
			}
		}
	}
	return false
}

// Most log keys are short ASCII names. Normalize those without allocating word
// slices; only candidate matches need the word-boundary/metadata checks below.
func normalizeFieldName(name string) string {
	var scratch [128]byte
	if len(name) > len(scratch) {
		return strings.Join(fieldWords(name), "")
	}
	n := 0
	unchanged := true
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 128 {
			return strings.Join(fieldWords(name), "")
		}
		normalized, letter := normalizedFieldByte(c)
		if normalized != c {
			unchanged = false
		}
		c = normalized
		if letter {
			scratch[n] = c
			n++
		} else {
			unchanged = false
		}
	}
	if unchanged {
		return name
	}
	return string(scratch[:n])
}

// fieldWords gives flags, environment variables, dotted settings and Go fields
// the same spelling, including acronym boundaries such as JWTSecret and APIKey.
func fieldWords(name string) []string {
	runes := []rune(strings.TrimSpace(name))
	var b strings.Builder
	for i, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			b.WriteByte(' ')
			continue
		}
		if i > 0 && unicode.IsUpper(r) && (unicode.IsLower(runes[i-1]) ||
			(unicode.IsUpper(runes[i-1]) && i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
			b.WriteByte(' ')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return strings.Fields(b.String())
}

// DisplayValue projects a configuration value for output only. Callers must
// retain the original value for execution and persistence.
func DisplayValue(name, value string) string {
	if value == "" {
		return ""
	}
	if IsSensitiveFieldName(name) {
		return redactedPlaceholder
	}
	return RedactText(value)
}

// DisplayFlagDefault projects help metadata even when a secret default is unset.
// Runtime flag values and configuration presence are unaffected.
func DisplayFlagDefault(name, value string) string {
	if IsSensitiveFieldName(name) {
		return redactedPlaceholder
	}
	return DisplayValue(name, value)
}

// forEachCredentialURL locates URL-shaped spans in text and lets url.Parse
// define the accepted URL grammar. It deliberately does not tokenize URL
// characters: several valid userinfo delimiters are also punctuation in prose.
func forEachCredentialURL(text string, visit func(start, end int)) {
	for search := 0; search < len(text); {
		colonOffset := strings.IndexByte(text[search:], ':')
		if colonOffset < 0 {
			return
		}
		colon := search + colonOffset
		start := colon
		for start > search && isURLSchemeByte(text[start-1]) {
			start--
		}
		if start == colon || !isASCIIAlpha(text[start]) || start > 0 && isURLSchemeByte(text[start-1]) {
			search = colon + 1
			continue
		}

		end := colon + 1
		for end < len(text) {
			r, size := utf8.DecodeRuneInString(text[end:])
			if unicode.IsControl(r) {
				break
			}
			if cyphertext.IsWhitespace(r) {
				// Malformed userinfo can contain whitespace before its @.
				// Keep the whole authority for the canonical fail-closed reader.
				authority := colon + 3
				if strings.HasPrefix(text[colon+1:], "//") && end >= authority && !strings.ContainsAny(text[authority:end], "/?#@") {
					rest := text[end:]
					at, boundary := strings.IndexByte(rest, '@'), strings.IndexAny(rest, "/?#\r\n\t")
					if at >= 0 && (boundary < 0 || at < boundary) {
						end += at + 1
						continue
					}
				}
				break
			}
			end += size
		}
		// Strip only the matching outer wrapper. Apostrophes inside valid
		// URL userinfo remain URL data rather than premature delimiters.
		end = cyphertext.TrimQuoteWrapper(text, start, end)
		candidateEnd := parseableURLSpanEnd(text, start, end)
		candidate := text[start:candidateEnd]
		if _, err := url.Parse(candidate); err == nil || RedactURL(candidate) != candidate {
			visit(start, candidateEnd)
			search = candidateEnd
			continue
		}
		search = colon + 1
	}
}

func isURLSchemeByte(b byte) bool {
	return isASCIIAlpha(b) || b >= '0' && b <= '9' || b == '+' || b == '.' || b == '-'
}

func isASCIIAlpha(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z'
}

// If a prose or JSON wrapper closes a URL with a character rejected by
// url.Parse, peel only that rejected wrapper until the longest accepted span
// is found. Valid single quotes and other URL sub-delimiters are never peeled.
func parseableURLSpanEnd(text string, start, end int) int {
	for candidateEnd := end; candidateEnd > start; {
		if _, err := url.Parse(text[start:candidateEnd]); err == nil {
			return candidateEnd
		}
		r, size := utf8.DecodeLastRuneInString(text[start:candidateEnd])
		if !strings.ContainsRune(`"}]>`, r) || size == 0 {
			break
		}
		candidateEnd -= size
	}
	// RedactURL has a fail-closed path for malformed credential-bearing URLs.
	return end
}

// RedactURL preserves credential-free URLs byte for byte. Only userinfo and
// sensitive query values are changed; paths, hosts and ordinary query options
// remain useful in diagnostics. Invalid credential-bearing URLs fail closed.
func RedactURL(raw string) string {
	// Diagnostic text uses a visible placeholder; normalize our own marker
	// before parsing so repeated projection preserves its endpoint context.
	u, err := url.Parse(strings.ReplaceAll(raw, redactedPlaceholder, url.QueryEscape(redactedPlaceholder)))
	if err != nil {
		if strings.Contains(raw, "@") {
			return redactedPlaceholder
		}
		for _, separator := range []string{"?", "#"} {
			if _, query, ok := strings.Cut(raw, separator); ok {
				mapped, err := mapURLCredentialQuery(query, redactURLCredential)
				if err != nil || mapped != query {
					return redactedPlaceholder
				}
			}
		}
		return raw
	}
	changed := false
	err = MapURLCredentials(u, func(*url.Userinfo) *url.Userinfo {
		changed = true
		return url.User(redactedPlaceholder)
	}, func(value string, err error) (string, error) {
		changed = true
		return redactURLCredential(value, err)
	})
	if err != nil {
		return redactedPlaceholder
	}
	if !changed {
		return raw
	}
	return u.String()
}

func redactURLCredential(string, error) (string, error) {
	return redactedPlaceholder, nil
}

// RedactValue projects structured diagnostic values without changing runtime
// inputs or converting their numeric types. Maps and slices are copied only when
// a descendant changes; unchanged branches remain shared and must not be mutated.
// For example, RedactValue(params, ContextCredentials(query)...) protects a
// query's credentials repeated under otherwise ordinary parameter names.
func RedactValue(value any, credentials ...string) any {
	safe, _ := redactJSONCredentialValues(value, "", IsSensitiveFieldName, func(text string) string {
		return RedactText(text, credentials...)
	}, nil)
	return safe
}

type credentialContainer struct {
	kind    reflect.Kind
	address uintptr
	length  int
}

// redactJSONCredentialValues is the structured-value owner for display text,
// runtime diagnostic maps and slog attributes. Each boundary supplies its own
// field policy and text projection; traversal and copy-on-write stay identical.
func redactJSONCredentialValues(value any, path string, sensitive func(string) bool, redactText func(string) string, active map[credentialContainer]struct{}) (any, bool) {
	// Retain an ancestor edge unchanged: the downstream encoder then reports
	// its existing unsupported-cycle diagnostic. Only the active path counts;
	// shared non-cyclic branches must each receive their full projection.
	switch value.(type) {
	case map[string]any, []any:
		container := reflect.ValueOf(value)
		if container.Len() != 0 {
			identity := credentialContainer{container.Kind(), uintptr(container.UnsafePointer()), container.Len()}
			if _, cyclic := active[identity]; cyclic {
				return value, false
			}
			if active == nil {
				active = make(map[credentialContainer]struct{})
			}
			active[identity] = struct{}{}
			defer delete(active, identity)
		}
	}
	switch value := value.(type) {
	case map[string]any:
		out := value
		changed := false
		for key, child := range value {
			name := path + "." + key
			var safe any
			var childChanged bool
			if sensitive(name) {
				safe, childChanged = redactedPlaceholder, child != redactedPlaceholder
			} else {
				safe, childChanged = redactJSONCredentialValues(child, name, sensitive, redactText, active)
			}
			if childChanged {
				if !changed {
					out = make(map[string]any, len(value))
					for key, original := range value {
						out[key] = original
					}
					changed = true
				}
				out[key] = safe
			}
		}
		return out, changed
	case []any:
		out := value
		changed := false
		for i, child := range value {
			safe, childChanged := redactJSONCredentialValues(child, path, sensitive, redactText, active)
			if childChanged {
				if !changed {
					out = append([]any(nil), value...)
					changed = true
				}
				out[i] = safe
			}
		}
		return out, changed
	case []string:
		out := value
		changed := false
		for i, text := range value {
			safe := redactText(text)
			if safe != text {
				if !changed {
					out = append([]string(nil), value...)
					changed = true
				}
				out[i] = safe
			}
		}
		return out, changed
	case string:
		safe := redactText(value)
		return safe, safe != value
	}
	return value, false
}

func redactCredentialText(text string, credentials []string) string {
	return redactOwnedCredentials(redactCredentialURLs(text), credentials)
}

func redactCredentialURLs(text string) string {
	var out strings.Builder
	last := 0
	found := false
	forEachCredentialURL(text, func(start, end int) {
		if start < last {
			return
		}
		safe := RedactURL(text[start:end])
		if safe == text[start:end] {
			return
		}
		// URL serialization escapes angle brackets; diagnostic text retains
		// the canonical visible marker used by every other secret projection.
		safe = strings.ReplaceAll(safe, url.QueryEscape(redactedPlaceholder), redactedPlaceholder)
		if !found {
			out.Grow(len(text))
			found = true
		}
		out.WriteString(text[last:start])
		out.WriteString(safe)
		last = end
	})
	if !found {
		return text
	}
	out.WriteString(text[last:])
	return out.String()
}

// ClientCredentials keeps endpoint context distinct from opaque secrets. A key
// can itself look like a URL; its entire value still needs redaction. Ordinary
// endpoint text, including credential-free malformed URLs, remains diagnostic.
func ClientCredentials(endpoint string, secrets ...string) []string {
	values := append([]string(nil), secrets...)
	u, err := url.Parse(endpoint)
	if err != nil {
		if RedactURL(endpoint) != endpoint {
			values = append(values, endpoint)
		}
		return values
	}
	_ = MapURLCredentials(u, func(user *url.Userinfo) *url.Userinfo {
		values = append(values, user.String(), user.Username())
		if password, ok := user.Password(); ok {
			values = append(values, password, user.Username()+":"+password)
		}
		return user
	}, func(value string, _ error) (string, error) {
		values = append(values, value)
		return value, nil
	})
	return values
}

// ContextCredentials extracts URL-owned credentials from diagnostic source
// context, such as an original DDL command. It uses the same URL reader as
// client configuration; it does not classify unrelated prose as a credential.
func ContextCredentials(context string, secrets ...string) []string {
	values := append([]string(nil), secrets...)
	trimmed := strings.TrimSpace(context)
	if len(trimmed) > 0 && strings.ContainsRune("{[\"", rune(trimmed[0])) {
		var parsed any
		if json.Unmarshal([]byte(context), &parsed) == nil {
			collectJSONURLCredentials(parsed, &values)
			return values
		}
	}
	collectTextURLCredentials(context, &values)
	return values
}

func collectJSONURLCredentials(value any, values *[]string) {
	switch value := value.(type) {
	case map[string]any:
		for _, child := range value {
			collectJSONURLCredentials(child, values)
		}
	case []any:
		for _, child := range value {
			collectJSONURLCredentials(child, values)
		}
	case string:
		collectTextURLCredentials(value, values)
	}
}

func collectTextURLCredentials(text string, values *[]string) {
	forEachCredentialURL(text, func(start, end int) {
		*values = append(*values, ClientCredentials(text[start:end])...)
	})
}

// RedactText removes credentials owned by the caller, including the encodings
// used by HTTP forms, JSON and Basic authorization. It never reads environment
// variables or discovers credentials from unrelated clients.
func RedactText(text string, credentials ...string) string {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) > 0 && strings.ContainsRune("{[\"", rune(trimmed[0])) {
		var value any
		if json.Unmarshal([]byte(text), &value) == nil {
			safe, changed := redactJSONCredentialValues(value, "", IsSensitiveFieldName, func(text string) string {
				return redactCredentialText(text, credentials)
			}, nil)
			if !changed {
				return text
			}
			if encoded, err := json.Marshal(safe); err == nil {
				text = string(encoded)
			}
			return text
		}
	}
	return redactCredentialText(text, credentials)
}

// RedactError keeps errors.Is/As and transport/retry classification intact.
// Localized message identity/data is also retained as a safe display copy.
func RedactError(err error, credentials ...string) error {
	result, _ := redactError(err, credentials...)
	return result
}

var diagnosticCatalog = sync.OnceValues(func() (*localization.Manager, error) {
	return localization.NewManager(nil, nil)
})

// RedactErrorDetails suppresses source-bearing diagnostic fields when a source
// reader cannot safely delimit a credential. Parameter-free catalog messages
// are reconstructed from the embedded catalog, never from an error's fallback.
// Error identity and causes remain available for classification.
func RedactErrorDetails(err error) error {
	result, _ := projectError(err, func(string) string { return redactedPlaceholder }, true)
	return result
}

func redactError(err error, credentials ...string) (error, bool) {
	return projectError(err, func(text string) string { return RedactText(text, credentials...) }, false)
}

func projectError(err error, project func(string) string, suppressDetails bool) (error, bool) {
	if err == nil {
		return nil, false
	}
	text := project(err.Error())
	changed := text != err.Error()
	var localized *localization.LocalizedError
	if errors.As(err, &localized) {
		copy := *localized
		copy.Message.Fallback = project(copy.Message.Fallback)
		changed = changed || copy.Message.Fallback != localized.Message.Fallback
		copy.Message.Data = make(map[string]any, len(localized.Message.Data))
		for key, value := range localized.Message.Data {
			if suppressDetails {
				// Data can contain arbitrary structured values, including nested
				// messages. Nothing supplied by the diagnostic is trusted here.
				copy.Message.Data[key] = redactedPlaceholder
				changed = true
				continue
			}
			switch value := value.(type) {
			case string:
				safe := project(value)
				copy.Message.Data[key] = safe
				changed = changed || safe != value
			case error:
				safe, redacted := projectError(value, project, false)
				copy.Message.Data[key] = safe
				changed = changed || redacted
			default:
				copy.Message.Data[key] = value
			}
		}
		if suppressDetails {
			copy.Message.PluralCount = nil
			changed = true
			if len(localized.Message.Data) == 0 && localized.Message.PluralCount == nil {
				catalog, catalogErr := diagnosticCatalog()
				if catalogErr == nil {
					catalogText, _, renderErr := catalog.Render(context.Background(), localization.MessageRequest(localization.Message{ID: localized.Message.ID}))
					if renderErr == nil {
						text = catalogText
						copy.Message.Fallback = catalogText
					}
				}
			}
		}
		localized = &copy
	}
	if !changed {
		return err, false
	}
	return localization.ProjectError(err, text, localized, nil), true
}

// DisplayArgs is a copy of argv for diagnostic endpoints. It never changes the
// process arguments or flag getters. Both flag assignment forms are supported.
func DisplayArgs(args []string) []string {
	out := make([]string, len(args))
	secretNext := false
	for i, arg := range args {
		if secretNext {
			out[i] = redactedPlaceholder
			secretNext = false
			continue
		}
		if strings.HasPrefix(arg, "-") {
			name, value, assigned := strings.Cut(arg, "=")
			if IsSensitiveFieldName(name) {
				out[i] = name
				if assigned {
					out[i] += "=" + redactedPlaceholder
				} else {
					secretNext = true
				}
				continue
			}
			if assigned {
				out[i] = name + "=" + RedactText(value)
				continue
			}
		}
		out[i] = RedactText(arg)
	}
	return out
}
