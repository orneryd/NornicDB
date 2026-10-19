package observability

import (
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type sensitiveNameNode struct {
	children map[byte]*sensitiveNameNode
	terminal bool
}

// Compile the canonical names once. A normal log key usually has no matching
// first byte, so the hot path avoids word allocations and scanning every alias.
type sensitiveNames struct {
	keys  map[string]struct{}
	roots [256]*sensitiveNameNode
}

func newSensitiveNames(keys map[string]struct{}) *sensitiveNames {
	names := &sensitiveNames{keys: make(map[string]struct{}, len(keys))}
	for raw := range keys {
		key := normalizeFieldName(raw)
		if key == "" {
			continue
		}
		names.keys[key] = struct{}{}
		node := names.roots[key[0]]
		if node == nil {
			node = &sensitiveNameNode{}
			names.roots[key[0]] = node
		}
		for i := 1; i < len(key); i++ {
			if node.children == nil {
				node.children = make(map[byte]*sensitiveNameNode)
			}
			child := node.children[key[i]]
			if child == nil {
				child = &sensitiveNameNode{}
				node.children[key[i]] = child
			}
			node = child
		}
		node.terminal = true
	}
	return names
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

const credentialURLScheme = `[A-Za-z][A-Za-z0-9+.-]*://`

var credentialURL = regexp.MustCompile(credentialURLScheme + `[^\s"'<>\x00]+`)
var scalarCredentialURL = regexp.MustCompile(`^` + credentialURLScheme)

// RedactURL preserves credential-free URLs byte for byte. Only userinfo and
// sensitive query values are changed; paths, hosts and ordinary query options
// remain useful in diagnostics. Invalid credential-bearing URLs fail closed.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
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

// Error bodies can also carry newly issued credentials, whose values were not
// in the request. Apply the same field-name policy to structured error data.
func redactJSON(value any, path string) bool {
	changed := false
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			name := path + "." + key
			if IsSensitiveFieldName(name) {
				value[key] = redactedPlaceholder
				changed = true
			} else if redactJSON(child, name) {
				changed = true
			}
		}
	case []any:
		for _, child := range value {
			changed = redactJSON(child, path) || changed
		}
	}
	return changed
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
	for _, endpoint := range credentialURL.FindAllString(context, -1) {
		values = append(values, ClientCredentials(endpoint)...)
	}
	return values
}

// RedactText removes credentials owned by the caller, including the encodings
// used by HTTP forms, JSON and Basic authorization. It never reads environment
// variables or discovers credentials from unrelated clients.
func RedactText(text string, credentials ...string) string {
	text = redactOwnedCredentials(text, credentials)
	if strings.Contains(text, "://") {
		// Config/flag URL scalars may be malformed. Inspect their complete
		// value before tokenization so quotes or whitespace in userinfo cannot
		// split a credential away from its '@' delimiter.
		if scalarCredentialURL.MatchString(text) {
			text = RedactURL(text)
		}
		text = credentialURL.ReplaceAllStringFunc(text, RedactURL)
	}
	trimmed := strings.TrimSpace(text)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		var value any
		if json.Unmarshal([]byte(text), &value) == nil && redactJSON(value, "") {
			if encoded, err := json.Marshal(value); err == nil {
				text = string(encoded)
			}
		}
	}
	return text
}

type displayError struct {
	cause     error
	text      string
	localized *localization.LocalizedError
}

func (e *displayError) Error() string { return e.text }
func (e *displayError) Unwrap() error { return e.cause }

// Renderers use errors.As to recover message identity. Give them a safe copy,
// while Unwrap still exposes the original cause for Is/As and retry handling.
func (e *displayError) As(target any) bool {
	if localized, ok := target.(**localization.LocalizedError); ok && e.localized != nil {
		*localized = e.localized
		return true
	}
	return false
}

// RedactError keeps errors.Is/As and transport/retry classification intact.
// Localized message identity/data is also retained as a safe display copy.
func RedactError(err error, credentials ...string) error {
	result, _ := redactError(err, credentials...)
	return result
}

func redactError(err error, credentials ...string) (error, bool) {
	if err == nil {
		return nil, false
	}
	text := RedactText(err.Error(), credentials...)
	changed := text != err.Error()
	var localized *localization.LocalizedError
	if errors.As(err, &localized) {
		copy := *localized
		copy.Message.Fallback = RedactText(copy.Message.Fallback, credentials...)
		changed = changed || copy.Message.Fallback != localized.Message.Fallback
		copy.Message.Data = make(map[string]any, len(localized.Message.Data))
		for key, value := range localized.Message.Data {
			switch value := value.(type) {
			case string:
				safe := RedactText(value, credentials...)
				copy.Message.Data[key] = safe
				changed = changed || safe != value
			case error:
				safe, redacted := redactError(value, credentials...)
				copy.Message.Data[key] = safe
				changed = changed || redacted
			default:
				copy.Message.Data[key] = value
			}
		}
		localized = &copy
	}
	if !changed {
		return err, false
	}
	return &displayError{cause: err, text: text, localized: localized}, true
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
