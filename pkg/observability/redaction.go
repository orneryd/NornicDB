package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// DefaultRedactKeys is the canonical sensitive-key allow-list for slog
// records and display helpers. Keys are matched case-insensitively, regardless
// of slog.Group nesting depth. Add canonical extensions before constructing
// handlers or using display helpers; do not mutate the list concurrently with
// redaction. NORNICDB_LOG_REDACT_EXTRA (comma-separated) adds log-only keys.
//
// Note: pkg/audit/audit.go owns its own signed audit stream and is
// intentionally NOT a consumer of this list (LOG-10 / SEC-05). A future
// leaf package pkg/redaction/ can canonicalize the constants if audit
// ever grows its own redaction needs — that's a post-M1 consolidation,
// out of Phase 2 scope.
//
// `credentials` is included specifically to catch Bolt HELLO auth tokens
// (D-03a) — the protocol's own field name.
var DefaultRedactKeys = []string{
	"password", "token", "authorization", "secret", "api_key", "credentials",
	"embedding_key", "private_key", "encryption_key", "master_key", "access_key",
	"tls_key", "secret_key", "jwt_key", "client_key", "sign_key", "signing_key",
	"auth.token", "auth.password", "auth.credentials", "db.password", "bolt.auth_token",
	"http.authorization", "user.password", "user.token", "access_token", "refresh_token", "session_token",
	"credential", "access_key_id", "url_query_key", "url_query_signature", "url_query_sig", "url_query_code", "url_query_state", "url_query_auth",
}

var builtInRedactSet = defaultRedactSet()
var builtInSensitiveNames = newSensitiveNames(builtInRedactSet)

// redactedPlaceholder is the value emitted in place of any matched key.
const redactedPlaceholder = "<REDACTED>"

// RedactedPlaceholder returns the canonical placeholder used for sensitive values.
func RedactedPlaceholder() string {
	return redactedPlaceholder
}

// IsSensitiveFieldName reports whether a field name should be treated as sensitive.
// Matching is case-insensitive and recognizes common separators such as '.', '_',
// and '-' so names like client_secret and user.password are caught by the base key set.
func IsSensitiveFieldName(name string) bool {
	if builtInSensitiveNames.isSensitive(name) {
		return true
	}
	names := currentCanonicalSensitiveNames()
	return names != builtInSensitiveNames && names.isSensitive(name)
}

// redactingHandler walks slog.Attr groups recursively and replaces values
// of any key matching the redact set with "<REDACTED>". CRLF stripping
// (LOG-03) happens here as well — the SAME chokepoint applies regardless
// of inner handler choice.
//
// File budget: kept under 200 LOC per CONTEXT.
type redactingHandler struct {
	inner slog.Handler
	keys  map[string]struct{}
	names *sensitiveNames
}

// newRedactingHandler creates a handler with the given redact set.
//
// Callers SHOULD pass the result of redactSetFromEnv() so the
// NORNICDB_LOG_REDACT_EXTRA env var is honored. The defaultRedactSet()
// helper is used internally by tests that want only the built-in keys.
func newRedactingHandler(inner slog.Handler, keys map[string]struct{}) *redactingHandler {
	return &redactingHandler{inner: inner, keys: keys, names: newSensitiveNames(keys)}
}

// defaultRedactSet returns a lowercased set of just the built-in
// DefaultRedactKeys. Used by tests and as the base for redactSetFromEnv.
func defaultRedactSet() map[string]struct{} {
	set := make(map[string]struct{}, len(DefaultRedactKeys))
	for _, k := range DefaultRedactKeys {
		set[strings.ToLower(k)] = struct{}{}
	}
	return set
}

// redactSetFromEnv merges DefaultRedactKeys with NORNICDB_LOG_REDACT_EXTRA
// (comma-separated). Empty/whitespace tokens are skipped; all keys
// lowercased for case-insensitive lookup.
func redactSetFromEnv() map[string]struct{} {
	set := defaultRedactSet()
	extra := os.Getenv("NORNICDB_LOG_REDACT_EXTRA")
	if extra == "" {
		return set
	}
	for _, k := range strings.Split(extra, ",") {
		k = strings.TrimSpace(strings.ToLower(k))
		if k == "" {
			continue
		}
		set[k] = struct{}{}
	}
	return set
}

// crlfReplacer is a single immutable replacer used to strip CR and LF bytes
// from any non-redacted string value. Allocated once at package init.
var crlfReplacer = strings.NewReplacer("\r", "", "\n", "")

// Enabled delegates.
func (h *redactingHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.inner.Enabled(ctx, lvl)
}

// Handle redacts every Attr in the record (recursively) and forwards the
// redacted record to the inner handler. Per Pitfall 6 we build a fresh
// slog.Record via slog.NewRecord rather than mutating the source.
func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, RedactText(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		nr.AddAttrs(h.redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, nr)
}

// redactAttr returns a redacted copy of a. Group attrs recurse; string
// diagnostic text gets credential-redacted and CRLF-stripped (LOG-03),
// including values that the downstream handler formats as text.
func (h *redactingHandler) redactAttr(a slog.Attr) slog.Attr {
	if h.names.isSensitive(a.Key) {
		return slog.String(a.Key, redactedPlaceholder)
	}
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(h.redactAttrs(a.Value.Group())...)}
	}
	if a.Value.Kind() == slog.KindString {
		s := RedactText(a.Value.String())
		if strings.ContainsAny(s, "\r\n") {
			s = crlfReplacer.Replace(s)
		}
		return slog.String(a.Key, s)
	}
	if a.Value.Kind() == slog.KindAny {
		switch value := a.Value.Any().(type) {
		case error:
			return h.redactAttr(slog.String(a.Key, value.Error()))
		case fmt.Stringer:
			return h.redactAttr(slog.String(a.Key, value.String()))
		case []string:
			var out []string
			for i, text := range value {
				redacted := h.redactAttr(slog.String(a.Key, text)).Value.String()
				if redacted != text && out == nil {
					out = append([]string(nil), value...)
				}
				if out != nil {
					out[i] = redacted
				}
			}
			if out != nil {
				return slog.Any(a.Key, out)
			}
		}
	}
	return a
}

// WithAttrs / WithGroup preserve the wrapper, including preformatted attributes.
func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &redactingHandler{inner: h.inner.WithAttrs(h.redactAttrs(attrs)), keys: h.keys, names: h.names}
}

func (h *redactingHandler) redactAttrs(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = h.redactAttr(a)
	}
	return out
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name), keys: h.keys, names: h.names}
}
