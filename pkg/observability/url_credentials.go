package observability

import (
	"net/url"
	"strings"
)

// IsSensitiveURLQueryName applies the canonical credential-name policy to a
// decoded URL key, including provider prefixes. Public metadata suffixes stay.
func IsSensitiveURLQueryName(name string) bool {
	parts := fieldWords(name)
	for i := range parts {
		if IsSensitiveFieldName("url_query_" + strings.Join(parts[i:], "_")) {
			return true
		}
	}
	return false
}

// MapURLCredentials is the shared URL credential owner for display, diagnostic
// context, catalog persistence and runtime restoration. It visits userinfo and
// credential query/fragment values, preserving all other URL fields, original
// query spelling, order, repeated keys and escaped delimiters. A nil userinfo
// callback leaves userinfo unchanged. value receives the raw value on a decode
// error: display can redact it, while persistence can reject it without storing
// an unusable credential. Callers must not use a partially mapped URL on error.
func MapURLCredentials(endpoint *url.URL, userinfo func(*url.Userinfo) *url.Userinfo, value func(string, error) (string, error)) error {
	if endpoint.User != nil && userinfo != nil {
		endpoint.User = userinfo(endpoint.User)
	}
	query, err := mapURLCredentialQuery(endpoint.RawQuery, value)
	endpoint.RawQuery = query
	if fragment := endpoint.EscapedFragment(); strings.Contains(fragment, "=") {
		mapped, fragmentErr := mapURLCredentialQuery(fragment, value)
		if fragmentErr != nil {
			return fragmentErr
		}
		if mapped != fragment {
			decoded, decodeErr := url.PathUnescape(mapped)
			if decodeErr != nil {
				return decodeErr
			}
			endpoint.Fragment = decoded
			endpoint.RawFragment = mapped
		}
	}
	return err
}

func mapURLCredentialQuery(query string, transform func(string, error) (string, error)) (string, error) {
	if query == "" {
		return query, nil
	}
	parts := strings.Split(query, "&")
	var firstErr error
	for i, part := range parts {
		key, raw, found := strings.Cut(part, "=")
		name, err := url.QueryUnescape(key)
		if err != nil {
			// Diagnostic extraction still needs later valid fields. Writers and
			// display discard the partially mapped URL when this error returns.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !found || !IsSensitiveURLQueryName(name) {
			continue
		}
		decoded, decodeErr := url.QueryUnescape(raw)
		if decodeErr != nil {
			decoded = raw
		}
		mapped, err := transform(decoded, decodeErr)
		if err != nil {
			return "", err
		}
		if mapped != decoded {
			parts[i] = key + "=" + url.QueryEscape(mapped)
		}
	}
	return strings.Join(parts, "&"), firstErr
}
