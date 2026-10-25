package observability

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// RequestURLMode selects how ComposeRequestURL interprets its target.
type RequestURLMode uint8

const (
	// AppendRequestRoute appends a client route to the configured base path.
	AppendRequestRoute RequestURLMode = iota
	// MergeRequestFollowup adopts a server-provided URL while retaining the
	// configured authentication material.
	MergeRequestFollowup
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

// ComposeRequestURL is the shared configured-URL lifecycle owner for outbound
// HTTP routes. AppendRequestRoute preserves the configured escaped path and
// appends target's escaped path. MergeRequestFollowup resolves target using URL
// reference rules, then overlays configured userinfo when present. Both modes
// keep the configured raw query first and append target's raw query verbatim,
// preserving spelling, order, duplicate keys and encoding. The target fragment
// replaces the configured fragment when supplied; otherwise the configured
// fragment is retained.
func ComposeRequestURL(configuredBase, target string, mode RequestURLMode) (string, error) {
	base, err := url.Parse(configuredBase)
	if err != nil {
		return "", RedactClientError(err, configuredBase)
	}
	reference, err := url.Parse(target)
	if err != nil {
		return "", RedactClientError(err, target)
	}

	var composed *url.URL
	switch mode {
	case AppendRequestRoute:
		if reference.IsAbs() || reference.Host != "" || reference.User != nil {
			return "", fmt.Errorf("request route must be relative")
		}
		copy := *base
		if err := appendEscapedURLPath(&copy, reference); err != nil {
			return "", err
		}
		composed = &copy
	case MergeRequestFollowup:
		composed = base.ResolveReference(reference)
		if base.User != nil {
			composed.User = base.User
		}
	default:
		return "", fmt.Errorf("invalid request URL composition mode")
	}

	composed.RawQuery = joinRawURLQueries(base.RawQuery, reference.RawQuery)
	composed.ForceQuery = base.ForceQuery || reference.ForceQuery
	if reference.Fragment == "" && reference.RawFragment == "" {
		composed.Fragment = base.Fragment
		composed.RawFragment = base.RawFragment
	}
	return composed.String(), nil
}

// NormalizeRequestBaseURL applies the configured-URL path contract without
// treating the complete URL as a string. It removes trailing path slashes
// while preserving userinfo and the raw query/fragment bytes verbatim.
func NormalizeRequestBaseURL(configuredBase string) (string, error) {
	base, err := url.Parse(configuredBase)
	if err != nil {
		return "", fmt.Errorf("invalid configured base URL")
	}
	escapedPath := strings.TrimRight(base.EscapedPath(), "/")
	if escapedPath == "" {
		base.Path = ""
		base.RawPath = ""
		return base.String(), nil
	}
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", fmt.Errorf("invalid escaped request path")
	}
	base.Path = decodedPath
	base.RawPath = escapedPath
	return base.String(), nil
}

type clientBoundaryError struct {
	text     string
	cause    error
	original error
}

func (e *clientBoundaryError) Error() string { return e.text }
func (e *clientBoundaryError) Unwrap() error { return e.cause }
func (e *clientBoundaryError) Is(target error) bool {
	return errors.Is(e.original, target)
}

// RedactClientError is the outbound-client error boundary paired with
// ComposeRequestURL. It removes configured URL credentials and opaque client
// secrets before an error can reach callers. Known error chains are rebuilt
// with safe causes, so errors.As cannot recover a credential-bearing
// *url.Error while errors.Is and retry sentinels remain usable.
func RedactClientError(err error, configuredURL string, secrets ...string) error {
	if err == nil {
		return nil
	}
	return redactClientError(err, ClientCredentials(configuredURL, secrets...))
}

func redactClientError(err error, credentials []string) error {
	if err == nil {
		return nil
	}

	var safeCause error
	if multiple, ok := err.(interface{ Unwrap() []error }); ok {
		causes := multiple.Unwrap()
		safe := make([]error, 0, len(causes))
		for _, cause := range causes {
			safe = append(safe, redactClientError(cause, credentials))
		}
		safeCause = errors.Join(safe...)
	} else if cause := errors.Unwrap(err); cause != nil {
		safeCause = redactClientError(cause, credentials)
	}

	switch typed := err.(type) {
	case *url.Error:
		copy := *typed
		copy.URL = RedactText(typed.URL, credentials...)
		copy.Err = safeCause
		return &copy
	case *localization.LocalizedError:
		copy := *typed
		copy.Cause = safeCause
		copy.Message.Fallback = RedactText(copy.Message.Fallback, credentials...)
		copy.Message.Data = make(map[string]any, len(typed.Message.Data))
		for key, value := range typed.Message.Data {
			switch value := value.(type) {
			case string:
				copy.Message.Data[key] = RedactText(value, credentials...)
			case error:
				copy.Message.Data[key] = redactClientError(value, credentials)
			default:
				copy.Message.Data[key] = value
			}
		}
		return &copy
	}

	text := RedactText(err.Error(), credentials...)
	if text == err.Error() && safeCause == nil {
		return err
	}
	return &clientBoundaryError{text: text, cause: safeCause, original: err}
}

func appendEscapedURLPath(base, route *url.URL) error {
	basePath := base.EscapedPath()
	routePath := route.EscapedPath()
	if routePath == "" {
		return nil
	}
	if basePath == "" || basePath == "/" {
		basePath = "/" + strings.TrimPrefix(routePath, "/")
	} else {
		basePath = strings.TrimSuffix(basePath, "/") + "/" + strings.TrimPrefix(routePath, "/")
	}
	decoded, err := url.PathUnescape(basePath)
	if err != nil {
		return fmt.Errorf("invalid escaped request path")
	}
	base.Path = decoded
	base.RawPath = basePath
	return nil
}

func joinRawURLQueries(configured, target string) string {
	if configured == "" {
		return target
	}
	if target == "" {
		return configured
	}
	return configured + "&" + target
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
