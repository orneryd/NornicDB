package oslocale

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/text/language"
)

// ErrNotDetected indicates that no usable operating-system language exists.
var ErrNotDetected = errors.New("operating system language not detected")

func environmentPreferences() ([]string, error) {
	for _, key := range []string{"LC_ALL", "LC_MESSAGES", "LANGUAGE", "LANG"} {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			continue
		}
		if key == "LANGUAGE" {
			return strings.Split(value, ":"), nil
		}
		return []string{value}, nil
	}
	return nil, ErrNotDetected
}

func parse(value string) (language.Tag, error) {
	normalized := strings.TrimSpace(value)
	upper := strings.ToUpper(normalized)
	if normalized == "" || upper == "C" || upper == "POSIX" || upper == "C.UTF-8" || upper == "C.UTF8" {
		return language.Und, nil
	}
	if index := strings.IndexByte(normalized, '.'); index >= 0 {
		normalized = normalized[:index]
	}
	if index := strings.IndexByte(normalized, '@'); index >= 0 {
		normalized = normalized[:index]
	}
	normalized = strings.ReplaceAll(normalized, "_", "-")
	return language.Parse(normalized)
}
