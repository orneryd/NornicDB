package localization

import (
	"fmt"
	"strings"

	"golang.org/x/text/language"
)

const (
	// EnvLanguage overrides the process-default language.
	EnvLanguage = "NORNICDB_LANGUAGE"
	// AutoLanguage requests operating-system language detection.
	AutoLanguage = "auto"
	// SourceLanguage is the complete embedded source catalog and final fallback.
	SourceLanguage = "en-US"
)

// NormalizeLanguage converts BCP 47 and common POSIX locale forms to a tag.
// It returns language.Und for C/POSIX language-neutral locales.
func NormalizeLanguage(value string) (language.Tag, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" || strings.EqualFold(normalized, AutoLanguage) {
		return language.Und, nil
	}
	upper := strings.ToUpper(normalized)
	if upper == "C" || upper == "POSIX" || upper == "C.UTF-8" || upper == "C.UTF8" {
		return language.Und, nil
	}
	if index := strings.IndexByte(normalized, '.'); index >= 0 {
		normalized = normalized[:index]
	}
	if index := strings.IndexByte(normalized, '@'); index >= 0 {
		normalized = normalized[:index]
	}
	normalized = strings.ReplaceAll(normalized, "_", "-")
	tag, err := language.Parse(normalized)
	if err != nil || tag == language.Und {
		return language.Und, fmt.Errorf("invalid language %q: %w", value, err)
	}
	return tag, nil
}
