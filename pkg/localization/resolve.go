package localization

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization/internal/oslocale"
	"golang.org/x/text/language"
)

// ProcessPreferences records startup language preferences and their source.
type ProcessPreferences struct {
	Preferences  []language.Tag
	Source       string
	DetectionErr error
}

// ResolveProcessPreferences applies environment, config, OS, and source fallback precedence.
func ResolveProcessPreferences(configLanguage string) (ProcessPreferences, error) {
	return resolveProcessPreferences(configLanguage, os.LookupEnv, oslocale.Preferences)
}

func resolveProcessPreferences(
	configLanguage string,
	lookupEnv func(string) (string, bool),
	detectOS func() ([]language.Tag, error),
) (ProcessPreferences, error) {
	if value, exists := lookupEnv(EnvLanguage); exists {
		value = strings.TrimSpace(value)
		if value != "" && !strings.EqualFold(value, AutoLanguage) {
			tag, err := NormalizeLanguage(value)
			if err != nil {
				return ProcessPreferences{}, fmt.Errorf("%s: %w", EnvLanguage, err)
			}
			return ProcessPreferences{Preferences: []language.Tag{tag}, Source: "env"}, nil
		}
		return detectOrFallback(detectOS)
	}

	configLanguage = strings.TrimSpace(configLanguage)
	if configLanguage != "" && !strings.EqualFold(configLanguage, AutoLanguage) {
		tag, err := NormalizeLanguage(configLanguage)
		if err != nil {
			return ProcessPreferences{}, fmt.Errorf("localization.language: %w", err)
		}
		return ProcessPreferences{Preferences: []language.Tag{tag}, Source: "config"}, nil
	}
	return detectOrFallback(detectOS)
}

func detectOrFallback(detectOS func() ([]language.Tag, error)) (ProcessPreferences, error) {
	preferences, err := detectOS()
	if err != nil || len(preferences) == 0 {
		if err == nil {
			err = oslocale.ErrNotDetected
		}
		return ProcessPreferences{
			Preferences:  []language.Tag{language.AmericanEnglish},
			Source:       "fallback",
			DetectionErr: err,
		}, nil
	}
	return ProcessPreferences{Preferences: preferences, Source: "os"}, nil
}

// LogProcessFallback emits bounded bootstrap diagnostics after logger initialization.
func LogProcessFallback(logger *slog.Logger, resolved ProcessPreferences, match Match) {
	if logger == nil {
		return
	}
	if resolved.DetectionErr != nil {
		logger.Warn(
			"unable to determine operating system language; using English (United States)",
			"event_id", "localization.os_language_undetected",
			"resolved_language", match.Tag.String(),
			"error", resolved.DetectionErr,
		)
		return
	}
	if !match.Exact {
		logger.Warn(
			"requested language pack is unavailable; using fallback language",
			"event_id", "localization.language_pack_missing",
			"requested_language", match.Requested.String(),
			"resolved_language", match.Tag.String(),
			"source", resolved.Source,
		)
	}
}
