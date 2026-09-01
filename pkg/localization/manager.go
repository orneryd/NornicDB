package localization

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

//go:embed catalog/active.*.yaml
var catalogFS embed.FS

// Match describes how requested preferences resolved to an installed catalog.
type Match struct {
	Requested  language.Tag
	Tag        language.Tag
	Confidence language.Confidence
	Exact      bool
}

// Manager owns an immutable localization bundle and process-default locale.
type Manager struct {
	bundle         *i18n.Bundle
	matcher        language.Matcher
	supported      []language.Tag
	defaultTag     language.Tag
	logger         *slog.Logger
	warningKeys    sync.Map
	missingEntries atomic.Uint64
}

// NewManager loads and validates all embedded catalogs.
func NewManager(defaultPreferences []language.Tag, logger *slog.Logger) (*Manager, error) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)

	paths, err := fs.Glob(catalogFS, "catalog/active.*.yaml")
	if err != nil {
		return nil, fmt.Errorf("list localization catalogs: %w", err)
	}
	sort.Strings(paths)
	if err := validateCatalogFiles(catalogFS, paths); err != nil {
		return nil, err
	}
	for _, path := range paths {
		if _, err := bundle.LoadMessageFileFS(catalogFS, path); err != nil {
			return nil, fmt.Errorf("load localization catalog %s: %w", path, err)
		}
	}

	supported := bundle.LanguageTags()
	sort.SliceStable(supported, func(i, j int) bool {
		if supported[i] == language.AmericanEnglish {
			return true
		}
		if supported[j] == language.AmericanEnglish {
			return false
		}
		return supported[i].String() < supported[j].String()
	})
	if len(supported) == 0 || supported[0] != language.AmericanEnglish {
		return nil, fmt.Errorf("source localization catalog %s is missing", SourceLanguage)
	}

	manager := &Manager{
		bundle:    bundle,
		matcher:   language.NewMatcher(supported),
		supported: append([]language.Tag(nil), supported...),
		logger:    logger,
	}
	match := manager.Match(defaultPreferences...)
	manager.defaultTag = match.Tag
	return manager, nil
}

// DefaultTag returns the resolved immutable process-default language.
func (m *Manager) DefaultTag() language.Tag {
	if m == nil || m.defaultTag == language.Und {
		return language.AmericanEnglish
	}
	return m.defaultTag
}

// SupportedTags returns the installed catalog languages.
func (m *Manager) SupportedTags() []language.Tag {
	if m == nil {
		return []language.Tag{language.AmericanEnglish}
	}
	return append([]language.Tag(nil), m.supported...)
}

// MissingCatalogEntryCount returns the number of render attempts that could
// not find a message in the selected catalog.
func (m *Manager) MissingCatalogEntryCount() uint64 {
	if m == nil {
		return 0
	}
	return m.missingEntries.Load()
}

// Match resolves ordered preferences against installed catalogs.
func (m *Manager) Match(preferred ...language.Tag) Match {
	if m == nil || m.matcher == nil {
		return Match{Tag: language.AmericanEnglish}
	}
	filtered := make([]language.Tag, 0, len(preferred))
	for _, tag := range preferred {
		if tag != language.Und {
			filtered = append(filtered, tag)
		}
	}
	if len(filtered) == 0 {
		filtered = []language.Tag{language.AmericanEnglish}
	}
	_, index, confidence := m.matcher.Match(filtered...)
	requested := filtered[0]
	return Match{
		Requested:  requested,
		Tag:        m.supported[index],
		Confidence: confidence,
		Exact:      confidence == language.Exact && requested.String() == m.supported[index].String(),
	}
}

// Resolve selects a language and emits a bounded warning when no exact pack exists.
func (m *Manager) Resolve(source string, preferred ...language.Tag) Match {
	match := m.Match(preferred...)
	if match.Requested != language.Und && !match.Exact {
		m.warnOnce(
			"pack:"+source+":"+match.Requested.String()+":"+match.Tag.String(),
			"localization.language_pack_missing",
			"requested_language", match.Requested.String(),
			"resolved_language", match.Tag.String(),
			"source", source,
		)
	}
	return match
}

// Render formats a message using context preferences or the process default.
func (m *Manager) Render(ctx context.Context, message Message) (string, language.Tag, error) {
	if m == nil {
		if message.Fallback != "" {
			return message.Fallback, language.AmericanEnglish, nil
		}
		return string(message.ID), language.AmericanEnglish, fmt.Errorf("localization manager is nil")
	}
	preferences := PreferencesFromContext(ctx)
	if len(preferences) == 0 {
		preferences = []language.Tag{m.DefaultTag()}
	}
	match := m.Match(preferences...)
	localizer := i18n.NewLocalizer(m.bundle, match.Tag.String())
	text, renderedTag, err := localizer.LocalizeWithTag(&i18n.LocalizeConfig{
		MessageID:    string(message.ID),
		TemplateData: message.Data,
		PluralCount:  message.PluralCount,
	})
	if err != nil {
		m.missingEntries.Add(1)
		m.warnOnce(
			"message:"+match.Tag.String()+":"+string(message.ID),
			"localization.catalog_entry_missing",
			"requested_language", match.Tag.String(),
			"message_id", string(message.ID),
		)
		fallback := i18n.NewLocalizer(m.bundle, SourceLanguage)
		text, fallbackErr := fallback.Localize(&i18n.LocalizeConfig{
			MessageID:    string(message.ID),
			TemplateData: message.Data,
			PluralCount:  message.PluralCount,
		})
		if fallbackErr != nil {
			return string(message.ID), language.AmericanEnglish, fmt.Errorf("render source message %s: %w", message.ID, fallbackErr)
		}
		return text, language.AmericanEnglish, nil
	}
	if renderedTag.String() != match.Tag.String() {
		m.missingEntries.Add(1)
		m.warnOnce(
			"message:"+match.Tag.String()+":"+string(message.ID),
			"localization.catalog_entry_missing",
			"requested_language", match.Tag.String(),
			"resolved_language", renderedTag.String(),
			"message_id", string(message.ID),
		)
	}
	return text, renderedTag, nil
}

// MustRenderEnglish renders the guaranteed source-language fallback.
func (m *Manager) MustRenderEnglish(message Message) string {
	text, _, err := m.Render(WithPreferences(context.Background(), language.AmericanEnglish), message)
	if err != nil {
		return string(message.ID)
	}
	return text
}

func (m *Manager) warnOnce(key, eventID string, attributes ...any) {
	if m == nil || m.logger == nil {
		return
	}
	if _, loaded := m.warningKeys.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	m.logger.Warn("localization fallback", append([]any{"event_id", eventID}, attributes...)...)
}
