package localization

import (
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

var templateFieldPattern = regexp.MustCompile(`\{\{\s*\.([A-Za-z][A-Za-z0-9_]*)`)

type catalogEntry struct {
	ID          string `yaml:"id"`
	Description string `yaml:"description"`
	Zero        string `yaml:"zero"`
	One         string `yaml:"one"`
	Two         string `yaml:"two"`
	Few         string `yaml:"few"`
	Many        string `yaml:"many"`
	Other       string `yaml:"other"`
}

func validateCatalogFiles(fsys fs.FS, paths []string) error {
	catalogs := make(map[string]map[string]catalogEntry, len(paths))
	for _, path := range paths {
		entries, err := readCatalogEntries(fsys, path)
		if err != nil {
			return err
		}
		languageTag := catalogLanguageFromPath(path)
		if languageTag == "" {
			return fmt.Errorf("catalog %s has no language tag", path)
		}
		parsedTag, err := language.Parse(languageTag)
		if err != nil || parsedTag.String() != languageTag {
			return fmt.Errorf("catalog %s has invalid or non-canonical language tag %q", path, languageTag)
		}
		if catalogs[languageTag] == nil {
			catalogs[languageTag] = make(map[string]catalogEntry)
		}
		for id, entry := range entries {
			if _, exists := catalogs[languageTag][id]; exists {
				return fmt.Errorf("catalog %s contains duplicate message ID %s across domain files", languageTag, id)
			}
			catalogs[languageTag][id] = entry
		}
	}

	source, ok := catalogs[SourceLanguage]
	if !ok || len(source) == 0 {
		return fmt.Errorf("source localization catalog %s is missing or empty", SourceLanguage)
	}
	for languageTag, entries := range catalogs {
		if languageTag == SourceLanguage {
			continue
		}
		for id := range source {
			if _, exists := entries[id]; !exists {
				return fmt.Errorf("catalog %s is missing source message %s", languageTag, id)
			}
		}
		for id, translated := range entries {
			base, exists := source[id]
			if !exists {
				return fmt.Errorf("catalog %s contains unknown message %s", languageTag, id)
			}
			if err := validateEntrySchema(base, translated); err != nil {
				return fmt.Errorf("catalog %s message %s differs from %s: %w", languageTag, id, SourceLanguage, err)
			}
		}
	}
	return nil
}

func readCatalogEntries(fsys fs.FS, path string) (map[string]catalogEntry, error) {
	data, err := fs.ReadFile(fsys, path)
	if err != nil {
		return nil, fmt.Errorf("read localization catalog %s: %w", path, err)
	}
	var entries []catalogEntry
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse localization catalog %s: %w", path, err)
	}
	byID := make(map[string]catalogEntry, len(entries))
	for _, entry := range entries {
		entry.ID = strings.TrimSpace(entry.ID)
		if entry.ID == "" {
			return nil, fmt.Errorf("catalog %s contains an empty message ID", path)
		}
		if _, exists := byID[entry.ID]; exists {
			return nil, fmt.Errorf("catalog %s contains duplicate message ID %s", path, entry.ID)
		}
		if strings.TrimSpace(entry.Other) == "" {
			return nil, fmt.Errorf("catalog %s message %s has no other form", path, entry.ID)
		}
		for form, value := range map[string]string{
			"zero": entry.Zero, "one": entry.One, "two": entry.Two,
			"few": entry.Few, "many": entry.Many, "other": entry.Other,
		} {
			if value == "" {
				continue
			}
			if _, err := template.New(entry.ID + "." + form).Option("missingkey=error").Parse(value); err != nil {
				return nil, fmt.Errorf("catalog %s message %s %s form: %w", path, entry.ID, form, err)
			}
		}
		byID[entry.ID] = entry
	}
	return byID, nil
}

func catalogLanguageFromPath(path string) string {
	name := strings.TrimSuffix(path, ".yaml")
	index := strings.LastIndexByte(name, '.')
	if index < 0 || index == len(name)-1 {
		return ""
	}
	return name[index+1:]
}

func validateEntrySchema(source, translated catalogEntry) error {
	sourceForms := entryForms(source)
	translatedForms := entryForms(translated)
	if strings.Join(sortedKeys(sourceForms), "\x00") != strings.Join(sortedKeys(translatedForms), "\x00") {
		return fmt.Errorf("different plural forms")
	}
	for form, sourceText := range sourceForms {
		if strings.Join(templateFields(sourceText), "\x00") != strings.Join(templateFields(translatedForms[form]), "\x00") {
			return fmt.Errorf("different template fields in %s form", form)
		}
	}
	return nil
}

func entryForms(entry catalogEntry) map[string]string {
	forms := map[string]string{
		"zero": entry.Zero, "one": entry.One, "two": entry.Two,
		"few": entry.Few, "many": entry.Many, "other": entry.Other,
	}
	for form, value := range forms {
		if value == "" {
			delete(forms, form)
		}
	}
	return forms
}

func templateFields(value string) []string {
	matches := templateFieldPattern.FindAllStringSubmatch(value, -1)
	fields := make([]string, 0, len(matches))
	for _, match := range matches {
		fields = append(fields, match[1])
	}
	sort.Strings(fields)
	return fields
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
