package localization

import (
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"text/template"

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
		for id, translated := range entries {
			base, exists := source[id]
			if !exists {
				return fmt.Errorf("catalog %s contains unknown message %s", languageTag, id)
			}
			if !sameFields(base, translated) {
				return fmt.Errorf("catalog %s message %s has different template fields from %s", languageTag, id, SourceLanguage)
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

func sameFields(left, right catalogEntry) bool {
	return strings.Join(entryFields(left), "\x00") == strings.Join(entryFields(right), "\x00")
}

func entryFields(entry catalogEntry) []string {
	set := make(map[string]struct{})
	for _, value := range []string{entry.Zero, entry.One, entry.Two, entry.Few, entry.Many, entry.Other} {
		for _, match := range templateFieldPattern.FindAllStringSubmatch(value, -1) {
			set[match[1]] = struct{}{}
		}
	}
	fields := make([]string, 0, len(set))
	for field := range set {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}
