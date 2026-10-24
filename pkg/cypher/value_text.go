package cypher

import "strings"

// The one renderer of a Cypher value as text. A style says how a value that
// is not a container is written (leaf) and how containers are spelled; the
// renderer walks the lists and maps itself, each member through the
// cancellation owner's check (walk), sorts map keys through the bulk owner
// when the style asks, and writes into one builder.

// valueTextStyle is one way of writing values as text.
type valueTextStyle struct {
	// list and mapping say which values are written as lists and maps.
	list    func(value interface{}) ([]interface{}, bool)
	mapping func(value interface{}) (map[string]interface{}, bool)
	// listOpen, listClose and listSeparator spell a list; mapOpen, mapClose,
	// mapSeparator and keySeparator a map (key keySeparator value).
	listOpen, listClose, listSeparator            string
	mapOpen, mapClose, mapSeparator, keySeparator string
	// sorted writes a map's keys in order (else in map order).
	sorted bool
	// key writes a map key (nil: as it is).
	key func(out *strings.Builder, key string)
	// leaf writes any other value through the owner's check (walk); false
	// means it cannot be written.
	leaf func(walk *materializedRowCheckpoint, out *strings.Builder, value interface{}) bool
}

// writeValueText writes value in style; false means some value in it cannot
// be written in the style.
func writeValueText(walk *materializedRowCheckpoint, out *strings.Builder, value interface{}, style *valueTextStyle) bool {
	walk.Check()
	if entries, isMap := style.mapping(value); isMap {
		keys := make([]string, 0, len(entries))
		for key := range observedEntries(walk, entries) {
			keys = append(keys, key)
		}
		if style.sorted {
			sortWalkKeys(walk, keys)
		}
		out.WriteString(style.mapOpen)
		for index, key := range observedItems(walk, keys) {
			if index > 0 {
				out.WriteString(style.mapSeparator)
			}
			if style.key != nil {
				style.key(out, key)
			} else {
				writeObservedText(walk, out, key)
			}
			out.WriteString(style.keySeparator)
			if !writeValueText(walk, out, entries[key], style) {
				return false
			}
		}
		out.WriteString(style.mapClose)
		return true
	}
	if items, isList := style.list(value); isList {
		out.WriteString(style.listOpen)
		for index, item := range observedItems(walk, items) {
			if index > 0 {
				out.WriteString(style.listSeparator)
			}
			if !writeValueText(walk, out, item, style) {
				return false
			}
		}
		out.WriteString(style.listClose)
		return true
	}
	return style.leaf(walk, out, value)
}

// valueText is writeValueText's text ("" when a value cannot be written).
func valueText(walk *materializedRowCheckpoint, value interface{}, style *valueTextStyle) string {
	var out strings.Builder
	if !writeValueText(walk, &out, value, style) {
		return ""
	}
	return out.String()
}

// stringMap is a map[string]interface{} value.
func stringMap(value interface{}) (map[string]interface{}, bool) {
	entries, isMap := value.(map[string]interface{})
	return entries, isMap
}

// anyList is a []interface{} value.
func anyList(value interface{}) ([]interface{}, bool) {
	items, isList := value.([]interface{})
	return items, isList
}
