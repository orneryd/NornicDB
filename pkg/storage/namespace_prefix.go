package storage

import "strings"

func namespacePrefixFromID(id string) (string, bool) {
	if id == "" {
		return "", false
	}
	idx := strings.IndexByte(id, ':')
	if idx <= 0 {
		return "", false
	}
	return id[:idx+1], true
}

func isSystemNamespaceID(id string) bool {
	prefix, ok := namespacePrefixFromID(id)
	if !ok {
		return false
	}
	return prefix == "system:"
}
