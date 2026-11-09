package storage

import "fmt"

// Historical per-record table fixture. Current writers use dictionary epochs.
func bufferedWALPropertyBindings(writes map[string][]byte, dictionary *propertyKeyDictionary) ([]byte, error) {
	var seen map[walPropertyToken]bool
	bindings := []byte{1}
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	_, err := rewriteWALPropertyBodies(writes, func(namespace string, id uint64) (uint64, error) {
		token := walPropertyToken{namespace, id}
		if !seen[token] {
			name, ok := dictionary.reverse[namespace][id]
			if !ok {
				return 0, fmt.Errorf("wal: property token %d has no source name in namespace %q", id, namespace)
			}
			bindings = appendWALPropertyBinding(bindings, namespace, id, name)
			seen[token] = true
		}
		return id, nil
	}, func(namespace string, count int) {
		if seen == nil {
			// Capacity depends on the first body, never the whole dictionary.
			seen = make(map[walPropertyToken]bool, count)
			bindings = make([]byte, 1, 1+count*(len(namespace)+24))
			bindings[0] = 1
		}
	})
	return bindings, err
}
