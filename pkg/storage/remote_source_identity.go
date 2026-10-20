package storage

const remotePhysicalSourceKey = "\x00remotePhysicalSource"

type remotePhysicalSource struct{ location string }

// IdentityKey keeps transport graph values in the same physical-source domain
// as native values. The source is sealed metadata; public element IDs, numeric
// IDs, properties and JSON shape remain the transport's values.
func (value RemoteEntityValue) IdentityKey() string {
	element, _ := value["elementId"].(string)
	if source, ok := value[remotePhysicalSourceKey].(remotePhysicalSource); ok {
		return (EntityIdentity{Source: source.location, LocalID: element}).Key()
	}
	return element
}

// BindRemoteRowsToSource copies only branches containing remote graph markers.
// Ordinary maps are never promoted, and no transport/caller snapshot is mutated.
func BindRemoteRowsToSource(rows [][]interface{}, source string) [][]interface{} {
	var result [][]interface{}
	for i, row := range rows {
		var bound []interface{}
		for j, value := range row {
			if replacement, changed := bindRemoteSource(value, source); changed {
				if bound == nil {
					bound = append([]interface{}(nil), row...)
				}
				bound[j] = replacement
			}
		}
		if bound != nil {
			if result == nil {
				result = append([][]interface{}(nil), rows...)
			}
			result[i] = bound
		}
	}
	if result == nil {
		return rows
	}
	return result
}

func bindRemoteSource(value interface{}, source string) (interface{}, bool) {
	switch value := value.(type) {
	case RemoteEntityValue:
		if value == nil {
			return value, false
		}
		if previous, ok := value[remotePhysicalSourceKey].(remotePhysicalSource); ok && previous.location == source {
			return value, false
		}
		bound := make(RemoteEntityValue, len(value)+1)
		for key, item := range value {
			bound[key] = item
		}
		bound[remotePhysicalSourceKey] = remotePhysicalSource{source}
		return bound, true
	case RemotePathValue:
		bound := make(RemotePathValue, len(value))
		changed := false
		for i, entity := range value {
			next, replaced := bindRemoteSource(entity, source)
			bound[i] = next.(RemoteEntityValue)
			changed = changed || replaced
		}
		if changed {
			return bound, true
		}
	case []interface{}:
		var bound []interface{}
		for i, item := range value {
			if next, changed := bindRemoteSource(item, source); changed {
				if bound == nil {
					bound = append([]interface{}(nil), value...)
				}
				bound[i] = next
			}
		}
		if bound != nil {
			return bound, true
		}
	case map[string]interface{}:
		var bound map[string]interface{}
		for key, item := range value {
			if next, changed := bindRemoteSource(item, source); changed {
				if bound == nil {
					bound = make(map[string]interface{}, len(value))
					for key, item := range value {
						bound[key] = item
					}
				}
				bound[key] = next
			}
		}
		if bound != nil {
			return bound, true
		}
	}
	return value, false
}
