package cypher

// cleanMapWithCancellation keeps APOC's same-kind value comparison and null
// removal while charging key collection and each nested equality to one probe.
func cleanMapWithCancellation(value, keys, values interface{}, probe *pipelineCancellation) map[string]interface{} {
	result := make(map[string]interface{})
	fields, ok := value.(map[string]interface{})
	if !ok {
		return result
	}
	removed := pipelineStringSet{probe: probe}
	if names, ok := keys.([]interface{}); ok {
		for _, name := range names {
			if probe != nil && !probe.check() {
				return nil
			}
			if text, ok := name.(string); ok {
				_ = removed.add(text)
			}
			if probe != nil && probe.err != nil {
				return nil
			}
		}
	}
	listed, _ := values.([]interface{})
	for name, item := range fields {
		if probe != nil && !probe.check() {
			return nil
		}
		skip := removed.contains(name)
		if probe != nil && probe.err != nil {
			return nil
		}
		if skip || item == nil {
			continue
		}
		for _, excluded := range listed {
			if apocSameValueWithCancellation(item, excluded, probe) {
				skip = true
				break
			}
			if probe != nil && probe.err != nil {
				return nil
			}
		}
		if !skip {
			result[name] = item
		}
	}
	return result
}
