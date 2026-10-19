// Package embeddingsource defines the input contract carried by managed vectors.
package embeddingsource

// MetaKey is reserved for the persisted consumed-source descriptor.
const MetaKey = "__nornic_embedding_source"

// Source describes inputs consumed by one result. Text uses the recorded worker
// policy; Properties also includes absent keys whose later addition changes the
// input. AllProperties is the conservative default for legacy structured APIs.
// Providers set Text/Properties/AllProperties; the text builder supplies policy.
type Source struct {
	Text          bool
	IncludeLabels bool
	Include       []string
	Exclude       []string
	Properties    []string
	AllProperties bool
}

// Metadata returns an owned, codec-neutral snapshot for the result sidecar.
func (s Source) Metadata() map[string]any {
	return map[string]any{"version": "1", "text": s.Text, "labels": s.IncludeLabels, "include": append([]string(nil), s.Include...), "exclude": append([]string(nil), s.Exclude...), "properties": append([]string(nil), s.Properties...), "all": s.AllProperties}
}

// Read decodes both in-memory and storage-decoded descriptors. A present but
// unsupported descriptor must never fall back to a narrower legacy policy.
func Read(meta map[string]any) (s Source, present, valid bool) {
	raw, present := meta[MetaKey]
	if !present {
		return s, false, true
	}
	m, ok := raw.(map[string]any)
	if !ok || m["version"] != "1" {
		return s, true, false
	}
	s.Text, ok = m["text"].(bool)
	if !ok {
		return s, true, false
	}
	s.IncludeLabels, ok = m["labels"].(bool)
	if !ok {
		return s, true, false
	}
	s.AllProperties, ok = m["all"].(bool)
	if !ok {
		return s, true, false
	}
	read := func(v any) ([]string, bool) {
		switch x := v.(type) {
		case nil:
			return nil, true
		case []string:
			return x, true
		case []any:
			out := make([]string, len(x))
			for i, item := range x {
				var ok bool
				out[i], ok = item.(string)
				if !ok {
					return nil, false
				}
			}
			return out, true
		default:
			return nil, false
		}
	}
	if s.Include, ok = read(m["include"]); !ok {
		return s, true, false
	}
	if s.Exclude, ok = read(m["exclude"]); !ok {
		return s, true, false
	}
	if s.Properties, ok = read(m["properties"]); !ok {
		return s, true, false
	}
	return s, true, true
}
