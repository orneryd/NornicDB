// Package embeddingsource defines the input contract carried by managed vectors.
package embeddingsource

// MetaKey is reserved for the persisted consumed-source descriptor.
const MetaKey = "__nornic_embedding_source"

// Source describes inputs consumed by one result, including its chunks. Text
// uses the recorded worker policy; Properties names whole node-property values,
// including absent keys whose later addition changes input. AllProperties is
// the conservative default for legacy structured APIs. Providers declare
// Text/Properties/AllProperties; the text builder supplies the captured policy.
type Source struct {
	Text          bool
	IncludeLabels bool
	Include       []string
	Exclude       []string
	Properties    []string
	AllProperties bool
}

// Metadata returns an owned, codec-neutral snapshot for the result sidecar.
// The private v1 tuple is [version,text,all,labels,include,exclude,properties].
// It avoids per-node field-name maps while preserving absent-key dependencies.
func (s Source) Metadata() []any {
	return []any{"1", s.Text, s.AllProperties, s.IncludeLabels, append([]string(nil), s.Include...), append([]string(nil), s.Exclude...), append([]string(nil), s.Properties...)}
}

// Read accepts in-memory and storage-decoded tuples. An unsupported present
// descriptor never falls back to a narrower legacy source definition.
func Read(meta map[string]any) (s Source, present, valid bool) {
	raw, present := meta[MetaKey]
	if !present {
		return s, false, true
	}
	fields, ok := raw.([]any)
	if !ok || len(fields) != 7 || fields[0] != "1" {
		return s, true, false
	}
	if s.Text, ok = fields[1].(bool); !ok {
		return s, true, false
	}
	if s.AllProperties, ok = fields[2].(bool); !ok {
		return s, true, false
	}
	if s.IncludeLabels, ok = fields[3].(bool); !ok {
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
	if s.Include, ok = read(fields[4]); !ok {
		return s, true, false
	}
	if s.Exclude, ok = read(fields[5]); !ok {
		return s, true, false
	}
	if s.Properties, ok = read(fields[6]); !ok {
		return s, true, false
	}
	return s, true, true
}
