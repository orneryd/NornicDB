package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// ddlPropertyReference is an admitted source reference, with exact decoded
// identities. Offsets are byte spans in the supplied list, excluding trivia.
// Qualified distinguishes a legacy bare key from a qualified empty name.
type ddlPropertyReference struct {
	Start, End     int
	Qualifier, Key string
	Qualified      bool
}

// readDDLPropertyReferences owns admission AND decoding. Delimiters inside
// quoted names are never list syntax. Callers retain their qualified/bare mode
// and semantic checks (arity, duplicate keys and pattern-variable identity).
func readDDLPropertyReferences(text string, allowBare bool) ([]ddlPropertyReference, bool) {
	cyphertext.RecordPropertyReference()
	at, unfinished := cyphertext.GapEnd(text, 0)
	if unfinished >= 0 {
		return nil, false
	}
	var refs []ddlPropertyReference
	for at < len(text) {
		start := at
		first, ok := cyphertext.ScanIdentifier(text, at)
		end := first.End
		if !ok {
			return nil, false
		}
		ref := ddlPropertyReference{Start: start, End: end, Key: first.Value}
		at, unfinished = cyphertext.GapEnd(text, end)
		if unfinished >= 0 {
			return nil, false
		}
		if at < len(text) && text[at] == '.' {
			at, unfinished = cyphertext.GapEnd(text, at+1)
			if unfinished >= 0 {
				return nil, false
			}
			key, valid := cyphertext.ScanIdentifier(text, at)
			keyEnd := key.End
			if !valid {
				return nil, false
			}
			ref.Qualifier, ref.Key, ref.End, ref.Qualified = first.Value, key.Value, keyEnd, true
			at, unfinished = cyphertext.GapEnd(text, keyEnd)
			if unfinished >= 0 {
				return nil, false
			}
		} else if !allowBare {
			return nil, false
		}
		if ref.Key == "" {
			return nil, false
		}
		refs = append(refs, ref)
		if at == len(text) {
			return refs, true
		}
		if text[at] != ',' {
			return nil, false
		}
		at, unfinished = cyphertext.GapEnd(text, at+1)
		if unfinished >= 0 || at == len(text) {
			return nil, false
		}
	}
	return refs, true
}

func ddlPropertyKeys(refs []ddlPropertyReference) []string {
	keys := make([]string, len(refs))
	for i, ref := range refs {
		keys[i] = ref.Key
	}
	return keys
}
