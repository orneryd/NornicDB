package cyphernames

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// DeclarationForms changes only the declaration marker and surrounding whitespace.
// The emitted name bytes (including any quoting defect) remain untouched. Every
// caller must test the original output as well as these equivalent spellings.
var DeclarationForms = []string{":", ": ", " :\t", " IS ", "\tIS\t", "\nIS\n"}

// RelationshipBracket extracts the actual emitter's type and property syntax.
// It does not decode and re-quote the type: that would repair a broken emitter.
func RelationshipBracket(text string) (string, error) {
	open := strings.IndexByte(text, '[')
	if open < 0 {
		return "", fmt.Errorf("missing relationship: %s", text)
	}
	close := cyphertext.FindMatchingDelimiter(text, open, '[', ']', cyphertext.OpeningDelimiter)
	if close < 0 {
		return "", fmt.Errorf("unclosed relationship: %s", text)
	}
	return text[open : close+1], nil
}

// WithDeclarationForm retains the type expression and properties byte for byte.
// Only a leading variable/marker is replaced; punctuation inside names is never
// searched or replaced. Current emitters use a colon, while rewrite inputs use IS.
func WithDeclarationForm(bracket, form string) (string, error) {
	at := 1
	for at < len(bracket) && (bracket[at] == ' ' || bracket[at] == '\t' || bracket[at] == '\n') {
		at++
	}
	if at < len(bracket) && bracket[at] != ':' {
		if !strings.HasPrefix(bracket[at:], "IS ") {
			name, ok := cyphertext.ScanIdentifier(bracket, at)
			end := name.End
			if !ok {
				return "", fmt.Errorf("invalid variable: %s", bracket)
			}
			at = end
		}
	}
	for at < len(bracket) && (bracket[at] == ' ' || bracket[at] == '\t' || bracket[at] == '\n') {
		at++
	}
	end := at + 1
	if at < len(bracket) && bracket[at] == ':' {
	} else if strings.HasPrefix(bracket[at:], "IS") {
		end = at + 2
	} else {
		return "", fmt.Errorf("missing declaration marker: %s", bracket)
	}
	return "[r" + form + bracket[end:], nil
}

// StoredRelationship is an observation of re-imported data, not a source token.
type StoredRelationship struct {
	Type       string
	Properties map[string]any
}

// Execute observes returned relationships from the public query executor.
type Execute func(string, map[string]any) ([]StoredRelationship, error)

// SchemaRoundTrip compares installed names/keys through public SHOW, rather
// than accepting a successfully parsed DDL statement as proof of preservation.
func SchemaRoundTrip(text, name string, keys []string, execute func(string) ([][]any, error)) error {
	if _, err := execute(text); err != nil {
		return fmt.Errorf("schema re-import: %w", err)
	}
	show := "SHOW CONSTRAINTS"
	if strings.Contains(text, " INDEX ") {
		show = "SHOW INDEXES"
	}
	rows, err := execute(show + " YIELD labelsOrTypes, properties RETURN labelsOrTypes, properties")
	if err != nil {
		return err
	}
	matches := 0
	for _, row := range rows {
		// Empty built-in lookup indexes have no label/type population.
		if len(row) < 2 || row[0] == nil {
			continue
		}
		if !reflect.DeepEqual(row[0], []string{name}) {
			return fmt.Errorf("installed schema types: %#v, want %q", rows, name)
		}
		if len(keys) > 0 && !reflect.DeepEqual(row[1], keys) {
			return fmt.Errorf("installed schema keys: %#v, want %#v", rows, keys)
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("installed schema population: %#v, want one %q", rows, name)
	}

	return nil
}

// RoundTrip imports an emitted relationship bracket, then reads it through a
// typed MATCH. The caller supplies a fresh database per subject. The CREATE
// proves storage type/key preservation; MATCH catches A|B being treated as an
// alternative, including the IS review witness. Properties are the emitter's
// actual map/parameter syntax; callers specify exactly what must survive.
func RoundTrip(bracket, name string, params map[string]any, properties map[string]any, execute Execute) error {
	prefix := ""
	if _, ok := params["row"]; ok {
		prefix = "WITH $row AS row "
	}
	created, err := execute(prefix+"CREATE ()-"+bracket+"->() RETURN r", params)
	if err != nil {
		return fmt.Errorf("re-import %s: %w", bracket, err)
	}
	if len(created) != 1 || created[0].Type != name {
		return fmt.Errorf("stored type: got %#v, want %q", created, name)
	}
	for key, value := range properties {
		if !reflect.DeepEqual(created[0].Properties[key], value) {
			return fmt.Errorf("stored key %q: got %#v, want %#v", key, created[0].Properties, value)
		}
	}
	// Preserve only the emitted type in the MATCH: CREATE property parameters
	// can be expressions, but are irrelevant to type selection here.
	match := bracket
	if at := cyphertext.IndexByteOutsideQuotedText(match, '{'); at >= 0 {
		match = match[:at] + "]"
	}
	rows, err := execute("MATCH ()-"+match+"->() RETURN r", params)
	if err != nil {
		return fmt.Errorf("re-read %s: %w", match, err)
	}
	if len(rows) != 1 || rows[0].Type != name {
		return fmt.Errorf("type selection %s: got %#v, want one %q", match, rows, name)
	}
	for key, value := range properties {
		if !reflect.DeepEqual(rows[0].Properties[key], value) {
			return fmt.Errorf("re-read key %q: got %#v, want %#v", key, rows[0].Properties, value)
		}
	}
	return nil
}
