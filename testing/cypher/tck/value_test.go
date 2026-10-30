package tck

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	gherkin "github.com/cucumber/gherkin/go/v26"
	messages "github.com/cucumber/messages/go/v21"
)

func TestParseValuePreservesCypherTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  any
	}{
		{name: "null", input: "null", want: nil},
		{name: "integer", input: "-12", want: int64(-12)},
		{name: "float", input: "12.0", want: float64(12)},
		{name: "boolean", input: "true", want: true},
		{name: "escaped string", input: `'line\nvalue'`, want: "line\nvalue"},
		{name: "nested containers", input: `{name: 'A', values: [1, 2.0, null]}`, want: map[string]any{
			"name": "A", "values": []any{int64(1), float64(2), nil},
		}},
		{name: "node", input: `(:B:A {name: 'A'})`, want: NodeValue{
			Labels: []string{"B", "A"}, Properties: map[string]any{"name": "A"},
		}},
		{name: "relationship", input: `[:KNOWS {since: 2020}]`, want: RelationshipValue{
			Type: "KNOWS", Properties: map[string]any{"since": int64(2020)},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseValue(tt.input)
			if err != nil {
				t.Fatalf("ParseValue() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseValue() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestParseValuePreservesPathDirection(t *testing.T) {
	value, err := ParseValue(`<(:A)<-[:LEFT]-(:B)-[:RIGHT]->(:C)>`)
	if err != nil {
		t.Fatalf("ParseValue() error = %v", err)
	}
	path, ok := value.(PathValue)
	if !ok {
		t.Fatalf("ParseValue() type = %T, want PathValue", value)
	}
	if len(path.Nodes) != 3 || len(path.Segments) != 2 {
		t.Fatalf("path shape = %d nodes/%d segments", len(path.Nodes), len(path.Segments))
	}
	if path.Segments[0].Forward || !path.Segments[1].Forward {
		t.Fatalf("path directions = %v, %v", path.Segments[0].Forward, path.Segments[1].Forward)
	}
}

func TestParseValueSupportsSpecialFloats(t *testing.T) {
	for _, input := range []string{"NaN", "Inf", "-Inf"} {
		value, err := ParseValue(input)
		if err != nil {
			t.Fatalf("ParseValue(%q) error = %v", input, err)
		}
		got := value.(float64)
		if input == "NaN" && !math.IsNaN(got) {
			t.Fatalf("ParseValue(%q) = %v", input, got)
		}
	}
}

func TestParseValueRejectsTrailingContent(t *testing.T) {
	if _, err := ParseValue("1 unexpected"); err == nil {
		t.Fatal("ParseValue() accepted trailing content")
	}
}

func TestParseValueSupportsPinnedCorpusTables(t *testing.T) {
	root := filepath.Join("testdata", "opencypher", "features")
	ids := &messages.Incrementing{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".feature") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		document, err := gherkin.ParseGherkinDocument(strings.NewReader(string(content)), ids.NewId)
		if err != nil {
			return err
		}
		for _, pickle := range gherkin.Pickles(*document, path, ids.NewId) {
			for _, step := range pickle.Steps {
				if step.Argument == nil {
					continue
				}
				table := step.Argument.DataTable
				if table == nil {
					continue
				}
				switch {
				case step.Text == "parameters are:":
					for _, row := range table.Rows {
						if len(row.Cells) == 2 {
							assertCorpusValueParses(t, path, pickle.Name, step.Text, row.Cells[1].Value)
						}
					}
				case strings.HasPrefix(step.Text, "the result should be") && step.Text != "the result should be empty":
					for _, row := range table.Rows[1:] {
						for _, cell := range row.Cells {
							assertCorpusValueParses(t, path, pickle.Name, step.Text, cell.Value)
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk pinned corpus: %v", err)
	}
}

func TestBindingsCoverEveryPinnedCorpusStepForm(t *testing.T) {
	patterns := make([]*regexp.Regexp, len(bindingPatterns))
	for i, pattern := range bindingPatterns {
		patterns[i] = regexp.MustCompile(pattern)
	}
	root := filepath.Join("testdata", "opencypher", "features")
	ids := &messages.Incrementing{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".feature") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		document, err := gherkin.ParseGherkinDocument(strings.NewReader(string(content)), ids.NewId)
		if err != nil {
			return err
		}
		for _, pickle := range gherkin.Pickles(*document, path, ids.NewId) {
			for _, step := range pickle.Steps {
				matched := false
				for _, pattern := range patterns {
					if pattern.MatchString(step.Text) {
						matched = true
						break
					}
				}
				if !matched {
					t.Errorf("%s: %s: no binding for %q", path, pickle.Name, step.Text)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk pinned corpus: %v", err)
	}
}

func assertCorpusValueParses(t *testing.T, path, scenario, step, input string) {
	t.Helper()
	if _, err := ParseValue(input); err != nil {
		t.Errorf("%s: %s: %s: ParseValue(%q): %v", path, scenario, step, input, err)
	}
}
