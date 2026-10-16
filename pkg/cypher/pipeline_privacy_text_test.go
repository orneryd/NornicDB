package cypher

import (
	"context"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

func TestPipelinePrivacyTextReaders(t *testing.T) {
	text := strings.Repeat(" é ", 1500)
	for _, entry := range []struct {
		name string
		args []string
	}{
		{"left", []string{"text", "one"}}, {"right", []string{"text", "one"}},
		{"substring", []string{"text", "one"}}, {"substring", []string{"text", "one", "one"}},
		{"btrim", []string{"text"}}, {"ltrim", []string{"text", "space"}}, {"rtrim", []string{"text", "space"}},
		{"trim", []string{"text"}}, {"trim", []string{"mode", "space", "text"}},
		{"trim", []string{"BOTH space FROM text"}},
	} {
		t.Run(entry.name+strings.Join(entry.args, ","), func(t *testing.T) {
			runPrivacyReaderSamples(t, func(probe *pipelineCancellation) interface{} {
				value, found, err := cypherfn.EvaluateFunction(entry.name, entry.args, cypherfn.Context{
					Cancellation: probe,
					Eval: func(name string) (interface{}, error) {
						switch name {
						case "text":
							return text, nil
						case "one":
							return int64(1), nil
						case "mode":
							return "BOTH", nil
						case "space":
							return " ", nil
						}
						return nil, nil
					},
				})
				require.True(t, found)
				if probe.err == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				return value
			})
		})
	}
	for _, cutset := range []string{"", " é"} {
		for _, leading := range []bool{true, false} {
			for _, trailing := range []bool{true, false} {
				probe := pipelineCancellation{ctx: context.Background()}
				require.Equal(t, trimCharacters(text, cutset, leading, trailing), trimCharactersWithCancellation(text, cutset, leading, trailing, &probe))
			}
		}
	}
}
