package cypher

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// Author: #1663 implementation lane. Tests compare semantic argument text
// separately from the coordinate assertions on each grammar source view.
func useArgumentTexts(arguments []cypherantlr.UseArgument) []string {
	var texts []string
	for _, argument := range arguments {
		texts = append(texts, argument.Text)
	}
	return texts
}
