package fabric

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// Author: #1663 implementation lane. Text is a derived test observation;
// executable consumers receive the grammar's carried Source windows.
func useArgumentTexts(arguments []cypherantlr.UseArgument) []string {
	var texts []string
	for _, argument := range arguments {
		texts = append(texts, argument.Text)
	}
	return texts
}
