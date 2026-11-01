//go:build !cgo

package workcensus

import "fmt"

func extractSyntaxDocument(c Config, root, path, language string, out *Inventory) error {
	return fmt.Errorf("maintained language documents require a CGO-enabled census for the Tree-sitter grammars")
}
