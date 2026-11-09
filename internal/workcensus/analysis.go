package workcensus

import (
	"go/ast"
	"go/token"
	"go/types"
)

// GoSyntax is a read-only, process-local view of a selected member. Clients may
// interpret its domain semantics without loading, parsing or walking source
// again. Info is nil for inactive or syntax-only documents. Object identity in
// Info.Defs/Uses joins def/use members; it is not a persisted source identity.
type GoSyntax struct {
	// Package is the actual loaded type scope, even when a compiled file has
	// no owned objects. It is nil for source outside the typed composition.
	Package   *types.Package
	Node      ast.Node
	Owner     ast.Node
	Ancestors []ast.Node
	Info      *types.Info
	FileSet   *token.FileSet
	Source    []byte
}

// Assignment routes an exact unresolved member to an operator-named owner.
// It is distinct from callee delegation and gives no behavioral fix credit.
type Assignment struct {
	ID        string `json:"id"`
	MemberID  string `json:"member_id"`
	Digest    string `json:"digest"`
	Class     string `json:"class"`
	Owner     string `json:"owner"`
	Reference string `json:"reference"`
}
