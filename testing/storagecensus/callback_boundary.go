package storagecensus

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
)

// reviewedCallbackBoundary records the reviewed quiescence contract of the
// publication arbiter. Its mutex joins an in-flight action before return; its
// sealed flag rejects actions retained beyond return. The generic escape walk
// cannot infer that contract through a caller-supplied decision function.
//
// This summary grants no write acquisition itself. It only lets the existing
// analysis propagate the actual caller's holds into this argument. Any body
// change revokes the summary until reviewed again. Runtime acceptance/veto and
// retained-action tests live in TestResidualPublicationDecisionRoutes.
func reviewedCallbackBoundary(qualified string, argument int, syntax ast.Node, set *token.FileSet) bool {
	if qualified != "pkg/storage.DecidePublication" || argument != 1 {
		return false
	}
	declaration, ok := syntax.(*ast.FuncDecl)
	if !ok || declaration.Body == nil {
		return false
	}
	var normalized bytes.Buffer
	if err := format.Node(&normalized, set, declaration.Body); err != nil {
		return false
	}
	return fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes())) == "ecaa5aa04706799b8116c64a788fbf51a2e923386332eb1cd0f85ae6b2ebcf25"
}
