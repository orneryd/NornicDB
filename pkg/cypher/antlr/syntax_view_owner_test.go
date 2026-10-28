package antlr

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// The shared two-word view is the one node/token identity owner: public named
// types embed it as their single field and cannot grow beyond two words.
func TestSyntaxViewSharedOwnerLayout(t *testing.T) {
	require.Equal(t, uintptr(16), unsafe.Sizeof(SyntaxNode{}))
	require.Equal(t, uintptr(16), unsafe.Sizeof(SyntaxToken{}))
	require.Equal(t, unsafe.Sizeof(SyntaxNode{}), unsafe.Sizeof(syntaxView[syntaxNode]{}))
	require.Equal(t, unsafe.Sizeof(SyntaxToken{}), unsafe.Sizeof(syntaxView[syntaxToken]{}))
	require.Equal(t, uintptr(8), unsafe.Sizeof(StatementSyntax{}))
	require.Equal(t, uintptr(8), unsafe.Sizeof(StatementEffects{}))
	var node SyntaxNode
	var token SyntaxToken
	require.Nil(t, node.data)
	require.Nil(t, token.data, "a zero node and zero token share one absent product")
	require.Equal(t, node.id, token.id)
	require.False(t, node.Valid())
}

// The public constructors and accessors keep their expression types, so a
// method expression still compiles against the retained named identity.
var (
	_ func(StatementSyntax) SyntaxNode       = StatementSyntax.Root
	_ func(StatementSyntax, int) SyntaxToken = StatementSyntax.Token
	_ func(SyntaxNode) (int, int)            = SyntaxNode.Span
	_ func(SyntaxNode) string                = SyntaxNode.Text
	_ func(SyntaxToken) (int, int)           = SyntaxToken.Span
	_ func(SyntaxToken) string               = SyntaxToken.Text
)

// Named outer views retain the original nil/zero receiver behavior. These
// controls exercise the API directly rather than inspecting its implementation.
func TestSyntaxViewZeroAndNilReceivers(t *testing.T) {
	var node SyntaxNode
	var token SyntaxToken
	var nodePointer *SyntaxNode
	var tokenPointer *SyntaxToken
	require.Panics(t, func() { node.Span() })
	require.Panics(t, func() { node.Text() })
	require.Panics(t, func() { token.Span() })
	require.Panics(t, func() { token.Text() })
	require.Panics(t, func() { nodePointer.Span() })
	require.Panics(t, func() { nodePointer.Text() })
	require.Panics(t, func() { tokenPointer.Span() })
	require.Panics(t, func() { tokenPointer.Text() })
}
