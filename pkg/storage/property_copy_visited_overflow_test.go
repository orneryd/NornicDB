package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// The walker tracks only callerCopyInlineReferences references without
// allocating. A graph deeper or more widely shared than that must spill into the
// map and still deliver the whole caller-copy contract: a repeated reference and
// a cycle keep their identity inside the copy, and no part of the copy is shared
// with the input. This control deliberately outgrows the inline entries, so the
// common shallow containers keep their allocation-free visited state.
func TestCallerCopyVisitedOverflowPreservesAliasesAndCycles(t *testing.T) {
	require.Positive(t, callerCopyInlineReferences, "the inline visited entries are the optimization under test")

	t.Run("containers", func(t *testing.T) {
		root := map[string]any{}
		children := make([]any, callerCopyInlineReferences+2)
		for i := range children {
			children[i] = map[string]any{"index": int64(i)}
		}
		root["children"] = children
		root["shared"] = children[0]
		root["self"] = root
		children[0].(map[string]any)["root"] = root

		// The control is meaningful only while the input really exceeds the inline
		// entries, so inspect the walk that the copy itself performs.
		probe := callerCopyWalker{}
		_ = probe.value(root)
		require.NotNil(t, probe.seen, "control must exceed the inline capacity and spill")
		require.Equal(t, len(probe.inline), probe.count)

		copied := CopyValue(root, nil).(map[string]any)
		copiedChildren := copied["children"].([]any)
		require.Len(t, copiedChildren, len(children))
		require.NotEqual(t, reflect.ValueOf(children).Pointer(), reflect.ValueOf(copiedChildren).Pointer())

		first := copiedChildren[0].(map[string]any)
		require.NotEqual(t, reflect.ValueOf(children[0]).Pointer(), reflect.ValueOf(first).Pointer())
		shared := copied["shared"].(map[string]any)
		require.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(shared).Pointer(),
			"a repeated reference past the inline capacity keeps one copy")
		require.Equal(t, reflect.ValueOf(copied).Pointer(), reflect.ValueOf(first["root"]).Pointer(),
			"a cycle past the inline capacity resolves to the copied ancestor")
		require.Equal(t, reflect.ValueOf(copied).Pointer(), reflect.ValueOf(copied["self"]).Pointer())

		// Every route inside the copy reaches that one copy.
		shared["index"] = int64(70)
		require.Equal(t, int64(70), first["index"])
		first["extra"] = "copied"
		require.Equal(t, "copied", copied["self"].(map[string]any)["children"].([]any)[0].(map[string]any)["extra"])

		// Nothing in the copy is shared with the input.
		require.Equal(t, int64(0), children[0].(map[string]any)["index"])
		require.NotContains(t, children[0].(map[string]any), "extra")
		children[1].(map[string]any)["index"] = int64(11)
		require.Equal(t, int64(1), copiedChildren[1].(map[string]any)["index"])
	})

	t.Run("records", func(t *testing.T) {
		type link struct {
			Index int
			Next  *link
		}
		head := &link{}
		current := head
		for i := 1; i <= callerCopyInlineReferences+2; i++ {
			next := &link{Index: i}
			current.Next = next
			current = next
		}
		current.Next = head

		probe := callerCopyWalker{}
		_ = probe.value(head)
		require.NotNil(t, probe.seen, "control must exceed the inline capacity and spill")

		copied := CopyValue(head, nil).(*link)
		require.NotSame(t, head, copied)
		last := copied
		for i := 0; i < callerCopyInlineReferences+2; i++ {
			last = last.Next
		}
		require.Same(t, copied, last.Next, "a pointer cycle past the inline capacity resolves to the copied head")
		copied.Next.Index = 99
		require.Equal(t, 1, head.Next.Index, "the copied chain is not shared with the input")
	})
}
