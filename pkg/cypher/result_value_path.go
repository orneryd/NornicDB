package cypher

import "reflect"

// resultValuePath detects cycles along one result traversal. Shallow results
// use the inline stack; only unusually deep containers allocate overflow space.
// Revisiting an alias after leaving its branch remains valid. Keeping overflow
// separate avoids a self-reference that makes even a scalar walk escape to the heap.
type resultValuePath struct {
	inline [8]resultValueContainer
	active []resultValueContainer // Overflow only: never point into inline.
	depth  int
}

type resultValueContainer struct {
	pointer uintptr
	length  int
	kind    reflect.Kind
}

func (path *resultValuePath) enter(value any) bool {
	view := reflect.ValueOf(value)
	container := resultValueContainer{uintptr(view.UnsafePointer()), view.Len(), view.Kind()}
	inlineDepth := min(path.depth, len(path.inline))
	for _, ancestor := range path.inline[:inlineDepth] {
		if container == ancestor {
			return false
		}
	}
	for _, ancestor := range path.active {
		if container == ancestor {
			return false
		}
	}
	if path.depth < len(path.inline) {
		path.inline[path.depth] = container
	} else {
		path.active = append(path.active, container)
	}
	path.depth++
	return true
}

func (path *resultValuePath) leave() {
	path.depth--
	if path.depth >= len(path.inline) {
		path.active = path.active[:len(path.active)-1]
	}
}
