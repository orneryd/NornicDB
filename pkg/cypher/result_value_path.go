package cypher

import "reflect"

// resultValuePath detects cycles along one result traversal. Shallow results
// use the inline stack; only unusually deep containers allocate overflow space.
// Revisiting an alias after leaving its branch remains valid.
type resultValuePath struct {
	inline [8]resultValueContainer
	active []resultValueContainer
}

type resultValueContainer struct {
	pointer uintptr
	length  int
	kind    reflect.Kind
}

func (path *resultValuePath) enter(value any) bool {
	view := reflect.ValueOf(value)
	container := resultValueContainer{uintptr(view.UnsafePointer()), view.Len(), view.Kind()}
	for _, ancestor := range path.active {
		if container == ancestor {
			return false
		}
	}
	if path.active == nil {
		path.active = path.inline[:0]
	}
	path.active = append(path.active, container)
	return true
}

func (path *resultValuePath) leave() {
	path.active = path.active[:len(path.active)-1]
}
