package storage

// materializedRead owns the read/error/result boundary for namespace views.
// Protocol bindings choose whether a provider's value survives its error.
func materializedRead[T any](read func() (T, error), view func(T) T, keepErrorValue bool) (T, error) {
	value, err := read()
	if err != nil && !keepErrorValue {
		var zero T
		return zero, err
	}
	if err != nil {
		return value, err
	}
	return view(value), nil
}

// materializedValues owns the collection walk. Its typed operation owns the
// result so a borrowed initial slice or generic return cannot alias a local
// append accumulator and inhibit the compiler's exclusive-slice optimization.
func materializedValues[T any](values []*T, visit func(*T)) {
	for _, value := range values {
		visit(value)
	}
}

// materializedKeyed is the shared batch-result walk. Projection binds each
// authoritative result key to its owned or borrowed entity view.
func materializedKeyed[K comparable, T any](values map[K]*T, project func(K, *T) (K, *T)) map[K]*T {
	out := make(map[K]*T, len(values))
	for key, value := range values {
		if id, item := project(key, value); item != nil {
			out[id] = item
		}
	}
	return out
}

// materializedIDs maps request identifiers once, preserving a non-nil empty
// request for protocols that distinguish it from nil.
func materializedIDs[I any](ids []I, prefix func(I) I) []I {
	out := make([]I, len(ids))
	for i, id := range ids {
		out[i] = prefix(id)
	}
	return out
}

// materializedCollection binds each protocol's empty result before creating
// its typed accumulator. Literal collectors remain inline, so nil-only and
// reserved output scopes preserve their distinct allocation contracts.
func materializedCollection[T any](values []*T, empty []*T, collect func([]*T) []*T) []*T {
	if len(values) == 0 {
		return empty
	}
	return collect(values)
}
