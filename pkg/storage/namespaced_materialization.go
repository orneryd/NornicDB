package storage

import "context"

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

// materializedKeyedWithContext joins the original requests to their already
// qualified keys. Projection changes the entity view, never the caller's key.
// physical is produced by materializedIDsWithContext from the same requests.
func materializedKeyedWithContext[K comparable, T any](ctx context.Context, requests, physical []K, values map[K]*T, project func(K, *T) *T) (map[K]*T, error) {
	out := make(map[K]*T, len(values))
	for i, key := range requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item := project(physical[i], values[physical[i]]); item != nil {
			out[key] = item
		}
	}
	return out, nil
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
