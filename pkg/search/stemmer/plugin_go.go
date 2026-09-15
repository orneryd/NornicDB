//go:build !windows

package stemmer

import (
	"fmt"
	"plugin"
)

func openPlugin(path, entrypoint string) (Plugin, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open stemmer plugin: %w", err)
	}
	sym, err := p.Lookup(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("lookup stemmer entrypoint %q: %w", entrypoint, err)
	}
	if impl, ok := sym.(Plugin); ok {
		return impl, nil
	}
	if implPtr, ok := sym.(*Plugin); ok && implPtr != nil && *implPtr != nil {
		return *implPtr, nil
	}
	return nil, fmt.Errorf("stemmer entrypoint %q does not implement stemmer.Plugin", entrypoint)
}
