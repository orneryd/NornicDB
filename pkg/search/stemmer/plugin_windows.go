//go:build windows

package stemmer

import "errors"

// DynamicLoadSupported reports whether this platform supports Go plugin loading.
func DynamicLoadSupported() bool { return false }

func openPlugin(path, entrypoint string) (Plugin, error) {
	return nil, errors.New("stemmer Go plugins are not supported on Windows")
}
