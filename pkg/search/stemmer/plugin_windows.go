//go:build windows

package stemmer

import "errors"

func openPlugin(path, entrypoint string) (Plugin, error) {
	return nil, errors.New("stemmer Go plugins are not supported on Windows")
}
