//go:build nommap || (!darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd)

package search

import (
	"fmt"
	"os"
)

const vectorFileMappingSupported = false

func mapVectorFileReadOnly(_ *os.File, _ int) ([]byte, func() error, error) {
	return nil, nil, fmt.Errorf("read-only vector mapping is unsupported on this platform")
}
