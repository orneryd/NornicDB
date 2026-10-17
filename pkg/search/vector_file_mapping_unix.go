//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && !nommap

package search

import (
	"os"

	"golang.org/x/sys/unix"
)

const vectorFileMappingSupported = true

func mapVectorFileReadOnly(file *os.File, size int) ([]byte, func() error, error) {
	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return unix.Munmap(data) }, nil
}
