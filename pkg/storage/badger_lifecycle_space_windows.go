//go:build windows
// +build windows

package storage

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32DLL             = syscall.NewLazyDLL("kernel32.dll")
	procGetDiskFreeSpaceExW = kernel32DLL.NewProc("GetDiskFreeSpaceExW")
)

func dataDirFreeSpace(dir string) (int64, error) {
	path, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return 0, fmt.Errorf("encode data directory path %q: %w", dir, err)
	}

	var freeBytesAvailable uint64
	var totalNumberOfBytes uint64
	var totalNumberOfFreeBytes uint64

	r1, _, callErr := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(path)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return 0, fmt.Errorf("get free space for %q: %w", dir, callErr)
		}
		return 0, fmt.Errorf("get free space for %q: unknown Windows API error", dir)
	}

	return int64(freeBytesAvailable), nil
}
