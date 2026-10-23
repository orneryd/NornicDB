package bolt

import (
	"net"
	"unsafe"

	"golang.org/x/sys/windows"
)

var wsaPoll = windows.NewLazySystemDLL("ws2_32.dll").NewProc("WSAPoll")

// WSAPoll observes hangup without consuming bytes or changing socket event
// registration, so the active RUN can stop even when its queue is full.
func newPeerCloseProbe(conn net.Conn) (func() bool, func()) {
	socket := socketConn(conn)
	if socket == nil {
		return nil, nil
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return nil, nil
	}
	return func() bool {
		closed := false
		err := raw.Control(func(fd uintptr) {
			poll := struct {
				Socket  uintptr
				Events  int16
				Revents int16
			}{Socket: fd, Events: 0x0100} // POLLRDNORM
			n, _, _ := wsaPoll.Call(uintptr(unsafe.Pointer(&poll)), 1, 0)
			closed = int32(n) > 0 && poll.Revents&(0x0001|0x0002|0x0004) != 0 // ERR, HUP, NVAL
		})
		return err != nil || closed
	}, nil
}
