package bolt

import (
	"golang.org/x/sys/unix"
	"net"
)

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
			events := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLRDHUP}}
			n, err := unix.Poll(events, 0)
			closed = err == nil && n > 0 && events[0].Revents&(unix.POLLRDHUP|unix.POLLHUP|unix.POLLERR) != 0
		})
		return err != nil || closed
	}, nil
}
