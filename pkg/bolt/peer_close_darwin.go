package bolt

import (
	"golang.org/x/sys/unix"
	"net"
)

// EV_EOF observes a peer FIN even when unread bytes remain in the socket.
func newPeerCloseProbe(conn net.Conn) (func() bool, func()) {
	socket := socketConn(conn)
	if socket == nil {
		return nil, nil
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return nil, nil
	}
	queue, err := unix.Kqueue()
	if err != nil {
		return nil, nil
	}
	release := func() { _ = unix.Close(queue) }
	var registrationErr error
	err = raw.Control(func(fd uintptr) {
		event := unix.Kevent_t{Filter: unix.EVFILT_READ, Flags: unix.EV_ADD | unix.EV_ENABLE}
		unix.SetKevent(&event, int(fd), unix.EVFILT_READ, unix.EV_ADD|unix.EV_ENABLE)
		_, registrationErr = unix.Kevent(queue, []unix.Kevent_t{event}, nil, &unix.Timespec{})
	})
	if err != nil || registrationErr != nil {
		release()
		return nil, nil
	}
	return func() bool {
		if err := raw.Control(func(uintptr) {}); err != nil {
			return true
		}
		events := make([]unix.Kevent_t, 1)
		n, err := unix.Kevent(queue, nil, events, &unix.Timespec{})
		return err == nil && n > 0 && events[0].Flags&unix.EV_EOF != 0
	}, release
}
