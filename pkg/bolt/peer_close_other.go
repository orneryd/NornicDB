//go:build !darwin && !linux && !windows

package bolt

import "net"

// Other platforms retain context cancellation and the normal EOF reader.
func newPeerCloseProbe(net.Conn) (func() bool, func()) { return nil, nil }
