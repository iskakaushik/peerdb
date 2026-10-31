//go:build linux

package connpostgres

import (
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func tcpUserTimeout(network, _ string, rc syscall.RawConn) error {
	if !strings.HasPrefix(network, "tcp") {
		return nil
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, 60000)
	}); err != nil {
		return err
	}
	return serr
}
