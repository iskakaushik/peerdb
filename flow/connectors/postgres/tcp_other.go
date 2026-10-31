//go:build !linux

package connpostgres

import "syscall"

var tcpUserTimeout func(string, string, syscall.RawConn) error
