//go:build !windows

package server

import "syscall"

// clientGoneErrnos are what a write to a peer that closed, reset or vanished returns, with macOS's EPROTOTYPE (golang/go#51538).
var clientGoneErrnos = []syscall.Errno{syscall.EPIPE, syscall.ECONNRESET, syscall.ENOTCONN, syscall.ETIMEDOUT, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.EPROTOTYPE}
