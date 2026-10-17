//go:build !windows

package server

import "syscall"

// clientGoneErrnos are what a write to a peer that closed, reset or vanished returns.
var clientGoneErrnos = []syscall.Errno{syscall.EPIPE, syscall.ECONNRESET, syscall.ENOTCONN, syscall.ETIMEDOUT, syscall.EHOSTUNREACH}
