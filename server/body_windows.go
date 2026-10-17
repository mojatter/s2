package server

import "syscall"

// clientGoneErrnos are what a write to a peer that closed or reset returns, as internal/poll treats them.
var clientGoneErrnos = []syscall.Errno{syscall.WSAECONNRESET, syscall.WSAECONNABORTED, syscall.ERROR_NETNAME_DELETED}
