package transport

import "errors"

var (
	ErrPeerUnavailable      = errors.New("peer unavailable")
	ErrInvalidResponse      = errors.New("invalid peer response")
	ErrUnexpectedStatus     = errors.New("unexpected RPC status")
	ErrPeerIdentityMismatch = errors.New("peer identity mismatch")
)
